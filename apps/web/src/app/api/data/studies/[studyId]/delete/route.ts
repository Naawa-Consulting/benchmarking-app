import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../../../_lib/authz";
import { supabaseAdminPostgrest } from "../../../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

function canDelete(role: string) {
  return role === "owner" || role === "admin";
}

function getLegacyApiBaseUrl() {
  const base = process.env.LEGACY_API_BASE_URL || process.env.NEXT_PUBLIC_API_BASE_URL || "";
  if (!base) {
    throw new Error("Missing LEGACY_API_BASE_URL (or NEXT_PUBLIC_API_BASE_URL).");
  }
  return base.replace(/\/+$/, "");
}

async function readJsonSafe(response: Response) {
  const text = await response.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { detail: text };
  }
}

function slugifyStudyId(value: string) {
  const base = value.replace(/\.[^/.]+$/, "");
  const normalized = base
    .toLowerCase()
    .trim()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_]/g, "")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
  return normalized || "study";
}

async function deleteByIds(table: string, ids: string[]) {
  if (ids.length === 0) return;
  const joined = ids.join(",");
  await supabaseAdminPostgrest(`${table}?id=in.(${joined})`, {
    method: "DELETE",
  });
}

async function deleteSnapshotByStudy(studyId: string) {
  const tables = ["journey_metrics", "touchpoint_metrics", "study_catalog"];
  const warnings: Array<{ table: string; error: unknown }> = [];
  for (const table of tables) {
    const result = await supabaseAdminPostgrest(
      `${table}?study_id=eq.${encodeURIComponent(studyId)}`,
      { method: "DELETE" }
    );
    if (!result.response.ok) {
      warnings.push({ table, error: result.data });
    }
  }
  return warnings;
}

export async function POST(
  request: NextRequest,
  context: { params: { studyId: string } }
) {
  const authz = await getRequestAuthz(request);
  const authRequired = (process.env.BBS_AUTH_MODE || "off").toLowerCase() === "supabase";
  if (authRequired && !authz.user_id) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }
  if (!canDelete(authz.role)) {
    return NextResponse.json({ detail: "Forbidden: only owner/admin can delete studies." }, { status: 403 });
  }

  const studyId = slugifyStudyId(context.params.studyId || "");
  if (!studyId) {
    return NextResponse.json({ detail: "Invalid study id." }, { status: 400 });
  }

  const legacyBase = getLegacyApiBaseUrl();
  const localDeleteResponse = await fetch(
    `${legacyBase}/ingest/study/delete?study_id=${encodeURIComponent(studyId)}`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      cache: "no-store",
    }
  );
  const localDeleteData = await readJsonSafe(localDeleteResponse);
  if (!localDeleteResponse.ok) {
    return NextResponse.json(
      { detail: "Local delete failed.", error: localDeleteData },
      { status: localDeleteResponse.status || 500 }
    );
  }

  const filesResult = await supabaseAdminPostgrest(
    "uploaded_files?select=id,filename,metadata"
  );
  const jobsResult = await supabaseAdminPostgrest(
    "ingestion_jobs?select=id,file_id,payload,operation"
  );

  const fileIds: string[] = [];
  if (filesResult.response.ok && Array.isArray(filesResult.data)) {
    for (const row of filesResult.data as Array<{ id?: string; filename?: string; metadata?: Record<string, unknown> }>) {
      const metaStudyId =
        row.metadata && typeof row.metadata.study_id === "string" ? String(row.metadata.study_id) : null;
      const nameStudyId = row.filename ? slugifyStudyId(row.filename) : null;
      if ((metaStudyId && metaStudyId === studyId) || (nameStudyId && nameStudyId === studyId)) {
        if (typeof row.id === "string") fileIds.push(row.id);
      }
    }
  }

  const jobIds: string[] = [];
  const sourcePushIds: string[] = [];
  if (jobsResult.response.ok && Array.isArray(jobsResult.data)) {
    for (const row of jobsResult.data as Array<{ id?: string; file_id?: string | null; payload?: Record<string, unknown>; operation?: string }>) {
      const payloadStudyId =
        row.payload && typeof row.payload.study_id === "string" ? String(row.payload.study_id) : null;
      const payloadStudies = row.payload && Array.isArray(row.payload.study_ids) ? row.payload.study_ids : [];
      const payloadIncludes =
        payloadStudies.some((item) => typeof item === "string" && item === studyId);
      if (
        (typeof row.file_id === "string" && fileIds.includes(row.file_id)) ||
        payloadStudyId === studyId ||
        payloadIncludes
      ) {
        if (typeof row.id === "string") {
          jobIds.push(row.id);
          if (row.operation === "push_snapshot") sourcePushIds.push(row.id);
        }
      }
    }
  }

  const versionsResult = await supabaseAdminPostgrest(
    "data_versions?select=id,source_job_id"
  );
  const versionIds: string[] = [];
  if (versionsResult.response.ok && Array.isArray(versionsResult.data)) {
    for (const row of versionsResult.data as Array<{ id?: string; source_job_id?: string | null }>) {
      if (typeof row.id === "string" && typeof row.source_job_id === "string" && jobIds.includes(row.source_job_id)) {
        versionIds.push(row.id);
      }
    }
  }

  await deleteByIds("data_versions", versionIds);
  await deleteByIds("ingestion_jobs", jobIds);
  await deleteByIds("uploaded_files", fileIds);

  const snapshotWarnings = await deleteSnapshotByStudy(studyId);

  return NextResponse.json({
    ok: true,
    study_id: studyId,
    local: localDeleteData,
    deleted: {
      uploaded_files: fileIds.length,
      ingestion_jobs: jobIds.length,
      data_versions: versionIds.length,
      push_jobs: sourcePushIds.length,
    },
    snapshot_warnings: snapshotWarnings,
  });
}
