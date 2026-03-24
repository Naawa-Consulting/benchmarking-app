import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../_lib/authz";
import { supabaseAdminPostgrest } from "../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

type StudyItem = {
  id: string;
  name: string;
  source: string;
  status?: string;
  landing_file?: string;
  local_ready?: boolean;
  mapped?: boolean;
  raw_ready?: boolean;
  mapping_ready?: boolean;
  curated_ready?: boolean;
  published_to_supabase?: boolean;
};

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

function asBool(value: unknown) {
  return value === true;
}

export async function GET(request: NextRequest) {
  const authz = await getRequestAuthz(request);
  if (!authz.user_id) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }

  const [jobsRes, filesRes, versionsRes] = await Promise.all([
    supabaseAdminPostgrest(
      "ingestion_jobs?select=id,file_id,status,operation,payload,created_at&order=created_at.desc&limit=500"
    ),
    supabaseAdminPostgrest("uploaded_files?select=id,filename,status,metadata,created_at&order=created_at.desc&limit=500"),
    supabaseAdminPostgrest(
      "data_versions?select=id,label,status,published_at,source_job_id,created_at&order=created_at.desc&limit=500"
    ),
  ]);

  if (!jobsRes.response.ok || !filesRes.response.ok || !versionsRes.response.ok) {
    return NextResponse.json(
      {
        detail: "Failed to load studies from Supabase.",
        errors: {
          jobs: jobsRes.data,
          files: filesRes.data,
          versions: versionsRes.data,
        },
      },
      { status: 500 }
    );
  }

  const files = Array.isArray(filesRes.data)
    ? (filesRes.data as Array<{ id: string; filename?: string; status?: string; metadata?: Record<string, unknown> }>)
    : [];
  const jobs = Array.isArray(jobsRes.data)
    ? (jobsRes.data as Array<{
        id: string;
        file_id?: string | null;
        operation?: string;
        status?: string;
        payload?: Record<string, unknown>;
        created_at?: string;
      }>)
    : [];
  const versions = Array.isArray(versionsRes.data)
    ? (versionsRes.data as Array<{ id: string; status?: string; source_job_id?: string | null }>)
    : [];

  const fileById = new Map(files.map((f) => [f.id, f]));
  const studyMap = new Map<string, StudyItem>();
  const versionJobIds = new Set(
    versions
      .map((v) => (typeof v.source_job_id === "string" ? v.source_job_id : null))
      .filter((v): v is string => Boolean(v))
  );
  const publishedByPush = new Set<string>();

  for (const job of jobs) {
    if (job.status !== "success") continue;
    const payload = (job.payload || {}) as Record<string, unknown>;
    if (job.operation !== "push_snapshot") continue;
    const studies = Array.isArray(payload.study_ids) ? payload.study_ids : [];
    for (const value of studies) {
      if (typeof value === "string" && value.trim()) {
        publishedByPush.add(value.trim());
      }
    }
  }

  for (const job of jobs) {
    const file = job.file_id ? fileById.get(job.file_id) : undefined;
    const payload = (job.payload || {}) as Record<string, unknown>;
    const payloadStudyId = typeof payload.study_id === "string" ? payload.study_id : null;
    const metadataStudyId =
      file && typeof file.metadata?.study_id === "string" ? String(file.metadata.study_id) : null;
    const fileStudyId = file?.filename ? slugifyStudyId(file.filename) : null;
    const studyId = payloadStudyId || metadataStudyId || fileStudyId;
    if (!studyId) continue;

    const pipelineStatus =
      payload && typeof payload.status === "object" && payload.status
        ? (payload.status as Record<string, unknown>)
        : null;

    if (!studyMap.has(studyId)) {
      studyMap.set(studyId, {
        id: studyId,
        name: studyId,
        source: "supabase",
        status: versionJobIds.has(job.id) ? "published" : job.status || "uploaded",
        landing_file: file?.filename,
        local_ready: pipelineStatus ? asBool(pipelineStatus.raw_ready) : false,
        mapped: pipelineStatus ? asBool(pipelineStatus.mapping_ready) : false,
        raw_ready: pipelineStatus ? asBool(pipelineStatus.raw_ready) : undefined,
        mapping_ready: pipelineStatus ? asBool(pipelineStatus.mapping_ready) : undefined,
        curated_ready: pipelineStatus ? asBool(pipelineStatus.curated_ready) : undefined,
        published_to_supabase: publishedByPush.has(studyId),
      });
    }
  }

  // Include not-yet-run uploads as studies too.
  for (const file of files) {
    const metadataStudyId =
      typeof file.metadata?.study_id === "string" ? String(file.metadata.study_id) : null;
    const studyId = metadataStudyId || (file.filename ? slugifyStudyId(file.filename) : null);
    if (!studyId || studyMap.has(studyId)) continue;
    studyMap.set(studyId, {
      id: studyId,
      name: studyId,
      source: "supabase",
      status: file.status || "uploaded",
      landing_file: file.filename,
      local_ready: false,
      mapped: false,
      raw_ready: false,
      mapping_ready: false,
      curated_ready: false,
      published_to_supabase: publishedByPush.has(studyId),
    });
  }

  const studies = Array.from(studyMap.values()).sort((a, b) => a.id.localeCompare(b.id));
  return NextResponse.json({ studies });
}
