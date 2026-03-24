import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../../../_lib/authz";
import {
  supabaseAdminPostgrest,
} from "../../../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

function isValidUuid(value: string) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    value
  );
}

async function appendJobLog(jobId: string, level: "info" | "warn" | "error", message: string, context?: unknown) {
  await supabaseAdminPostgrest("ingestion_job_logs", {
    method: "POST",
    body: [
      {
        job_id: jobId,
        level,
        message,
        context: context ?? {},
      },
    ],
  });
}

function slugifyStudyId(filename: string) {
  const base = filename.replace(/\.[^/.]+$/, "");
  const normalized = base
    .toLowerCase()
    .trim()
    .replace(/\s+/g, "_")
    .replace(/[^a-z0-9_]/g, "")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "");
  return normalized || "study";
}

function getLegacyApiBaseUrl() {
  const base =
    process.env.LEGACY_API_BASE_URL ||
    process.env.NEXT_PUBLIC_API_BASE_URL ||
    "";
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

export async function POST(
  request: NextRequest,
  context: { params: { jobId: string } }
) {
  const authz = await getRequestAuthz(request);
  const authRequired = (process.env.BBS_AUTH_MODE || "off").toLowerCase() === "supabase";
  if (authRequired && !authz.user_id) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }
  if (!authz.can_mutate) {
    return NextResponse.json({ detail: "Forbidden: insufficient permissions" }, { status: 403 });
  }

  const jobId = context.params.jobId;
  if (!isValidUuid(jobId)) {
    return NextResponse.json({ detail: "Invalid job id." }, { status: 400 });
  }

  const existing = await supabaseAdminPostgrest(
    `ingestion_jobs?select=id,status,operation,file_id,payload&id=eq.${jobId}&limit=1`
  );
  if (!existing.response.ok || !Array.isArray(existing.data) || !existing.data[0]) {
    return NextResponse.json(
      { detail: "Job not found.", error: existing.data },
      { status: existing.response.status || 404 }
    );
  }

  const current = existing.data[0] as { status?: string; operation?: string };
  if (current.status === "running") {
    return NextResponse.json({ detail: "Job is already running." }, { status: 409 });
  }

  const markRunning = await supabaseAdminPostgrest(`ingestion_jobs?id=eq.${jobId}`, {
    method: "PATCH",
    headers: { Prefer: "return=representation" },
    body: {
      status: "running",
      started_at: new Date().toISOString(),
      finished_at: null,
      error_message: null,
    },
  });
  if (!markRunning.response.ok || !Array.isArray(markRunning.data) || !markRunning.data[0]) {
    return NextResponse.json(
      { detail: "Failed to mark job as running.", error: markRunning.data },
      { status: markRunning.response.status || 500 }
    );
  }

  await appendJobLog(jobId, "info", "Job execution started.", {
    operation: current.operation || "upload",
    requested_by: authz.user_id,
  });

  try {
    const withFile = await supabaseAdminPostgrest(
      `ingestion_jobs?select=id,file_id,payload&id=eq.${jobId}&limit=1`
    );
    if (!withFile.response.ok || !Array.isArray(withFile.data) || !withFile.data[0]) {
      throw new Error("Failed to load job file metadata.");
    }

    const row = withFile.data[0] as {
      file_id?: string | null;
    };
    if (!row.file_id) {
      throw new Error("Job has no attached uploaded file.");
    }

    const fileRes = await supabaseAdminPostgrest(
      `uploaded_files?select=id,bucket,storage_path,filename,status,metadata&id=eq.${row.file_id}&limit=1`
    );
    if (!fileRes.response.ok || !Array.isArray(fileRes.data) || !fileRes.data[0]) {
      throw new Error("Attached file record not found.");
    }

    const uploadedFile = fileRes.data[0] as {
      id?: string;
      bucket?: string;
      storage_path?: string;
      filename?: string;
      status?: string;
      metadata?: Record<string, unknown>;
    };
    if (!uploadedFile.bucket || !uploadedFile.storage_path) {
      throw new Error("Uploaded file metadata is incomplete.");
    }

    const filename = uploadedFile.filename || "study.sav";
    const payload = (withFile.data[0] as { payload?: Record<string, unknown> }).payload || {};
    const payloadStudyId = typeof payload.study_id === "string" ? payload.study_id : null;
    const metadataStudyId =
      uploadedFile.metadata && typeof uploadedFile.metadata.study_id === "string"
        ? String(uploadedFile.metadata.study_id)
        : null;
    const studyId = payloadStudyId || metadataStudyId || slugifyStudyId(filename);
    const legacyBase = getLegacyApiBaseUrl();

    await appendJobLog(jobId, "info", "Running journey pipeline ensure.", {
      study_id: studyId,
      endpoint: `${legacyBase}/pipeline/journey/ensure`,
    });

    const ensureResponse = await fetch(
      `${legacyBase}/pipeline/journey/ensure?study_id=${encodeURIComponent(
        studyId
      )}&sync_raw=1&force=1`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        cache: "no-store",
      }
    );
    const ensureData = await readJsonSafe(ensureResponse);
    if (!ensureResponse.ok) {
      throw new Error(
        `Pipeline ensure failed (${ensureResponse.status}): ${JSON.stringify(ensureData)}`
      );
    }

    const statusResponse = await fetch(
      `${legacyBase}/pipeline/journey/status?study_id=${encodeURIComponent(studyId)}`,
      {
        method: "GET",
        headers: { "Content-Type": "application/json" },
        cache: "no-store",
      }
    );
    const statusData = await readJsonSafe(statusResponse);

    const payloadUpdate = await supabaseAdminPostgrest(`ingestion_jobs?id=eq.${jobId}`, {
      method: "PATCH",
      body: {
        payload: {
          study_id: studyId,
          pipeline: ensureData,
          status: statusResponse.ok ? statusData : null,
          mode: "local_first",
        },
      },
    });
    if (!payloadUpdate.response.ok) {
      await appendJobLog(jobId, "warn", "Could not persist pipeline payload in ingestion_jobs.", {
        error: payloadUpdate.data,
      });
    }

    const fileUpdate = await supabaseAdminPostgrest(`uploaded_files?id=eq.${row.file_id}`, {
      method: "PATCH",
      body: {
        status: "processed",
        metadata: {
          study_id: studyId,
          processed_by_job: jobId,
          processed_at: new Date().toISOString(),
          mode: "local_first",
        },
      },
    });
    if (!fileUpdate.response.ok) {
      await appendJobLog(jobId, "warn", "Could not update uploaded file status.", {
        error: fileUpdate.data,
      });
    }

    const markSuccess = await supabaseAdminPostgrest(`ingestion_jobs?id=eq.${jobId}`, {
      method: "PATCH",
      headers: { Prefer: "return=representation" },
      body: {
        status: "success",
        finished_at: new Date().toISOString(),
        error_message: null,
      },
    });
    if (!markSuccess.response.ok || !Array.isArray(markSuccess.data) || !markSuccess.data[0]) {
      throw new Error("Failed to mark job as success.");
    }

    await appendJobLog(jobId, "info", "Job execution completed.", {
      status: "success",
      study_id: studyId,
    });

    return NextResponse.json({
      ok: true,
      study_id: studyId,
      pipeline: ensureData,
      status: statusResponse.ok ? statusData : null,
      item: markSuccess.data[0],
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unexpected execution error.";
    await supabaseAdminPostgrest(`ingestion_jobs?id=eq.${jobId}`, {
      method: "PATCH",
      headers: { Prefer: "return=representation" },
      body: {
        status: "error",
        finished_at: new Date().toISOString(),
        error_message: message,
      },
    });
    await appendJobLog(jobId, "error", "Job execution failed.", { error: message });
    return NextResponse.json({ detail: message }, { status: 500 });
  }
}
