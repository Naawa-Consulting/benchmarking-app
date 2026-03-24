import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../_lib/authz";
import { supabaseAdminPostgrest } from "../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

function sanitizeFilename(name: string) {
  return name.replace(/[^a-zA-Z0-9._-]/g, "_").slice(0, 180);
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

export async function POST(request: NextRequest) {
  const authz = await getRequestAuthz(request);
  if (!authz.user_id) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }
  if (!authz.can_mutate) {
    return NextResponse.json({ detail: "Forbidden: insufficient permissions" }, { status: 403 });
  }

  const formData = await request.formData();
  const file = formData.get("file");
  if (!(file instanceof File)) {
    return NextResponse.json({ detail: "Missing file." }, { status: 400 });
  }

  const originalName = sanitizeFilename(file.name || "upload.sav");
  if (!originalName.toLowerCase().endsWith(".sav")) {
    return NextResponse.json({ detail: "Only .sav files are supported in this phase." }, { status: 400 });
  }

  const studyId = slugifyStudyId(originalName);
  const buffer = await file.arrayBuffer();
  const legacyBase = getLegacyApiBaseUrl();
  const form = new FormData();
  form.append("file", new Blob([buffer], { type: file.type || "application/octet-stream" }), originalName);
  const uploadResponse = await fetch(
    `${legacyBase}/ingest/upload?study_id=${encodeURIComponent(studyId)}`,
    {
      method: "POST",
      body: form,
      cache: "no-store",
    }
  );
  const uploadData = await readJsonSafe(uploadResponse);
  if (!uploadResponse.ok) {
    return NextResponse.json(
      { detail: "Local upload to ingest service failed.", error: uploadData },
      { status: uploadResponse.status || 500 }
    );
  }

  const storagePath = `local/${new Date().toISOString().slice(0, 10)}/${crypto.randomUUID()}-${originalName}`;
  const bucket = "local";

  const fileInsert = await supabaseAdminPostgrest("uploaded_files", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: [
      {
        bucket,
        storage_path: storagePath,
        filename: originalName,
        content_type: file.type || "application/octet-stream",
        size_bytes: buffer.byteLength,
        uploaded_by: authz.user_id,
        status: "uploaded",
        metadata: { study_id: studyId, mode: "local_first", uploaded_to_local: true },
      },
    ],
  });
  if (!fileInsert.response.ok || !Array.isArray(fileInsert.data) || !fileInsert.data[0]) {
    return NextResponse.json(
      { detail: "Failed to create uploaded_files record.", error: fileInsert.data },
      { status: fileInsert.response.status || 500 }
    );
  }

  const uploadedFile = fileInsert.data[0] as { id: string };
  const jobInsert = await supabaseAdminPostgrest("ingestion_jobs", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: [
      {
        file_id: uploadedFile.id,
        requested_by: authz.user_id,
        status: "pending",
        operation: "upload",
        payload: { filename: originalName, study_id: studyId, mode: "local_first" },
      },
    ],
  });
  if (!jobInsert.response.ok || !Array.isArray(jobInsert.data) || !jobInsert.data[0]) {
    return NextResponse.json(
      { detail: "Failed to create ingestion job.", error: jobInsert.data },
      { status: jobInsert.response.status || 500 }
    );
  }

  const job = jobInsert.data[0] as { id: string };
  await supabaseAdminPostgrest("ingestion_job_logs", {
    method: "POST",
    body: [
      {
        job_id: job.id,
        level: "info",
        message: "File uploaded to local landing and pending processing.",
        context: { storage_path: storagePath, filename: originalName, study_id: studyId, upload: uploadData },
      },
    ],
  });

  return NextResponse.json({
    ok: true,
    study_id: studyId,
    file_id: uploadedFile.id,
    job_id: job.id,
    storage_path: storagePath,
  });
}
