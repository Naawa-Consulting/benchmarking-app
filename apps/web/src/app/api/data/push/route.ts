import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../_lib/authz";
import { supabaseAdminPostgrest } from "../../_lib/supabase-admin";
import { canPush, normalizeStudyIds, markJobFailed } from "./_shared";

export const dynamic = "force-dynamic";
export const maxDuration = 30;

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

// This route only triggers the push and returns — it does not wait for the
// (potentially multi-minute) computation to finish. The actual work runs on
// Render as a FastAPI background task (see services/api/app/routers/pipeline.py,
// POST /pipeline/push/start), which has no per-request time ceiling the way this
// Vercel function does. When that background task finishes, FastAPI calls back
// into /api/data/push/finish (same origin) to do the derivation + Postgres
// upsert and mark the ingestion_jobs row done. The Admin UI polls
// GET /api/data/jobs for the job's status instead of waiting on this response.
export async function POST(request: NextRequest) {
  const authz = await getRequestAuthz(request);
  const authRequired = (process.env.BBS_AUTH_MODE || "off").toLowerCase() === "supabase";
  if (authRequired && !authz.user_id) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }
  if (!canPush(authz.role)) {
    return NextResponse.json({ detail: "Forbidden: only owner/admin can push to Supabase." }, { status: 403 });
  }

  const body = (await request.json().catch(() => null)) as Record<string, unknown> | null;
  const studyIds = normalizeStudyIds(body);
  if (studyIds.length === 0) {
    return NextResponse.json({ detail: "Provide study_id or study_ids." }, { status: 400 });
  }

  const created = await supabaseAdminPostgrest("ingestion_jobs", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: [
      {
        requested_by: authz.user_id,
        status: "running",
        operation: "push_snapshot",
        payload: { study_ids: studyIds, started_by: authz.email || authz.user_id },
        started_at: new Date().toISOString(),
      },
    ],
  });
  if (!created.response.ok || !Array.isArray(created.data) || !created.data[0]) {
    return NextResponse.json({ detail: "Failed to create push job.", error: created.data }, { status: 500 });
  }
  const pushJob = created.data[0] as { id: string };

  try {
    const legacyBase = getLegacyApiBaseUrl();
    const callbackUrl = `${request.nextUrl.origin}/api/data/push/finish`;
    const startResponse = await fetch(`${legacyBase}/pipeline/push/start`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(process.env.INTERNAL_API_KEY ? { "x-internal-api-key": process.env.INTERNAL_API_KEY } : {}),
      },
      body: JSON.stringify({
        job_id: pushJob.id,
        study_ids: studyIds,
        callback_url: callbackUrl,
        pushed_by: authz.email || authz.user_id || null,
      }),
      cache: "no-store",
    });
    if (!startResponse.ok) {
      const errData = await readJsonSafe(startResponse);
      const detail = `Failed to start push on backend (${startResponse.status}): ${JSON.stringify(errData)}`;
      await markJobFailed(pushJob.id, detail, "Failed to start push on backend.");
      return NextResponse.json({ detail, error: errData }, { status: startResponse.status || 500 });
    }
  } catch (error) {
    const detail = error instanceof Error ? error.message : "Failed to start push.";
    await markJobFailed(pushJob.id, detail, "Failed to start push on backend.");
    return NextResponse.json({ detail }, { status: 500 });
  }

  return NextResponse.json({ ok: true, job_id: pushJob.id, status: "running" });
}
