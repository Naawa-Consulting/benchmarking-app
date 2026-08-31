import { NextRequest, NextResponse } from "next/server";
import { finalizePush, markJobFailed } from "../_shared";

export const dynamic = "force-dynamic";
export const maxDuration = 30;

// Called by FastAPI (Render) once its background push computation finishes —
// never by the browser. Protected by the same shared secret used the other
// direction (apps/web -> FastAPI), just checked here instead.
export async function POST(request: NextRequest) {
  const providedKey = request.headers.get("x-internal-api-key");
  if (process.env.INTERNAL_API_KEY && providedKey !== process.env.INTERNAL_API_KEY) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }

  const body = (await request.json().catch(() => null)) as Record<string, unknown> | null;
  const jobId = typeof body?.job_id === "string" ? body.job_id : null;
  if (!jobId) {
    return NextResponse.json({ detail: "Missing job_id." }, { status: 400 });
  }

  if (typeof body?.error === "string") {
    // FastAPI's background computation failed before it had a result to send.
    await markJobFailed(jobId, body.error, "Push computation failed on the backend.");
    return NextResponse.json({ ok: true });
  }

  const studyIds = Array.isArray(body?.study_ids)
    ? (body.study_ids as unknown[]).filter((item): item is string => typeof item === "string")
    : [];

  const result = await finalizePush(
    jobId,
    studyIds,
    body?.journey,
    body?.touchpoints,
    body?.studies,
    body?.taxonomy,
    body?.demographics,
    typeof body?.pushed_by === "string" ? body.pushed_by : null
  );

  return NextResponse.json(result);
}
