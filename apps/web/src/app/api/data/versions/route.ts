import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../_lib/authz";
import { supabaseAdminPostgrest } from "../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const authz = await getRequestAuthz(request);
  const authRequired = (process.env.BBS_AUTH_MODE || "off").toLowerCase() === "supabase";
  if (authRequired && !authz.user_id) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }

  const limitRaw = request.nextUrl.searchParams.get("limit");
  const limit = Number.isFinite(Number(limitRaw)) ? Math.max(1, Math.min(200, Number(limitRaw))) : 20;

  const { response, data } = await supabaseAdminPostgrest(
    `data_versions?select=*&order=created_at.desc&limit=${limit}`
  );
  if (!response.ok) {
    return NextResponse.json(
      { detail: "Failed to load data versions.", error: data },
      { status: response.status || 500 }
    );
  }
  return NextResponse.json({
    ok: true,
    items: Array.isArray(data) ? data : [],
  });
}

export async function POST(request: NextRequest) {
  const authz = await getRequestAuthz(request);
  const authRequired = (process.env.BBS_AUTH_MODE || "off").toLowerCase() === "supabase";
  if (authRequired && !authz.user_id) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }
  if (!authz.can_mutate) {
    return NextResponse.json({ detail: "Forbidden: insufficient permissions" }, { status: 403 });
  }

  const payload = (await request.json().catch(() => null)) as
    | { label?: string; notes?: string | null; source_job_id?: string | null }
    | null;

  const label = (payload?.label || "").trim();
  if (!label) {
    return NextResponse.json({ detail: "Version label is required." }, { status: 400 });
  }

  const sourceJobId = payload?.source_job_id?.trim();
  const source_job_id = sourceJobId && sourceJobId.length > 0 ? sourceJobId : null;

  if (source_job_id) {
    const check = await supabaseAdminPostgrest(
      `ingestion_jobs?select=id,status&id=eq.${source_job_id}&limit=1`
    );
    if (!check.response.ok || !Array.isArray(check.data) || !check.data[0]) {
      return NextResponse.json(
        { detail: "Source job not found.", error: check.data },
        { status: 400 }
      );
    }
    const status = String((check.data[0] as { status?: string }).status || "");
    if (status !== "success") {
      return NextResponse.json(
        { detail: "Source job must be in success state before creating a version." },
        { status: 400 }
      );
    }
  }

  const insert = await supabaseAdminPostgrest("data_versions", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: [
      {
        label,
        notes: payload?.notes || null,
        source_job_id,
        created_by: authz.user_id,
        status: "draft",
      },
    ],
  });
  if (!insert.response.ok || !Array.isArray(insert.data) || !insert.data[0]) {
    return NextResponse.json(
      { detail: "Failed to create data version.", error: insert.data },
      { status: insert.response.status || 500 }
    );
  }

  return NextResponse.json({ ok: true, item: insert.data[0] });
}
