import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../../_lib/authz";
import { supabaseAdminPostgrest } from "../../../_lib/supabase-admin";

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
    `ingestion_jobs?select=id,status,operation,created_at,started_at,finished_at,error_message,payload,requested_by&operation=eq.push_snapshot&order=created_at.desc&limit=${limit}`
  );
  if (!response.ok) {
    return NextResponse.json(
      { detail: "Failed to load publish history.", error: data },
      { status: response.status || 500 }
    );
  }

  return NextResponse.json({
    ok: true,
    items: Array.isArray(data) ? data : [],
  });
}
