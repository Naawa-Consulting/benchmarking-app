import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../../../_lib/authz";
import { supabaseAdminPostgrest } from "../../../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

function isValidUuid(value: string) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    value
  );
}

export async function POST(
  request: NextRequest,
  context: { params: { versionId: string } }
) {
  const authz = await getRequestAuthz(request);
  if (!authz.user_id) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }
  if (!authz.can_mutate) {
    return NextResponse.json({ detail: "Forbidden: insufficient permissions" }, { status: 403 });
  }

  const versionId = context.params.versionId;
  if (!isValidUuid(versionId)) {
    return NextResponse.json({ detail: "Invalid version id." }, { status: 400 });
  }

  const check = await supabaseAdminPostgrest(
    `data_versions?select=id,status,label&id=eq.${versionId}&limit=1`
  );
  if (!check.response.ok || !Array.isArray(check.data) || !check.data[0]) {
    return NextResponse.json({ detail: "Version not found." }, { status: 404 });
  }

  const archived = await supabaseAdminPostgrest("data_versions?status=eq.published", {
    method: "PATCH",
    body: { status: "archived" },
  });
  if (!archived.response.ok) {
    return NextResponse.json(
      { detail: "Failed to archive previous published versions.", error: archived.data },
      { status: archived.response.status || 500 }
    );
  }

  const published = await supabaseAdminPostgrest(`data_versions?id=eq.${versionId}`, {
    method: "PATCH",
    headers: { Prefer: "return=representation" },
    body: {
      status: "published",
      published_at: new Date().toISOString(),
    },
  });
  if (!published.response.ok || !Array.isArray(published.data) || !published.data[0]) {
    return NextResponse.json(
      { detail: "Failed to publish data version.", error: published.data },
      { status: published.response.status || 500 }
    );
  }

  return NextResponse.json({ ok: true, item: published.data[0] });
}
