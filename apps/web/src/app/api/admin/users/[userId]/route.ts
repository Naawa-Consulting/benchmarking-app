import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../../_lib/authz";
import { supabaseAdminPostgrest } from "../../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

const ALLOWED_ROLES = new Set(["owner", "admin", "analyst", "viewer"]);

function normalizeRole(value: unknown) {
  const role = typeof value === "string" ? value.trim().toLowerCase() : "";
  return ALLOWED_ROLES.has(role) ? role : null;
}

async function requireAdminAccess(request: NextRequest) {
  const authz = await getRequestAuthz(request);
  if (!authz.is_admin_module_allowed) {
    return {
      denied: NextResponse.json({ detail: "Forbidden: insufficient permissions" }, { status: 403 }),
    };
  }
  return { denied: null };
}

export async function PATCH(
  request: NextRequest,
  context: { params: { userId: string } }
) {
  const { denied } = await requireAdminAccess(request);
  if (denied) return denied;

  const userId = context.params.userId;
  if (!userId) {
    return NextResponse.json({ detail: "User id is required." }, { status: 400 });
  }

  const payload = (await request.json().catch(() => ({}))) as { role?: string };
  const role = normalizeRole(payload.role);
  if (!role) {
    return NextResponse.json({ detail: "Invalid role." }, { status: 400 });
  }

  const { response, data } = await supabaseAdminPostgrest("user_roles?on_conflict=user_id", {
    method: "POST",
    headers: { Prefer: "resolution=merge-duplicates,return=representation" },
    body: [{ user_id: userId, role }],
  });

  if (!response.ok) {
    return NextResponse.json(
      { detail: "Failed to update role.", error: data },
      { status: response.status || 500 }
    );
  }

  return NextResponse.json({ ok: true, user_id: userId, role });
}
