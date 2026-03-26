import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../../_lib/authz";
import { supabaseAdminPostgrest, supabaseAuthAdmin } from "../../../_lib/supabase-admin";

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
      authz,
    };
  }
  return { denied: null, authz };
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

  const payload = (await request.json().catch(() => ({}))) as { role?: string; disabled?: boolean };
  const role = normalizeRole(payload.role);
  const disabled = typeof payload.disabled === "boolean" ? payload.disabled : undefined;
  if (payload.role != null && !role) {
    return NextResponse.json({ detail: "Invalid role." }, { status: 400 });
  }

  if (!role && disabled == null) {
    return NextResponse.json({ detail: "No changes supplied." }, { status: 400 });
  }

  if (role) {
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
  }

  if (disabled != null) {
    const updateAuth = await supabaseAuthAdmin(`users/${encodeURIComponent(userId)}`, {
      method: "PUT",
      body: {
        ban_duration: disabled ? "876000h" : "none",
      },
    });
    if (!updateAuth.response.ok) {
      return NextResponse.json(
        { detail: "Failed to update user enabled state.", error: updateAuth.data },
        { status: updateAuth.response.status || 500 }
      );
    }
  }

  return NextResponse.json({ ok: true, user_id: userId, role: role || null, disabled: disabled ?? null });
}

export async function DELETE(
  request: NextRequest,
  context: { params: { userId: string } }
) {
  const { denied, authz } = await requireAdminAccess(request);
  if (denied) return denied;

  const userId = context.params.userId;
  if (!userId) {
    return NextResponse.json({ detail: "User id is required." }, { status: 400 });
  }

  if (authz.user_id && authz.user_id === userId) {
    return NextResponse.json({ detail: "You cannot delete your own user." }, { status: 400 });
  }

  await Promise.all([
    supabaseAdminPostgrest(`user_access_scopes?user_id=eq.${encodeURIComponent(userId)}`, { method: "DELETE" }),
    supabaseAdminPostgrest(`user_permissions?user_id=eq.${encodeURIComponent(userId)}`, { method: "DELETE" }),
    supabaseAdminPostgrest(`user_roles?user_id=eq.${encodeURIComponent(userId)}`, { method: "DELETE" }),
  ]);

  const deleted = await supabaseAuthAdmin(`users/${encodeURIComponent(userId)}`, { method: "DELETE" });
  if (!deleted.response.ok) {
    return NextResponse.json(
      { detail: "Failed to delete auth user.", error: deleted.data },
      { status: deleted.response.status || 500 }
    );
  }

  return NextResponse.json({ ok: true, user_id: userId, deleted: true });
}
