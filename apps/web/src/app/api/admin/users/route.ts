import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../_lib/authz";
import { getSupabaseAdminConfig, supabaseAdminPostgrest, supabaseAuthAdmin } from "../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

type AuthAdminUser = {
  id?: string;
  email?: string | null;
  created_at?: string | null;
  last_sign_in_at?: string | null;
  email_confirmed_at?: string | null;
  banned_until?: string | null;
};

const ALLOWED_ROLES = new Set(["owner", "admin", "analyst", "viewer"]);

function normalizeRole(value: unknown) {
  const role = typeof value === "string" ? value.trim().toLowerCase() : "";
  return ALLOWED_ROLES.has(role) ? role : "viewer";
}

async function supabaseAuthRequest(
  path: string,
  init: {
    method?: "GET" | "POST" | "PATCH" | "DELETE";
    body?: unknown;
    headers?: Record<string, string>;
  } = {}
) {
  const { url, serviceRoleKey } = getSupabaseAdminConfig();
  const method = init.method || "GET";
  const response = await fetch(`${url}/auth/v1/${path.replace(/^\/+/, "")}`, {
    method,
    headers: {
      apikey: serviceRoleKey,
      Authorization: `Bearer ${serviceRoleKey}`,
      "Content-Type": "application/json",
      ...init.headers,
    },
    body: init.body == null ? undefined : JSON.stringify(init.body),
    cache: "no-store",
  });
  const text = await response.text();
  let data: unknown = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  return { response, data };
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

export async function GET(request: NextRequest) {
  const { denied } = await requireAdminAccess(request);
  if (denied) return denied;

  const [usersResult, rolesResult, permissionsResult, scopesResult] = await Promise.all([
    supabaseAuthAdmin("users?per_page=1000&page=1"),
    supabaseAdminPostgrest("user_roles?select=user_id,role"),
    supabaseAdminPostgrest("user_permissions?select=user_id,permission"),
    supabaseAdminPostgrest("user_access_scopes?select=user_id,scope_type"),
  ]);

  if (!usersResult.response.ok) {
    return NextResponse.json(
      { detail: "Failed to load auth users.", error: usersResult.data },
      { status: usersResult.response.status || 500 }
    );
  }

  const roleByUser = new Map<string, string>();
  if (Array.isArray(rolesResult.data)) {
    for (const row of rolesResult.data as Array<{ user_id?: string; role?: string }>) {
      if (typeof row.user_id === "string") {
        roleByUser.set(row.user_id, normalizeRole(row.role));
      }
    }
  }

  const canToggleByUser = new Set<string>();
  if (Array.isArray(permissionsResult.data)) {
    for (const row of permissionsResult.data as Array<{ user_id?: string; permission?: string }>) {
      if (typeof row.user_id === "string" && row.permission === "brands.toggle") {
        canToggleByUser.add(row.user_id);
      }
    }
  }

  const scopeCountsByUser = new Map<
    string,
    { market_sector: number; market_subsector: number; market_category: number }
  >();
  if (Array.isArray(scopesResult.data)) {
    for (const row of scopesResult.data as Array<{ user_id?: string; scope_type?: string }>) {
      if (typeof row.user_id !== "string") continue;
      const current = scopeCountsByUser.get(row.user_id) || {
        market_sector: 0,
        market_subsector: 0,
        market_category: 0,
      };
      const type = typeof row.scope_type === "string" ? row.scope_type.toLowerCase() : "";
      if (type === "market_sector") current.market_sector += 1;
      if (type === "market_subsector") current.market_subsector += 1;
      if (type === "market_category") current.market_category += 1;
      scopeCountsByUser.set(row.user_id, current);
    }
  }

  const usersPayload = usersResult.data as { users?: AuthAdminUser[] };
  const users = Array.isArray(usersPayload?.users) ? usersPayload.users : [];
  const items = users
    .map((user) => {
      const id = typeof user.id === "string" ? user.id : "";
      if (!id) return null;
      const role = normalizeRole(roleByUser.get(id));
      return {
        id,
        email: typeof user.email === "string" ? user.email : null,
        role,
        created_at: user.created_at ?? null,
        last_sign_in_at: user.last_sign_in_at ?? null,
        email_confirmed_at: user.email_confirmed_at ?? null,
        disabled: Boolean(user.banned_until),
        can_toggle_brands: role === "owner" || role === "admin" || canToggleByUser.has(id),
        scope_counts: scopeCountsByUser.get(id) || { market_sector: 0, market_subsector: 0, market_category: 0 },
      };
    })
    .filter((item): item is NonNullable<typeof item> => Boolean(item))
    .sort((a, b) => {
      const emailA = a.email || "";
      const emailB = b.email || "";
      return emailA.localeCompare(emailB);
    });

  return NextResponse.json({ items });
}

export async function POST(request: NextRequest) {
  const { denied } = await requireAdminAccess(request);
  if (denied) return denied;

  const payload = (await request.json().catch(() => ({}))) as { email?: string; role?: string };
  const email = typeof payload.email === "string" ? payload.email.trim().toLowerCase() : "";
  const role = normalizeRole(payload.role);
  if (!email) {
    return NextResponse.json({ detail: "Email is required." }, { status: 400 });
  }

  const createResult = await supabaseAuthAdmin("users", {
    method: "POST",
    body: {
      email,
      email_confirm: true,
      user_metadata: { invited_by_bbs_admin: true },
    },
  });

  if (!createResult.response.ok) {
    return NextResponse.json(
      { detail: "Failed to create user.", error: createResult.data },
      { status: createResult.response.status || 500 }
    );
  }

  const created = createResult.data as AuthAdminUser;
  const userId = typeof created?.id === "string" ? created.id : "";
  if (!userId) {
    return NextResponse.json({ detail: "User created without id." }, { status: 500 });
  }

  await supabaseAdminPostgrest("user_roles?on_conflict=user_id", {
    method: "POST",
    headers: { Prefer: "resolution=merge-duplicates,return=representation" },
    body: [{ user_id: userId, role }],
  });

  const siteUrl = (process.env.NEXT_PUBLIC_SITE_URL || "http://localhost:3000").replace(/\/+$/, "");
  const redirectTo = `${siteUrl}/auth/reset`;

  let inviteMethod: "invite" | "recover" | "generate_link" = "invite";
  let inviteSent = false;
  let inviteError: unknown = null;
  let manualLink: string | null = null;

  const inviteResult = await supabaseAuthRequest("invite", {
    method: "POST",
    body: { email, data: { invited_by_bbs_admin: true }, redirect_to: redirectTo },
  });
  if (inviteResult.response.ok) {
    inviteSent = true;
  } else {
    inviteMethod = "recover";
    const recoverResult = await supabaseAuthRequest("recover", {
      method: "POST",
      body: { email, redirect_to: redirectTo },
    });
    if (recoverResult.response.ok) {
      inviteSent = true;
    } else {
      inviteMethod = "generate_link";
      const recoveryResult = await supabaseAuthAdmin("generate_link", {
        method: "POST",
        body: {
          type: "recovery",
          email,
          options: { redirectTo, redirect_to: redirectTo },
        },
      });
      if (recoveryResult.response.ok) {
        const payload = recoveryResult.data as { properties?: { action_link?: string | null } } | null;
        manualLink = payload?.properties?.action_link || null;
      } else {
        inviteError = recoveryResult.data;
      }
      if (!inviteError) {
        inviteError = { invite: inviteResult.data, recover: recoverResult.data };
      }
    }
  }

  return NextResponse.json({
    ok: true,
    user: {
      id: userId,
      email,
      role,
    },
    invite: {
      sent: inviteSent,
      method: inviteMethod,
      manual_link: manualLink,
      error: inviteSent ? null : inviteError,
    },
  });
}
