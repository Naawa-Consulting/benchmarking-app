import { createServerClient } from "@supabase/ssr";
import { NextRequest } from "next/server";

export type BbsRole = "owner" | "admin" | "analyst" | "viewer";

export type ScopeType = "sector" | "subsector" | "category";

export type EffectiveScopes = {
  sector: string[];
  subsector: string[];
  category: string[];
};

export type RequestAuthz = {
  user_id: string | null;
  email: string | null;
  role: BbsRole;
  permissions: string[];
  can_toggle_brands: boolean;
  is_admin_module_allowed: boolean;
  effective_scopes: EffectiveScopes;
  can_mutate: boolean;
  is_viewer: boolean;
};

const MUTATING_ROLES = new Set<BbsRole>(["owner", "admin", "analyst"]);
const ADMIN_MODULE_ROLES = new Set<BbsRole>(["owner", "admin"]);
const BRAND_TOGGLE_ROLES = new Set<BbsRole>(["owner", "admin"]);

function isAuthEnabled() {
  return (process.env.BBS_AUTH_MODE || "off").toLowerCase() === "supabase";
}

function asRole(value: unknown): BbsRole {
  const normalized = typeof value === "string" ? value.trim().toLowerCase() : "";
  return normalized === "owner" || normalized === "admin" || normalized === "analyst"
    ? (normalized as BbsRole)
    : "viewer";
}

function canMutate(role: BbsRole) {
  return MUTATING_ROLES.has(role);
}

function canAccessAdminModule(role: BbsRole) {
  return ADMIN_MODULE_ROLES.has(role);
}

function hasRoleBasedBrandToggle(role: BbsRole) {
  return BRAND_TOGGLE_ROLES.has(role);
}

async function fetchRoleWithServiceKey(userId: string): Promise<BbsRole | null> {
  const supabaseUrl = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
  if (!supabaseUrl || !serviceRoleKey) return null;

  const response = await fetch(
    `${supabaseUrl.replace(/\/+$/, "")}/rest/v1/user_roles?select=role&user_id=eq.${encodeURIComponent(
      userId
    )}&limit=1`,
    {
      headers: {
        apikey: serviceRoleKey,
        Authorization: `Bearer ${serviceRoleKey}`,
      },
      cache: "no-store",
    }
  );

  if (!response.ok) return null;
  const rows = (await response.json()) as Array<{ role?: string }>;
  if (!Array.isArray(rows) || rows.length === 0) return null;
  return asRole(rows[0]?.role);
}

async function fetchPermissionsWithServiceKey(role: BbsRole): Promise<string[] | null> {
  const supabaseUrl = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
  if (!supabaseUrl || !serviceRoleKey) return null;

  const response = await fetch(
    `${supabaseUrl.replace(/\/+$/, "")}/rest/v1/role_permissions?select=permission&role=eq.${encodeURIComponent(
      role
    )}`,
    {
      headers: {
        apikey: serviceRoleKey,
        Authorization: `Bearer ${serviceRoleKey}`,
      },
      cache: "no-store",
    }
  );
  if (!response.ok) return null;
  const rows = (await response.json()) as Array<{ permission?: string }>;
  if (!Array.isArray(rows)) return null;
  return rows
    .map((row) => row.permission)
    .filter((value): value is string => typeof value === "string");
}

async function fetchUserPermissionsWithServiceKey(userId: string): Promise<string[] | null> {
  const supabaseUrl = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
  if (!supabaseUrl || !serviceRoleKey) return null;

  const response = await fetch(
    `${supabaseUrl.replace(
      /\/+$/,
      ""
    )}/rest/v1/user_permissions?select=permission&user_id=eq.${encodeURIComponent(userId)}`,
    {
      headers: {
        apikey: serviceRoleKey,
        Authorization: `Bearer ${serviceRoleKey}`,
      },
      cache: "no-store",
    }
  );

  if (!response.ok) return null;
  const rows = (await response.json()) as Array<{ permission?: string }>;
  if (!Array.isArray(rows)) return null;
  return rows
    .map((row) => row.permission)
    .filter((value): value is string => typeof value === "string");
}

async function fetchUserScopesWithServiceKey(userId: string): Promise<EffectiveScopes | null> {
  const supabaseUrl = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
  if (!supabaseUrl || !serviceRoleKey) return null;

  const response = await fetch(
    `${supabaseUrl.replace(
      /\/+$/,
      ""
    )}/rest/v1/user_access_scopes?select=scope_type,scope_key&user_id=eq.${encodeURIComponent(userId)}`,
    {
      headers: {
        apikey: serviceRoleKey,
        Authorization: `Bearer ${serviceRoleKey}`,
      },
      cache: "no-store",
    }
  );

  if (!response.ok) return null;
  const rows = (await response.json()) as Array<{ scope_type?: string; scope_key?: string }>;
  if (!Array.isArray(rows)) return null;

  const sector = new Set<string>();
  const subsector = new Set<string>();
  const category = new Set<string>();
  for (const row of rows) {
    const key = typeof row.scope_key === "string" ? row.scope_key.trim() : "";
    const type = typeof row.scope_type === "string" ? row.scope_type.trim().toLowerCase() : "";
    if (!key) continue;
    if (type === "sector") sector.add(key);
    if (type === "subsector") subsector.add(key);
    if (type === "category") category.add(key);
  }

  return {
    sector: Array.from(sector).sort(),
    subsector: Array.from(subsector).sort(),
    category: Array.from(category).sort(),
  };
}

export function isMutatingDataPath(pathname: string) {
  const clean = pathname.split("?")[0];
  return (
    clean.startsWith("/rules") ||
    clean.startsWith("/mapping") ||
    clean.startsWith("/pipeline/") ||
    clean.startsWith("/study-config") ||
    clean.startsWith("/taxonomy/study") ||
    clean.startsWith("/demographics/config") ||
    clean.startsWith("/question-map/")
  );
}

export async function getRequestAuthz(request: NextRequest): Promise<RequestAuthz> {
  if (!isAuthEnabled()) {
    return {
      user_id: null,
      email: null,
      role: "owner",
      permissions: ["*"],
      can_toggle_brands: true,
      is_admin_module_allowed: true,
      effective_scopes: { sector: [], subsector: [], category: [] },
      can_mutate: true,
      is_viewer: false,
    };
  }

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL || "";
  const supabaseAnonKey =
    process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY ||
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ||
    "";

  if (!supabaseUrl || !supabaseAnonKey) {
    return {
      user_id: null,
      email: null,
      role: "viewer",
      permissions: [],
      can_toggle_brands: false,
      is_admin_module_allowed: false,
      effective_scopes: { sector: [], subsector: [], category: [] },
      can_mutate: false,
      is_viewer: true,
    };
  }

  const supabase = createServerClient(supabaseUrl, supabaseAnonKey, {
    cookies: {
      getAll() {
        return request.cookies.getAll();
      },
      setAll() {
        // No-op for API authz checks.
      },
    },
  });

  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user) {
    return {
      user_id: null,
      email: null,
      role: "viewer",
      permissions: [],
      can_toggle_brands: false,
      is_admin_module_allowed: false,
      effective_scopes: { sector: [], subsector: [], category: [] },
      can_mutate: false,
      is_viewer: true,
    };
  }

  let resolvedRole = await fetchRoleWithServiceKey(user.id);
  if (!resolvedRole) {
    try {
      const { data } = await supabase
        .from("user_roles")
        .select("role")
        .eq("user_id", user.id)
        .maybeSingle();
      resolvedRole = asRole(data?.role);
    } catch {
      resolvedRole = "viewer";
    }
  }

  const servicePermissions = await fetchPermissionsWithServiceKey(resolvedRole);
  let permissions = servicePermissions ?? null;
  if (!permissions) {
    try {
      const { data } = await supabase
        .from("role_permissions")
        .select("permission")
        .eq("role", resolvedRole);
      permissions = Array.isArray(data)
        ? data
            .map((row) => row.permission)
            .filter((value): value is string => typeof value === "string")
        : [];
    } catch {
      permissions = [];
    }
  }

  const userPermissionRows = (await fetchUserPermissionsWithServiceKey(user.id)) ?? [];

  let effectiveScopes =
    (await fetchUserScopesWithServiceKey(user.id)) ??
    ({
      sector: [],
      subsector: [],
      category: [],
    } satisfies EffectiveScopes);

  if (!effectiveScopes) {
    effectiveScopes = { sector: [], subsector: [], category: [] };
  }

  const mergedPermissions = Array.from(new Set([...(permissions ?? []), ...userPermissionRows]));
  const canToggleBrands =
    hasRoleBasedBrandToggle(resolvedRole) || mergedPermissions.includes("brands.toggle");

  return {
    user_id: user.id,
    email: user.email ?? null,
    role: resolvedRole,
    permissions: mergedPermissions,
    can_toggle_brands: canToggleBrands,
    is_admin_module_allowed: canAccessAdminModule(resolvedRole),
    effective_scopes: effectiveScopes,
    can_mutate: canMutate(resolvedRole),
    is_viewer: resolvedRole === "viewer",
  };
}
