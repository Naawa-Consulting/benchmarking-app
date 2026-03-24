import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../../../_lib/authz";
import { supabaseAdminPostgrest } from "../../../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

function normalizeStrings(values: unknown): string[] {
  if (!Array.isArray(values)) return [];
  return Array.from(
    new Set(
      values
        .map((value) => (typeof value === "string" ? value.trim() : ""))
        .filter(Boolean)
    )
  ).sort();
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

export async function GET(
  request: NextRequest,
  context: { params: { userId: string } }
) {
  const { denied } = await requireAdminAccess(request);
  if (denied) return denied;

  const userId = context.params.userId;
  if (!userId) {
    return NextResponse.json({ detail: "User id is required." }, { status: 400 });
  }

  const [permissionsResult, scopesResult, taxonomyResult] = await Promise.all([
    supabaseAdminPostgrest(
      `user_permissions?select=permission&user_id=eq.${encodeURIComponent(userId)}`
    ),
    supabaseAdminPostgrest(
      `user_access_scopes?select=scope_type,scope_key&user_id=eq.${encodeURIComponent(userId)}`
    ),
    supabaseAdminPostgrest("study_catalog?select=sector,subsector,category"),
  ]);

  const canToggleBrands =
    Array.isArray(permissionsResult.data) &&
    (permissionsResult.data as Array<{ permission?: string }>).some(
      (row) => row.permission === "brands.toggle"
    );

  const scopes = { sector: [] as string[], subsector: [] as string[], category: [] as string[] };
  if (Array.isArray(scopesResult.data)) {
    for (const row of scopesResult.data as Array<{ scope_type?: string; scope_key?: string }>) {
      const key = typeof row.scope_key === "string" ? row.scope_key.trim() : "";
      const type = typeof row.scope_type === "string" ? row.scope_type.toLowerCase() : "";
      if (!key) continue;
      if (type === "sector") scopes.sector.push(key);
      if (type === "subsector") scopes.subsector.push(key);
      if (type === "category") scopes.category.push(key);
    }
  }

  const available = { sector: [] as string[], subsector: [] as string[], category: [] as string[] };
  if (Array.isArray(taxonomyResult.data)) {
    const sectorSet = new Set<string>();
    const subsectorSet = new Set<string>();
    const categorySet = new Set<string>();
    for (const row of taxonomyResult.data as Array<{
      sector?: string | null;
      subsector?: string | null;
      category?: string | null;
    }>) {
      if (typeof row.sector === "string" && row.sector.trim()) sectorSet.add(row.sector.trim());
      if (typeof row.subsector === "string" && row.subsector.trim()) subsectorSet.add(row.subsector.trim());
      if (typeof row.category === "string" && row.category.trim()) categorySet.add(row.category.trim());
    }
    available.sector = Array.from(sectorSet).sort();
    available.subsector = Array.from(subsectorSet).sort();
    available.category = Array.from(categorySet).sort();
  }

  return NextResponse.json({
    user_id: userId,
    can_toggle_brands: canToggleBrands,
    scopes: {
      sector: normalizeStrings(scopes.sector),
      subsector: normalizeStrings(scopes.subsector),
      category: normalizeStrings(scopes.category),
    },
    available,
  });
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

  const payload = (await request.json().catch(() => ({}))) as {
    can_toggle_brands?: boolean;
    scopes?: { sector?: string[]; subsector?: string[]; category?: string[] };
  };

  const canToggleBrands = Boolean(payload.can_toggle_brands);
  const sector = normalizeStrings(payload.scopes?.sector ?? []);
  const subsector = normalizeStrings(payload.scopes?.subsector ?? []);
  const category = normalizeStrings(payload.scopes?.category ?? []);

  const deletePerms = await supabaseAdminPostgrest(`user_permissions?user_id=eq.${encodeURIComponent(userId)}`, {
    method: "DELETE",
  });
  if (!deletePerms.response.ok) {
    return NextResponse.json(
      { detail: "Failed to clear user permissions.", error: deletePerms.data },
      { status: deletePerms.response.status || 500 }
    );
  }

  if (canToggleBrands) {
    const insertPerms = await supabaseAdminPostgrest("user_permissions", {
      method: "POST",
      headers: { Prefer: "resolution=ignore-duplicates,return=minimal" },
      body: [{ user_id: userId, permission: "brands.toggle" }],
    });
    if (!insertPerms.response.ok) {
      return NextResponse.json(
        { detail: "Failed to save user permission brands.toggle.", error: insertPerms.data },
        { status: insertPerms.response.status || 500 }
      );
    }
  }

  const deleteScopes = await supabaseAdminPostgrest(
    `user_access_scopes?user_id=eq.${encodeURIComponent(userId)}`,
    {
      method: "DELETE",
    }
  );
  if (!deleteScopes.response.ok) {
    return NextResponse.json(
      { detail: "Failed to clear user scopes.", error: deleteScopes.data },
      { status: deleteScopes.response.status || 500 }
    );
  }

  const scopeRows = [
    ...sector.map((scope_key) => ({ user_id: userId, scope_type: "sector", scope_key })),
    ...subsector.map((scope_key) => ({ user_id: userId, scope_type: "subsector", scope_key })),
    ...category.map((scope_key) => ({ user_id: userId, scope_type: "category", scope_key })),
  ];

  if (scopeRows.length) {
    const insertScopes = await supabaseAdminPostgrest("user_access_scopes", {
      method: "POST",
      headers: { Prefer: "resolution=ignore-duplicates,return=minimal" },
      body: scopeRows,
    });
    if (!insertScopes.response.ok) {
      return NextResponse.json(
        { detail: "Failed to save user scopes.", error: insertScopes.data },
        { status: insertScopes.response.status || 500 }
      );
    }
  }

  return NextResponse.json({
    ok: true,
    user_id: userId,
    can_toggle_brands: canToggleBrands,
    scopes: { sector, subsector, category },
  });
}
