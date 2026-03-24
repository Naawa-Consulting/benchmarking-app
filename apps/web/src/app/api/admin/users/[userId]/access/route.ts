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
    supabaseAdminPostgrest("study_catalog?select=market_sector,market_subsector,market_category"),
  ]);
  const effectiveTaxonomyResult =
    taxonomyResult.response.ok
      ? taxonomyResult
      : await supabaseAdminPostgrest("study_catalog?select=sector,subsector,category");

  const canToggleBrands =
    Array.isArray(permissionsResult.data) &&
    (permissionsResult.data as Array<{ permission?: string }>).some(
      (row) => row.permission === "brands.toggle"
    );

  const scopes = { market_sector: [] as string[], market_subsector: [] as string[], market_category: [] as string[] };
  if (Array.isArray(scopesResult.data)) {
    for (const row of scopesResult.data as Array<{ scope_type?: string; scope_key?: string }>) {
      const key = typeof row.scope_key === "string" ? row.scope_key.trim() : "";
      const type = typeof row.scope_type === "string" ? row.scope_type.toLowerCase() : "";
      if (!key) continue;
      if (type === "market_sector") scopes.market_sector.push(key);
      if (type === "market_subsector") scopes.market_subsector.push(key);
      if (type === "market_category") scopes.market_category.push(key);
    }
  }

  const available = { market_sector: [] as string[], market_subsector: [] as string[], market_category: [] as string[] };
  if (Array.isArray(effectiveTaxonomyResult.data)) {
    const sectorSet = new Set<string>();
    const subsectorSet = new Set<string>();
    const categorySet = new Set<string>();
    for (const row of effectiveTaxonomyResult.data as Array<{
      market_sector?: string | null;
      market_subsector?: string | null;
      market_category?: string | null;
      sector?: string | null;
      subsector?: string | null;
      category?: string | null;
    }>) {
      const sector = row.market_sector || row.sector;
      const subsector = row.market_subsector || row.subsector;
      const category = row.market_category || row.category;
      if (typeof sector === "string" && sector.trim()) sectorSet.add(sector.trim());
      if (typeof row.market_subsector === "string" && row.market_subsector.trim()) {
        subsectorSet.add(row.market_subsector.trim());
      }
      if (typeof subsector === "string" && subsector.trim()) {
        subsectorSet.add(subsector.trim());
      }
      if (typeof row.market_category === "string" && row.market_category.trim()) {
        categorySet.add(row.market_category.trim());
      }
      if (typeof category === "string" && category.trim()) {
        categorySet.add(category.trim());
      }
    }
    available.market_sector = Array.from(sectorSet).sort();
    available.market_subsector = Array.from(subsectorSet).sort();
    available.market_category = Array.from(categorySet).sort();
  }

  return NextResponse.json({
    user_id: userId,
    can_toggle_brands: canToggleBrands,
    scopes: {
      market_sector: normalizeStrings(scopes.market_sector),
      market_subsector: normalizeStrings(scopes.market_subsector),
      market_category: normalizeStrings(scopes.market_category),
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
    scopes?: { market_sector?: string[]; market_subsector?: string[]; market_category?: string[] };
  };

  const canToggleBrands = Boolean(payload.can_toggle_brands);
  const sector = normalizeStrings(payload.scopes?.market_sector ?? []);
  const subsector = normalizeStrings(payload.scopes?.market_subsector ?? []);
  const category = normalizeStrings(payload.scopes?.market_category ?? []);

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
    ...sector.map((scope_key) => ({ user_id: userId, scope_type: "market_sector", scope_key })),
    ...subsector.map((scope_key) => ({ user_id: userId, scope_type: "market_subsector", scope_key })),
    ...category.map((scope_key) => ({ user_id: userId, scope_type: "market_category", scope_key })),
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
    scopes: { market_sector: sector, market_subsector: subsector, market_category: category },
  });
}
