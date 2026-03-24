import { NextRequest } from "next/server";
import { getRequestAuthz } from "./authz";
import { supabaseAdminPostgrest } from "./supabase-admin";

type StudyCatalogRow = {
  study_id: string;
  market_sector?: string | null;
  market_subsector?: string | null;
  market_category?: string | null;
};

type ScopeContext = {
  authz: Awaited<ReturnType<typeof getRequestAuthz>>;
  allowedStudyIds: string[] | null;
};

function normalize(value: string | null | undefined) {
  return (value || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

function parseStudyIdsCsv(value: string | null | undefined) {
  if (!value) return [];
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function intersectIds(requested: string[] | null, allowed: string[]) {
  if (!requested || requested.length === 0) return allowed;
  const allowedSet = new Set(allowed);
  return requested.filter((id) => allowedSet.has(id));
}

export async function getScopeContext(request: NextRequest): Promise<ScopeContext> {
  const authz = await getRequestAuthz(request);
  if (!authz.is_viewer) {
    return { authz, allowedStudyIds: null };
  }

  const hasScopes =
    authz.effective_scopes.market_sector.length > 0 ||
    authz.effective_scopes.market_subsector.length > 0 ||
    authz.effective_scopes.market_category.length > 0;
  if (!hasScopes) {
    return { authz, allowedStudyIds: [] };
  }

  const baseRows: StudyCatalogRow[] = [];

  const studyCatalogResult = await supabaseAdminPostgrest(
    "study_catalog?select=study_id,market_sector,market_subsector,market_category"
  );
  if (studyCatalogResult.response.ok && Array.isArray(studyCatalogResult.data)) {
    baseRows.push(...(studyCatalogResult.data as StudyCatalogRow[]));
  } else if (!studyCatalogResult.response.ok) {
    const fallbackStudyCatalog = await supabaseAdminPostgrest(
      "study_catalog?select=study_id,sector,subsector,category"
    );
    if (fallbackStudyCatalog.response.ok && Array.isArray(fallbackStudyCatalog.data)) {
      baseRows.push(
        ...(fallbackStudyCatalog.data as Array<{
          study_id: string;
          sector?: string | null;
          subsector?: string | null;
          category?: string | null;
        }>).map((row) => ({
          study_id: row.study_id,
          market_sector: row.sector,
          market_subsector: row.subsector,
          market_category: row.category,
        }))
      );
    }
  }

  if (baseRows.length === 0) {
    const [journeyResult, touchpointResult] = await Promise.all([
      supabaseAdminPostgrest("journey_metrics?select=study_id,market_sector,market_subsector,market_category"),
      supabaseAdminPostgrest("touchpoint_metrics?select=study_id,market_sector,market_subsector,market_category"),
    ]);
    if (journeyResult.response.ok && Array.isArray(journeyResult.data)) {
      baseRows.push(...(journeyResult.data as StudyCatalogRow[]));
    }
    if (touchpointResult.response.ok && Array.isArray(touchpointResult.data)) {
      baseRows.push(...(touchpointResult.data as StudyCatalogRow[]));
    }
  }

  const sectorSet = new Set(authz.effective_scopes.market_sector.map(normalize));
  const subsectorSet = new Set(authz.effective_scopes.market_subsector.map(normalize));
  const categorySet = new Set(authz.effective_scopes.market_category.map(normalize));

  const allowedStudyIds = baseRows
    .filter((row) => {
      const sector = normalize(row.market_sector);
      const subsector = normalize(row.market_subsector);
      const category = normalize(row.market_category);
      return categorySet.has(category) || subsectorSet.has(subsector) || sectorSet.has(sector);
    })
    .map((row) => row.study_id)
    .filter((value) => typeof value === "string" && value.trim().length > 0);

  if (allowedStudyIds.length === 0) {
    const fallbackRows: StudyCatalogRow[] = [];
    const [journeyResult, touchpointResult] = await Promise.all([
      supabaseAdminPostgrest("journey_metrics?select=study_id,market_sector,market_subsector,market_category"),
      supabaseAdminPostgrest("touchpoint_metrics?select=study_id,market_sector,market_subsector,market_category"),
    ]);
    if (journeyResult.response.ok && Array.isArray(journeyResult.data)) {
      fallbackRows.push(...(journeyResult.data as StudyCatalogRow[]));
    }
    if (touchpointResult.response.ok && Array.isArray(touchpointResult.data)) {
      fallbackRows.push(...(touchpointResult.data as StudyCatalogRow[]));
    }

    const fallbackAllowed = fallbackRows
      .filter((row) => {
        const sector = normalize(row.market_sector);
        const subsector = normalize(row.market_subsector);
        const category = normalize(row.market_category);
        return categorySet.has(category) || subsectorSet.has(subsector) || sectorSet.has(sector);
      })
      .map((row) => row.study_id)
      .filter((value) => typeof value === "string" && value.trim().length > 0);

    return { authz, allowedStudyIds: Array.from(new Set(fallbackAllowed)).sort() };
  }

  return { authz, allowedStudyIds: Array.from(new Set(allowedStudyIds)).sort() };
}

export function scopeStudyIds(
  requestedStudyIds: string[] | null,
  allowedStudyIds: string[] | null
): string[] | null {
  if (allowedStudyIds === null) return requestedStudyIds;
  return intersectIds(requestedStudyIds, allowedStudyIds);
}

export function scopeStudyIdsCsv(
  requestedCsv: string | null | undefined,
  allowedStudyIds: string[] | null
): string | null {
  const requested = parseStudyIdsCsv(requestedCsv);
  const scoped = scopeStudyIds(requested.length ? requested : null, allowedStudyIds);
  if (!scoped || scoped.length === 0) return null;
  return scoped.join(",");
}

export function parseStudyIdsInput(input: unknown): string[] | null {
  if (Array.isArray(input)) {
    const ids = input
      .map((value) => (typeof value === "string" ? value.trim() : ""))
      .filter(Boolean);
    return ids.length ? ids : null;
  }
  if (typeof input === "string") {
    const ids = parseStudyIdsCsv(input);
    return ids.length ? ids : null;
  }
  return null;
}
