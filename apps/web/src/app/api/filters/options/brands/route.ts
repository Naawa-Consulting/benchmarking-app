import { NextRequest, NextResponse } from "next/server";
import { handleWithDataSource } from "../../../_lib/backend";
import { getScopeContext, parseStudyIdsInput, scopeStudyIds } from "../../../_lib/access-scope";
import { resolveMarketLens } from "../../../_lib/market-lens";
import { applyMarketFilterToStudyIds } from "../../../_lib/market-filter-scope";

export const dynamic = "force-dynamic";

// Lightweight brand list for the global Scope Bar. Replaces ScopeProvider's previous
// approach of running a full touchpoints aggregation (5.01 MB measured) plus, when the
// selection had no touchpoint rows, a second full journey scan (2.74 MB) — and keeping
// only `row.brand` from either. See supabase/sql/029_brand_options_rpc.sql and
// BITACORA.md 2026-09-01 (cont. 9).
//
// The backend returns distinct (brand x taxonomy) tuples; the market lens and the
// market-view selection filter are resolved here, mirroring what
// analytics/journey/table_multi/route.ts already does to its rows.

type BrandOptionRow = {
  brand?: string | null;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  market_sector?: string | null;
  market_subsector?: string | null;
  market_category?: string | null;
};

type MarketSelection = {
  sector: string | null;
  subsector: string | null;
  category: string | null;
};

function matchesMarketSelection(row: BrandOptionRow, selection: MarketSelection): boolean {
  if (!selection.sector && !selection.subsector && !selection.category) return true;
  const market = resolveMarketLens({
    sector: row.sector ?? null,
    subsector: row.subsector ?? null,
    category: row.category ?? null,
    market_sector: row.market_sector ?? null,
    market_subsector: row.market_subsector ?? null,
    market_category: row.market_category ?? null,
  });
  if (selection.sector && market.market_sector !== selection.sector) return false;
  if (selection.subsector && market.market_subsector !== selection.subsector) return false;
  if (selection.category && market.market_category !== selection.category) return false;
  return true;
}

export async function POST(request: NextRequest) {
  const scopeContext = await getScopeContext(request);
  const rawPayload = ((await request.json().catch(() => ({}))) || {}) as Record<string, unknown>;

  const selection: MarketSelection = {
    sector: typeof rawPayload.sector === "string" && rawPayload.sector ? rawPayload.sector : null,
    subsector:
      typeof rawPayload.subsector === "string" && rawPayload.subsector ? rawPayload.subsector : null,
    category:
      typeof rawPayload.category === "string" && rawPayload.category ? rawPayload.category : null,
  };

  if (scopeContext.allowedStudyIds && scopeContext.allowedStudyIds.length === 0) {
    return NextResponse.json({ items: [], brands: [] });
  }

  // Only study_ids and years reach the backend — the taxonomy selection is applied as a
  // study-id filter by applyMarketFilterToStudyIds, then re-checked per row below.
  const payload: Record<string, unknown> = {
    study_ids: rawPayload.study_ids ?? null,
    years: rawPayload.years ?? null,
    sector: selection.sector,
    subsector: selection.subsector,
    category: selection.category,
    taxonomy_view: "market",
  };

  const marketScoped = await applyMarketFilterToStudyIds({
    query: { taxonomy_view: "market" },
    payload,
    allowedStudyIds: scopeContext.allowedStudyIds,
  });

  // scopeStudyIds(null, allowed) returns the full allowed set, which is what a viewer
  // with no explicit study selection should get.
  const studyIds =
    scopeStudyIds(parseStudyIdsInput(marketScoped.payload.study_ids), scopeContext.allowedStudyIds) ?? [];

  const years = Array.isArray(marketScoped.payload.years) ? marketScoped.payload.years : null;
  const backendPayload: Record<string, unknown> = {
    study_ids: studyIds.length ? studyIds : null,
    years,
  };

  const legacyQuery = new URLSearchParams();
  if (studyIds.length) legacyQuery.set("study_ids", studyIds.join(","));
  if (years && years.length) legacyQuery.set("years", years.join(","));
  const legacyQueryString = legacyQuery.toString();

  const response = await handleWithDataSource(
    request,
    `/filters/options/brands${legacyQueryString ? `?${legacyQueryString}` : ""}`,
    "bbs_brand_options",
    { query: {}, payload: backendPayload },
    { method: "GET" }
  );

  if (!response.ok) return response;
  const data = (await response.json().catch(() => null)) as { items?: BrandOptionRow[] } | null;
  const items = Array.isArray(data?.items) ? (data?.items as BrandOptionRow[]) : [];

  const brands = Array.from(
    new Set(
      items
        .filter((row) => matchesMarketSelection(row, selection))
        .map((row) => (typeof row.brand === "string" ? row.brand.trim() : ""))
        .filter(Boolean)
    )
  ).sort((a, b) => a.localeCompare(b));

  return NextResponse.json({ brands, items: [] });
}
