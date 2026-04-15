import { NextRequest, NextResponse } from "next/server";
import { handleWithDataSource } from "../../../_lib/backend";
import { getScopeContext, parseStudyIdsInput, scopeStudyIds, scopeStudyIdsCsv } from "../../../_lib/access-scope";
import { resolveMarketLens } from "../../../_lib/market-lens";
import { applyMarketFilterToStudyIds } from "../../../_lib/market-filter-scope";
import { expandNseInPayload, expandNseInQuery } from "../../../_lib/demographics";

export const dynamic = "force-dynamic";

function withStudyScope(
  query: Record<string, string>,
  payload: Record<string, unknown>,
  allowedStudyIds: string[] | null
) {
  if (allowedStudyIds === null) return { query, payload };
  const scopedFromQuery = scopeStudyIdsCsv(query.studies || query.study_ids, allowedStudyIds);
  const scopedFromPayload = scopeStudyIds(parseStudyIdsInput(payload.study_ids), allowedStudyIds);
  const merged = scopedFromPayload ?? (scopedFromQuery ? scopedFromQuery.split(",") : []);
  return {
    query: {
      ...query,
      studies: merged.join(","),
      study_ids: merged.join(","),
    },
    payload: {
      ...payload,
      study_ids: merged,
    },
  };
}

type TouchpointRow = Record<string, unknown> & {
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

function applyMarketLensToRow(row: TouchpointRow): TouchpointRow {
  const market = resolveMarketLens({
    sector: typeof row.sector === "string" ? row.sector : null,
    subsector: typeof row.subsector === "string" ? row.subsector : null,
    category: typeof row.category === "string" ? row.category : null,
    market_sector: typeof row.market_sector === "string" ? row.market_sector : null,
    market_subsector: typeof row.market_subsector === "string" ? row.market_subsector : null,
    market_category: typeof row.market_category === "string" ? row.market_category : null,
  });

  return {
    ...row,
    market_sector: market.market_sector,
    market_subsector: market.market_subsector,
    market_category: market.market_category,
    sector: market.market_sector,
    subsector: market.market_subsector,
    category: market.market_category,
  };
}

function matchesMarketSelection(row: TouchpointRow, selection: MarketSelection): boolean {
  const sector = typeof row.sector === "string" ? row.sector : null;
  const subsector = typeof row.subsector === "string" ? row.subsector : null;
  const category = typeof row.category === "string" ? row.category : null;
  if (selection.sector && sector !== selection.sector) return false;
  if (selection.subsector && subsector !== selection.subsector) return false;
  if (selection.category && category !== selection.category) return false;
  return true;
}

function normalizeTouchpointPayloadTaxonomy(payload: unknown, taxonomyView: "market" | "standard"): unknown {
  if (taxonomyView !== "market" || !payload || typeof payload !== "object") return payload;
  const root = payload as Record<string, unknown>;
  const remapRows = (value: unknown) =>
    Array.isArray(value) ? value.map((item) => applyMarketLensToRow((item || {}) as TouchpointRow)) : value;
  return {
    ...root,
    rows: remapRows(root.rows),
  };
}

function applySelectionFilterToTouchpointsPayload(
  payload: unknown,
  taxonomyView: "market" | "standard",
  selection: MarketSelection
): unknown {
  if (taxonomyView !== "market" || !payload || typeof payload !== "object") return payload;
  if (!selection.sector && !selection.subsector && !selection.category) return payload;
  const root = payload as Record<string, unknown>;
  return {
    ...root,
    rows: Array.isArray(root.rows)
      ? root.rows.filter((item) => matchesMarketSelection((item || {}) as TouchpointRow, selection))
      : root.rows,
  };
}

async function respondWithTaxonomyNormalization(
  response: NextResponse,
  taxonomyView: "market" | "standard",
  selection: MarketSelection
) {
  if (!response.ok) return response;
  const payload = await response.json().catch(() => null);
  const normalized = normalizeTouchpointPayloadTaxonomy(payload, taxonomyView);
  const filtered = applySelectionFilterToTouchpointsPayload(normalized, taxonomyView, selection);
  return NextResponse.json(filtered, { status: response.status });
}

export async function GET(request: NextRequest) {
  const scopeContext = await getScopeContext(request);
  if (request.nextUrl.searchParams.get("taxonomy_view") === "standard") {
    console.warn("[touchpoints/table_multi] Ignoring legacy taxonomy_view=standard and forcing market.");
  }
  const taxonomyView = "market" as const;
  const selection: MarketSelection = {
    sector: request.nextUrl.searchParams.get("sector"),
    subsector: request.nextUrl.searchParams.get("subsector"),
    category: request.nextUrl.searchParams.get("category"),
  };
  if (scopeContext.allowedStudyIds && scopeContext.allowedStudyIds.length === 0) {
    return NextResponse.json({ rows: [] });
  }
  const marketScoped = await applyMarketFilterToStudyIds({
    query: { ...expandNseInQuery(Object.fromEntries(request.nextUrl.searchParams.entries())), taxonomy_view: "market" },
    payload: { taxonomy_view: "market" },
    allowedStudyIds: scopeContext.allowedStudyIds,
    preserveTaxonomyFilters: true,
  });
  const scoped = withStudyScope(
    marketScoped.query,
    marketScoped.payload,
    scopeContext.allowedStudyIds
  );
  scoped.query.taxonomy_view = "market";
  scoped.payload.taxonomy_view = "market";
  const queryString = new URLSearchParams(scoped.query).toString();
  const query = queryString ? `?${queryString}` : "";
  const response = await handleWithDataSource(
    request,
    `/analytics/touchpoints/table_multi${query}`,
    "bbs_touchpoints_table_multi",
    {
      query: scoped.query,
      payload: scoped.payload,
    },
    { method: "GET" }
  );
  return respondWithTaxonomyNormalization(response, taxonomyView, selection);
}

export async function POST(request: NextRequest) {
  const scopeContext = await getScopeContext(request);
  const rawPayload = expandNseInPayload((await request.json().catch(() => ({}))) as Record<string, unknown>);
  if (
    request.nextUrl.searchParams.get("taxonomy_view") === "standard" ||
    (typeof rawPayload.taxonomy_view === "string" && rawPayload.taxonomy_view.toLowerCase() === "standard")
  ) {
    console.warn("[touchpoints/table_multi] Ignoring legacy taxonomy_view=standard and forcing market.");
  }
  const payload: Record<string, unknown> = {
    ...rawPayload,
    taxonomy_view: "market",
  };
  const taxonomyView = "market" as const;
  const selection: MarketSelection = {
    sector: typeof payload.sector === "string" ? payload.sector : request.nextUrl.searchParams.get("sector"),
    subsector: typeof payload.subsector === "string" ? payload.subsector : request.nextUrl.searchParams.get("subsector"),
    category: typeof payload.category === "string" ? payload.category : request.nextUrl.searchParams.get("category"),
  };
  if (scopeContext.allowedStudyIds && scopeContext.allowedStudyIds.length === 0) {
    return NextResponse.json({ rows: [] });
  }
  const marketScoped = await applyMarketFilterToStudyIds({
    query: { ...expandNseInQuery(Object.fromEntries(request.nextUrl.searchParams.entries())), taxonomy_view: "market" },
    payload,
    allowedStudyIds: scopeContext.allowedStudyIds,
    preserveTaxonomyFilters: true,
  });
  const scoped = withStudyScope(
    marketScoped.query,
    marketScoped.payload,
    scopeContext.allowedStudyIds
  );
  scoped.query.taxonomy_view = "market";
  scoped.payload.taxonomy_view = "market";
  const queryString = new URLSearchParams(scoped.query).toString();
  const query = queryString ? `?${queryString}` : "";
  const response = await handleWithDataSource(
    request,
    `/analytics/touchpoints/table_multi${query}`,
    "bbs_touchpoints_table_multi",
    {
      query: scoped.query,
      payload: scoped.payload,
    },
    { method: "POST" }
  );
  return respondWithTaxonomyNormalization(response, taxonomyView, selection);
}
