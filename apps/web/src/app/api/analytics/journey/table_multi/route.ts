import { NextRequest, NextResponse } from "next/server";
import { handleWithDataSource } from "../../../_lib/backend";
import { getScopeContext, parseStudyIdsInput, scopeStudyIds, scopeStudyIdsCsv } from "../../../_lib/access-scope";
import { resolveMarketLens } from "../../../_lib/market-lens";
import { applyMarketFilterToStudyIds } from "../../../_lib/market-filter-scope";
import { expandNseInPayload, expandNseInQuery } from "../../../_lib/demographics";

export const dynamic = "force-dynamic";

type JourneyRow = Record<string, unknown> & {
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

function applyMarketLensToRow(row: JourneyRow): JourneyRow {
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
    // Keep legacy keys aligned with current chart/table consumers.
    sector: market.market_sector,
    subsector: market.market_subsector,
    category: market.market_category,
  };
}

function normalizeJourneyTaxonomyPayload(payload: unknown, taxonomyView: "market" | "standard"): unknown {
  if (taxonomyView !== "market" || !payload || typeof payload !== "object") return payload;
  const root = payload as Record<string, unknown>;
  const remapArray = (value: unknown) =>
    Array.isArray(value) ? value.map((item) => applyMarketLensToRow((item || {}) as JourneyRow)) : value;

  return {
    ...root,
    rows: remapArray(root.rows),
    selection_rows: remapArray(root.selection_rows),
    global_rows: remapArray(root.global_rows),
  };
}

function matchesMarketSelection(row: JourneyRow, selection: MarketSelection): boolean {
  const sector = typeof row.sector === "string" ? row.sector : null;
  const subsector = typeof row.subsector === "string" ? row.subsector : null;
  const category = typeof row.category === "string" ? row.category : null;
  if (selection.sector && sector !== selection.sector) return false;
  if (selection.subsector && subsector !== selection.subsector) return false;
  if (selection.category && category !== selection.category) return false;
  return true;
}

function applySelectionFilterToJourneyPayload(
  payload: unknown,
  taxonomyView: "market" | "standard",
  selection: MarketSelection
): unknown {
  if (taxonomyView !== "market" || !payload || typeof payload !== "object") return payload;
  if (!selection.sector && !selection.subsector && !selection.category) return payload;
  const root = payload as Record<string, unknown>;
  const filterRows = (value: unknown) =>
    Array.isArray(value) ? value.filter((item) => matchesMarketSelection((item || {}) as JourneyRow, selection)) : value;
  return {
    ...root,
    rows: filterRows(root.rows),
    selection_rows: filterRows(root.selection_rows),
  };
}

async function respondWithTaxonomyNormalization(
  response: NextResponse,
  taxonomyView: "market" | "standard",
  selection: MarketSelection
) {
  if (!response.ok) return response;
  const payload = await response.json().catch(() => null);
  const normalized = normalizeJourneyTaxonomyPayload(payload, taxonomyView);
  const filtered = applySelectionFilterToJourneyPayload(normalized, taxonomyView, selection);
  return NextResponse.json(filtered, { status: response.status });
}

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

export async function GET(request: NextRequest) {
  const scopeContext = await getScopeContext(request);
  const taxonomyView = request.nextUrl.searchParams.get("taxonomy_view") === "standard" ? "standard" : "market";
  const selection: MarketSelection = {
    sector: request.nextUrl.searchParams.get("sector"),
    subsector: request.nextUrl.searchParams.get("subsector"),
    category: request.nextUrl.searchParams.get("category"),
  };
  const responseMode = request.nextUrl.searchParams.get("response_mode");
  const skipViewerScoping = responseMode === "benchmark_global";

  if (skipViewerScoping) {
    const preScoped = await applyMarketFilterToStudyIds({
      query: expandNseInQuery(Object.fromEntries(request.nextUrl.searchParams.entries())),
      payload: {},
      allowedStudyIds: null,
    });
    const queryString = new URLSearchParams(preScoped.query).toString();
    const query = queryString ? `?${queryString}` : "";
    const response = await handleWithDataSource(
      request,
      `/analytics/journey/table_multi${query}`,
      "bbs_journey_table_multi",
      {
        query: preScoped.query,
        payload: preScoped.payload,
      },
      { method: "GET" }
    );
    return respondWithTaxonomyNormalization(response, taxonomyView, selection);
  }

  if (scopeContext.allowedStudyIds && scopeContext.allowedStudyIds.length === 0) {
    return NextResponse.json({ rows: [], selection_rows: [], global_rows: [] });
  }
  const marketScoped = await applyMarketFilterToStudyIds({
    query: expandNseInQuery(Object.fromEntries(request.nextUrl.searchParams.entries())),
    payload: {},
    allowedStudyIds: scopeContext.allowedStudyIds,
  });
  const scoped = withStudyScope(
    marketScoped.query,
    marketScoped.payload,
    scopeContext.allowedStudyIds
  );
  const queryString = new URLSearchParams(scoped.query).toString();
  const query = queryString ? `?${queryString}` : "";
  const response = await handleWithDataSource(
    request,
    `/analytics/journey/table_multi${query}`,
    "bbs_journey_table_multi",
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
  const payload = expandNseInPayload((await request.json().catch(() => ({}))) as Record<string, unknown>);
  const taxonomyView =
    typeof payload.taxonomy_view === "string" && payload.taxonomy_view.toLowerCase() === "standard"
      ? "standard"
      : request.nextUrl.searchParams.get("taxonomy_view") === "standard"
        ? "standard"
        : "market";
  const selection: MarketSelection = {
    sector:
      (typeof payload.sector === "string" ? payload.sector : null) ?? request.nextUrl.searchParams.get("sector"),
    subsector:
      (typeof payload.subsector === "string" ? payload.subsector : null) ??
      request.nextUrl.searchParams.get("subsector"),
    category:
      (typeof payload.category === "string" ? payload.category : null) ?? request.nextUrl.searchParams.get("category"),
  };
  const responseMode = request.nextUrl.searchParams.get("response_mode");
  const skipViewerScoping = responseMode === "benchmark_global";

  if (skipViewerScoping) {
    const preScoped = await applyMarketFilterToStudyIds({
      query: expandNseInQuery(Object.fromEntries(request.nextUrl.searchParams.entries())),
      payload,
      allowedStudyIds: null,
    });
    const queryString = new URLSearchParams(preScoped.query).toString();
    const query = queryString ? `?${queryString}` : "";
    const response = await handleWithDataSource(
      request,
      `/analytics/journey/table_multi${query}`,
      "bbs_journey_table_multi",
      {
        query: preScoped.query,
        payload: preScoped.payload,
      },
      { method: "POST" }
    );
    return respondWithTaxonomyNormalization(response, taxonomyView, selection);
  }

  if (scopeContext.allowedStudyIds && scopeContext.allowedStudyIds.length === 0) {
    return NextResponse.json({ rows: [], selection_rows: [], global_rows: [] });
  }
  const marketScoped = await applyMarketFilterToStudyIds({
    query: expandNseInQuery(Object.fromEntries(request.nextUrl.searchParams.entries())),
    payload,
    allowedStudyIds: scopeContext.allowedStudyIds,
  });
  const scoped = withStudyScope(
    marketScoped.query,
    marketScoped.payload,
    scopeContext.allowedStudyIds
  );
  const queryString = new URLSearchParams(scoped.query).toString();
  const query = queryString ? `?${queryString}` : "";
  const response = await handleWithDataSource(
    request,
    `/analytics/journey/table_multi${query}`,
    "bbs_journey_table_multi",
    {
      query: scoped.query,
      payload: scoped.payload,
    },
    { method: "POST" }
  );
  return respondWithTaxonomyNormalization(response, taxonomyView, selection);
}
