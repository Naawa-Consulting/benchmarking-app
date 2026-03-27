import { NextRequest, NextResponse } from "next/server";
import { callSupabaseRpc, getDataSource, handleWithDataSource } from "../_lib/backend";
import { getScopeContext, scopeStudyIdsCsv } from "../_lib/access-scope";
import { resolveMarketLens } from "../_lib/market-lens";
import { applyMarketFilterToStudyIds } from "../_lib/market-filter-scope";
import { expandNseInQuery } from "../_lib/demographics";

export const dynamic = "force-dynamic";

type NetworkNodeLike = Record<string, unknown> & {
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  market_sector?: string | null;
  market_subsector?: string | null;
  market_category?: string | null;
  context_sources?: Array<Record<string, unknown>> | null;
};

type TouchpointRowLike = {
  brand?: string | null;
  study_id?: string | null;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  market_sector?: string | null;
  market_subsector?: string | null;
  market_category?: string | null;
};

function topNonEmpty(values: Array<string | null | undefined>): string | null {
  const counts = new Map<string, number>();
  for (const value of values) {
    if (typeof value !== "string") continue;
    const key = value.trim();
    if (!key) continue;
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  let best: string | null = null;
  let bestCount = -1;
  for (const [key, count] of counts.entries()) {
    if (count > bestCount) {
      best = key;
      bestCount = count;
    }
  }
  return best;
}

function remapNodeToMarketLens(node: NetworkNodeLike): NetworkNodeLike {
  const resolvedMarket = resolveMarketLens({
    sector: typeof node.sector === "string" ? node.sector : null,
    subsector: typeof node.subsector === "string" ? node.subsector : null,
    category: typeof node.category === "string" ? node.category : null,
    market_sector: typeof node.market_sector === "string" ? node.market_sector : null,
    market_subsector: typeof node.market_subsector === "string" ? node.market_subsector : null,
    market_category: typeof node.market_category === "string" ? node.market_category : null,
  });
  const market = {
    market_sector:
      typeof node.market_sector === "string" && node.market_sector.trim()
        ? node.market_sector
        : resolvedMarket.market_sector,
    market_subsector:
      typeof node.market_subsector === "string" && node.market_subsector.trim()
        ? node.market_subsector
        : resolvedMarket.market_subsector,
    market_category:
      typeof node.market_category === "string" && node.market_category.trim()
        ? node.market_category
        : resolvedMarket.market_category,
  };

  const contextSources = Array.isArray(node.context_sources)
    ? node.context_sources.map((source) => {
        const resolvedSourceMarket = resolveMarketLens({
          sector: typeof source.sector === "string" ? source.sector : null,
          subsector: typeof source.subsector === "string" ? source.subsector : null,
          category: typeof source.category === "string" ? source.category : null,
          market_sector: typeof source.market_sector === "string" ? source.market_sector : null,
          market_subsector: typeof source.market_subsector === "string" ? source.market_subsector : null,
          market_category: typeof source.market_category === "string" ? source.market_category : null,
        });
        const sourceMarket = {
          market_sector:
            typeof source.market_sector === "string" && source.market_sector.trim()
              ? source.market_sector
              : resolvedSourceMarket.market_sector,
          market_subsector:
            typeof source.market_subsector === "string" && source.market_subsector.trim()
              ? source.market_subsector
              : resolvedSourceMarket.market_subsector,
          market_category:
            typeof source.market_category === "string" && source.market_category.trim()
              ? source.market_category
              : resolvedSourceMarket.market_category,
        };
        return {
          ...source,
          market_sector: sourceMarket.market_sector,
          market_subsector: sourceMarket.market_subsector,
          market_category: sourceMarket.market_category,
          sector: sourceMarket.market_sector,
          subsector: sourceMarket.market_subsector,
          category: sourceMarket.market_category,
        };
      })
    : node.context_sources;

  const inferredMarketSector = topNonEmpty(
    Array.isArray(contextSources)
      ? contextSources.map((source) =>
          typeof source.market_sector === "string" ? source.market_sector : null
        )
      : []
  );
  const inferredMarketSubsector = topNonEmpty(
    Array.isArray(contextSources)
      ? contextSources.map((source) =>
          typeof source.market_subsector === "string" ? source.market_subsector : null
        )
      : []
  );
  const inferredMarketCategory = topNonEmpty(
    Array.isArray(contextSources)
      ? contextSources.map((source) =>
          typeof source.market_category === "string" ? source.market_category : null
        )
      : []
  );
  const effectiveMarket = {
    market_sector: market.market_sector || inferredMarketSector || "Unassigned",
    market_subsector: market.market_subsector || inferredMarketSubsector || "Unassigned",
    market_category: market.market_category || inferredMarketCategory || "Unassigned",
  };

  return {
    ...node,
    market_sector: effectiveMarket.market_sector,
    market_subsector: effectiveMarket.market_subsector,
    market_category: effectiveMarket.market_category,
    // Keep legacy fields aligned with current consumers.
    sector: effectiveMarket.market_sector,
    subsector: effectiveMarket.market_subsector,
    category: effectiveMarket.market_category,
    context_sources: contextSources,
  };
}

async function fetchBrandContextSourcesFromTouchpoints(
  queryObj: Record<string, string>
): Promise<Map<string, Array<Record<string, unknown>>>> {
  const byBrand = new Map<string, Array<Record<string, unknown>>>();
  if (getDataSource() !== "supabase") return byBrand;
  const payload = {
    query: {
      ...queryObj,
      limit_mode: "all",
      sort_by: "recall",
      sort_dir: "desc",
    },
    payload: {},
  };
  try {
    const { response, data } = await callSupabaseRpc("bbs_touchpoints_table_multi", payload);
    if (!response.ok) return byBrand;
    const root = (data || {}) as { rows?: TouchpointRowLike[] };
    const rows = Array.isArray(root.rows) ? root.rows : [];
    const dedupe = new Set<string>();
    for (const row of rows) {
      const brand = typeof row.brand === "string" ? row.brand.trim() : "";
      if (!brand) continue;
      const source = {
        study_id: typeof row.study_id === "string" ? row.study_id : null,
        sector:
          typeof row.market_sector === "string" && row.market_sector.trim()
            ? row.market_sector
            : typeof row.sector === "string"
              ? row.sector
              : null,
        subsector:
          typeof row.market_subsector === "string" && row.market_subsector.trim()
            ? row.market_subsector
            : typeof row.subsector === "string"
              ? row.subsector
              : null,
        category:
          typeof row.market_category === "string" && row.market_category.trim()
            ? row.market_category
            : typeof row.category === "string"
              ? row.category
              : null,
      };
      const key = `${brand}|${source.study_id || ""}|${source.sector || ""}|${source.subsector || ""}|${source.category || ""}`;
      if (dedupe.has(key)) continue;
      dedupe.add(key);
      const bucket = byBrand.get(brand) || [];
      bucket.push(source);
      byBrand.set(brand, bucket);
    }
  } catch {
    return byBrand;
  }
  return byBrand;
}

function matchesSelection(
  node: NetworkNodeLike,
  selection: { sector: string | null; subsector: string | null; category: string | null }
) {
  if (node.type !== "brand") return false;
  const sector = typeof node.sector === "string" ? node.sector : null;
  const subsector = typeof node.subsector === "string" ? node.subsector : null;
  const category = typeof node.category === "string" ? node.category : null;
  if (selection.sector && sector !== selection.sector) return false;
  if (selection.subsector && subsector !== selection.subsector) return false;
  if (selection.category && category !== selection.category) return false;
  return true;
}

export async function GET(request: NextRequest) {
  const scopeContext = await getScopeContext(request);
  if (scopeContext.allowedStudyIds && scopeContext.allowedStudyIds.length === 0) {
    return NextResponse.json({
      ok: true,
      metric: "recall",
      filters: {},
      nodes: [],
      links: [],
      meta: { source: "supabase", warning: "No studies allowed for current user scope." },
    });
  }

  const initialQuery = expandNseInQuery(Object.fromEntries(request.nextUrl.searchParams.entries()));
  const marketScoped = await applyMarketFilterToStudyIds({
    query: initialQuery,
    payload: {},
    allowedStudyIds: scopeContext.allowedStudyIds,
  });
  const queryObj = marketScoped.query;
  if (scopeContext.allowedStudyIds !== null) {
    const scopedCsv = scopeStudyIdsCsv(queryObj.studies || queryObj.study_ids, scopeContext.allowedStudyIds);
    queryObj.studies = scopedCsv || "";
    queryObj.study_ids = scopedCsv || "";
  }
  if (!scopeContext.authz.can_toggle_brands) {
    queryObj.brands = "";
    queryObj.brands_mode = "disable";
    queryObj.network_brands = "disable";
  }
  const queryString = new URLSearchParams(queryObj).toString();
  const query = queryString ? `?${queryString}` : "";
  const response = await handleWithDataSource(
    request,
    `/network${query}`,
    "bbs_network",
    {
      query: queryObj,
      payload: {},
    },
    { method: "GET" }
  );

  const taxonomyView = queryObj.taxonomy_view === "standard" ? "standard" : "market";
  if (!response.ok || taxonomyView !== "market") {
    return response;
  }
  const selection = {
    sector: typeof initialQuery.sector === "string" ? initialQuery.sector : null,
    subsector: typeof initialQuery.subsector === "string" ? initialQuery.subsector : null,
    category: typeof initialQuery.category === "string" ? initialQuery.category : null,
  };

  const payload = await response.json().catch(() => null);
  if (!payload || typeof payload !== "object") {
    return response;
  }

  const root = payload as Record<string, unknown>;
  const brandContextSources = await fetchBrandContextSourcesFromTouchpoints(queryObj);
  const remappedNodes = Array.isArray(root.nodes)
    ? root.nodes.map((node) => {
        const current = (node || {}) as NetworkNodeLike;
        const brandName = typeof current.label === "string" ? current.label.trim() : "";
        const contextSources = brandName ? brandContextSources.get(brandName) : undefined;
        const enriched: NetworkNodeLike =
          contextSources && current.type === "brand"
            ? { ...current, context_sources: contextSources }
            : current;
        return remapNodeToMarketLens(enriched);
      })
    : root.nodes;
  if (!Array.isArray(remappedNodes) || (!selection.sector && !selection.subsector && !selection.category)) {
    return NextResponse.json({ ...root, nodes: remappedNodes }, { status: response.status });
  }

  const allowedNodeIds = new Set<string>();
  for (const node of remappedNodes) {
    const n = node as NetworkNodeLike;
    if (matchesSelection(n, selection) && typeof n.id === "string") {
      allowedNodeIds.add(n.id);
    }
  }
  const links = Array.isArray(root.links) ? (root.links as Array<Record<string, unknown>>) : [];
  const connectedTouchpoints = new Set<string>();
  for (const link of links) {
    const source = typeof link.source === "string" ? link.source : null;
    const target = typeof link.target === "string" ? link.target : null;
    if (!source || !target) continue;
    if (allowedNodeIds.has(source) || allowedNodeIds.has(target)) {
      connectedTouchpoints.add(source);
      connectedTouchpoints.add(target);
    }
  }
  const filteredNodes = remappedNodes.filter((node) => {
    const rawId = (node as NetworkNodeLike).id;
    const id = typeof rawId === "string" ? rawId : "";
    return allowedNodeIds.has(id) || connectedTouchpoints.has(id);
  });
  const filteredNodeIds = new Set(filteredNodes.map((node) => String((node as NetworkNodeLike).id || "")));
  const filteredLinks = links.filter((link) => {
    const source = typeof link.source === "string" ? link.source : "";
    const target = typeof link.target === "string" ? link.target : "";
    return filteredNodeIds.has(source) && filteredNodeIds.has(target);
  });

  return NextResponse.json({ ...root, nodes: filteredNodes, links: filteredLinks }, { status: response.status });
}
