import { NextRequest, NextResponse } from "next/server";
import { handleWithDataSource } from "../_lib/backend";
import { getScopeContext, scopeStudyIdsCsv } from "../_lib/access-scope";
import { resolveMarketLens } from "../_lib/market-lens";
import { applyMarketFilterToStudyIds } from "../_lib/market-filter-scope";

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

function remapNodeToMarketLens(node: NetworkNodeLike): NetworkNodeLike {
  const market = resolveMarketLens({
    sector: typeof node.sector === "string" ? node.sector : null,
    subsector: typeof node.subsector === "string" ? node.subsector : null,
    category: typeof node.category === "string" ? node.category : null,
    market_sector: typeof node.market_sector === "string" ? node.market_sector : null,
    market_subsector: typeof node.market_subsector === "string" ? node.market_subsector : null,
    market_category: typeof node.market_category === "string" ? node.market_category : null,
  });

  const contextSources = Array.isArray(node.context_sources)
    ? node.context_sources.map((source) => {
        const sourceMarket = resolveMarketLens({
          sector: typeof source.sector === "string" ? source.sector : null,
          subsector: typeof source.subsector === "string" ? source.subsector : null,
          category: typeof source.category === "string" ? source.category : null,
        });
        return {
          ...source,
          sector: sourceMarket.market_sector,
          subsector: sourceMarket.market_subsector,
          category: sourceMarket.market_category,
        };
      })
    : node.context_sources;

  return {
    ...node,
    market_sector: market.market_sector,
    market_subsector: market.market_subsector,
    market_category: market.market_category,
    // Keep legacy fields aligned with current consumers.
    sector: market.market_sector,
    subsector: market.market_subsector,
    category: market.market_category,
    context_sources: contextSources,
  };
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

  const initialQuery = Object.fromEntries(request.nextUrl.searchParams.entries());
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
  const remappedNodes = Array.isArray(root.nodes)
    ? root.nodes.map((node) => remapNodeToMarketLens((node || {}) as NetworkNodeLike))
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
