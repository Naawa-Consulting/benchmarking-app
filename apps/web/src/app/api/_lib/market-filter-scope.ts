import { getDataSource } from "./backend";
import { resolveMarketLens } from "./market-lens";
import { supabaseAdminPostgrest } from "./supabase-admin";

type StudyCatalogItem = {
  study_id: string;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  market_sector?: string | null;
  market_subsector?: string | null;
  market_category?: string | null;
};

type ScopePatchInput = {
  query: Record<string, string>;
  payload: Record<string, unknown>;
  allowedStudyIds: string[] | null;
  preserveTaxonomyFilters?: boolean;
};

type ScopePatchResult = {
  query: Record<string, string>;
  payload: Record<string, unknown>;
};

type MarketSelection = {
  sector: string | null;
  subsector: string | null;
  category: string | null;
};

let cache: { at: number; items: StudyCatalogItem[] } | null = null;
const CACHE_TTL_MS = 8000;

function parseCsv(csv: string | null | undefined): string[] {
  if (!csv) return [];
  return csv
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean);
}

function parseStudyIdsInput(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value
      .map((v) => (typeof v === "string" ? v.trim() : ""))
      .filter(Boolean);
  }
  if (typeof value === "string") return parseCsv(value);
  return [];
}

function intersectIds(base: string[], constraint: Set<string> | null): string[] {
  if (!constraint) return base;
  return base.filter((id) => constraint.has(id));
}

function topValue(values: Array<string | null | undefined>): string | null {
  const counts = new Map<string, number>();
  for (const value of values) {
    if (typeof value !== "string" || !value.trim()) continue;
    const key = value.trim();
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

async function fetchLegacyStudies(): Promise<StudyCatalogItem[]> {
  const base = (
    process.env.LEGACY_API_BASE_URL ||
    process.env.NEXT_PUBLIC_API_BASE_URL ||
    "http://localhost:8000"
  ).replace(/\/+$/, "");
  const response = await fetch(`${base}/filters/options/studies`, { cache: "no-store" });
  if (!response.ok) return [];
  const payload = (await response.json().catch(() => ({}))) as { items?: Array<Record<string, unknown>> };
  const items = Array.isArray(payload.items) ? payload.items : [];
  return items
    .map((row) => {
      const market = resolveMarketLens({
        sector: typeof row.sector === "string" ? row.sector : null,
        subsector: typeof row.subsector === "string" ? row.subsector : null,
        category: typeof row.category === "string" ? row.category : null,
        market_sector: typeof row.market_sector === "string" ? row.market_sector : null,
        market_subsector: typeof row.market_subsector === "string" ? row.market_subsector : null,
        market_category: typeof row.market_category === "string" ? row.market_category : null,
      });
      return {
        study_id: String(row.study_id || "").trim(),
        sector: typeof row.sector === "string" ? row.sector : null,
        subsector: typeof row.subsector === "string" ? row.subsector : null,
        category: typeof row.category === "string" ? row.category : null,
        market_sector: market.market_sector,
        market_subsector: market.market_subsector,
        market_category: market.market_category,
      } satisfies StudyCatalogItem;
    })
    .filter((row) => row.study_id);
}

async function fetchSupabaseStudies(): Promise<StudyCatalogItem[]> {
  const { response, data } = await supabaseAdminPostgrest(
    "study_catalog?select=study_id,sector,subsector,category,market_sector,market_subsector,market_category"
  );
  if (!response.ok || !Array.isArray(data)) return [];
  return data
    .map((row) => {
      const src = row as Record<string, unknown>;
      const market = resolveMarketLens({
        sector: typeof src.sector === "string" ? src.sector : null,
        subsector: typeof src.subsector === "string" ? src.subsector : null,
        category: typeof src.category === "string" ? src.category : null,
        market_sector: typeof src.market_sector === "string" ? src.market_sector : null,
        market_subsector: typeof src.market_subsector === "string" ? src.market_subsector : null,
        market_category: typeof src.market_category === "string" ? src.market_category : null,
      });
      return {
        study_id: String(src.study_id || "").trim(),
        sector: typeof src.sector === "string" ? src.sector : null,
        subsector: typeof src.subsector === "string" ? src.subsector : null,
        category: typeof src.category === "string" ? src.category : null,
        market_sector: market.market_sector,
        market_subsector: market.market_subsector,
        market_category: market.market_category,
      } satisfies StudyCatalogItem;
    })
    .filter((row) => row.study_id);
}

async function getStudiesCatalog(): Promise<StudyCatalogItem[]> {
  const now = Date.now();
  if (cache && now - cache.at < CACHE_TTL_MS) return cache.items;
  const items = getDataSource() === "supabase" ? await fetchSupabaseStudies() : await fetchLegacyStudies();
  cache = { at: now, items };
  return items;
}

function uniqueNonEmpty(values: Array<string | null | undefined>): string[] {
  const set = new Set<string>();
  for (const value of values) {
    if (typeof value !== "string") continue;
    const key = value.trim();
    if (!key) continue;
    set.add(key);
  }
  return Array.from(set);
}

export async function resolveStandardFallbackForMarketSelection(input: {
  selection: MarketSelection;
  allowedStudyIds: string[] | null;
  requestedStudyIds?: string[] | null;
}): Promise<{ sector: string | null; subsector: string | null; category: string | null }> {
  const studies = await getStudiesCatalog();
  const allowed = input.allowedStudyIds ? new Set(input.allowedStudyIds) : null;
  const requested = input.requestedStudyIds && input.requestedStudyIds.length ? new Set(input.requestedStudyIds) : null;

  let rows = studies
    .filter((row) => (allowed ? allowed.has(row.study_id) : true))
    .filter((row) => (requested ? requested.has(row.study_id) : true))
    .filter((row) => (input.selection.sector ? row.market_sector === input.selection.sector : true))
    .filter((row) => (input.selection.subsector ? row.market_subsector === input.selection.subsector : true))
    .filter((row) => (input.selection.category ? row.market_category === input.selection.category : true));

  if (!rows.length) {
    rows = studies
      .filter((row) => (allowed ? allowed.has(row.study_id) : true))
      .filter((row) => (requested ? requested.has(row.study_id) : true))
      .filter((row) => (input.selection.sector ? row.market_sector === input.selection.sector : true));
  }

  const sectors = uniqueNonEmpty(rows.map((row) => row.sector));
  const subsectors = uniqueNonEmpty(rows.map((row) => row.subsector));
  const categories = uniqueNonEmpty(rows.map((row) => row.category));

  // Avoid collapsing Market Lens selections into a single standard bucket when
  // the selection spans multiple standard sectors/subsectors/categories.
  const sector = sectors.length === 1 ? sectors[0] : null;
  const subsector = subsectors.length === 1 ? subsectors[0] : null;
  const category = categories.length === 1 ? categories[0] : null;

  return { sector, subsector, category };
}

export async function applyMarketFilterToStudyIds(input: ScopePatchInput): Promise<ScopePatchResult> {
  const query = { ...input.query };
  const payload = { ...input.payload };
  const payloadTaxonomyView =
    typeof payload.taxonomy_view === "string" ? payload.taxonomy_view.toLowerCase() : null;
  const taxonomyView = payloadTaxonomyView || (query.taxonomy_view === "standard" ? "standard" : "market");
  if (taxonomyView !== "market") {
    return { query, payload };
  }

  const sector =
    (typeof payload.sector === "string" ? payload.sector : null) ||
    (typeof query.sector === "string" ? query.sector : null);
  const subsector =
    (typeof payload.subsector === "string" ? payload.subsector : null) ||
    (typeof query.subsector === "string" ? query.subsector : null);
  const category =
    (typeof payload.category === "string" ? payload.category : null) ||
    (typeof query.category === "string" ? query.category : null);

  if (!sector && !subsector && !category) {
    return { query, payload };
  }

  const studies = await getStudiesCatalog();
  let ids = studies
    .filter((row) => (sector ? row.market_sector === sector : true))
    .filter((row) => (subsector ? row.market_subsector === subsector : true))
    .filter((row) => (category ? row.market_category === category : true))
    .map((row) => row.study_id);

  const requestedIds = parseStudyIdsInput(payload.study_ids);
  const requestedFromQuery = parseCsv(query.study_ids || query.studies);
  const requested = requestedIds.length ? requestedIds : requestedFromQuery;
  if (requested.length) {
    ids = intersectIds(ids, new Set(requested));
  }
  if (input.allowedStudyIds) {
    ids = intersectIds(ids, new Set(input.allowedStudyIds));
  }

  query.study_ids = ids.join(",");
  query.studies = ids.join(",");
  payload.study_ids = ids;

  if (!input.preserveTaxonomyFilters) {
    delete query.sector;
    delete query.subsector;
    delete query.category;
    payload.sector = null;
    payload.subsector = null;
    payload.category = null;
  }

  return { query, payload };
}
