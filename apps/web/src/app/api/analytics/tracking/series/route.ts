import { NextRequest, NextResponse } from "next/server";
import { getDataSource, handleWithDataSource } from "../../../_lib/backend";
import { getScopeContext, parseStudyIdsInput, scopeStudyIds, scopeStudyIdsCsv } from "../../../_lib/access-scope";
import { resolveMarketLens } from "../../../_lib/market-lens";
import { applyMarketFilterToStudyIds } from "../../../_lib/market-filter-scope";
import { expandNseInPayload, expandNseInQuery } from "../../../_lib/demographics";

export const dynamic = "force-dynamic";

function getLegacyBaseUrlForTracking() {
  const base = (
    process.env.LEGACY_API_BASE_URL ||
    process.env.NEXT_PUBLIC_API_BASE_URL ||
    "http://127.0.0.1:8000"
  ).trim();
  return base ? base.replace(/\/+$/, "") : null;
}

async function tryLegacyTrackingSeries(
  method: "GET" | "POST",
  query: Record<string, string>,
  payload: Record<string, unknown>
) {
  const base = getLegacyBaseUrlForTracking();
  if (!base) return null;
  try {
    if (method === "GET") {
      const qs = new URLSearchParams(query).toString();
      const resp = await fetch(`${base}/analytics/tracking/series${qs ? `?${qs}` : ""}`, {
        method: "GET",
        cache: "no-store",
      });
      if (!resp.ok) return null;
      return await resp.json().catch(() => null);
    }
    const resp = await fetch(`${base}/analytics/tracking/series`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      cache: "no-store",
    });
    if (!resp.ok) return null;
    return await resp.json().catch(() => null);
  } catch {
    return null;
  }
}

type MetricSeries = {
  deltas?: Record<string, number | null>;
  values?: Record<string, number | null>;
};

type TrackingRow = {
  entity: string;
  metrics?: Record<string, MetricSeries>;
};

type TrackingPayload = {
  ok?: boolean;
  meta?: Record<string, unknown>;
  entity_rows?: TrackingRow[];
  secondary_rows?: unknown[];
  delta_columns?: unknown[];
  brand_rows?: unknown[];
  touchpoint_rows?: unknown[];
  metric_meta_brand?: Record<string, unknown>;
  metric_meta_touchpoint?: Record<string, unknown>;
  resolved_breakdown?: string;
  entity_label?: string;
  periods?: Array<{ key?: string; label?: string }>;
  resolved_granularity?: string;
};

function parseYearsInput(value: unknown): string[] {
  if (Array.isArray(value)) {
    return value
      .map((item) => (typeof item === "string" ? item.trim() : ""))
      .filter((item) => /^\d{4}$/.test(item));
  }
  if (typeof value === "string") {
    return value
      .split(",")
      .map((item) => item.trim())
      .filter((item) => /^\d{4}$/.test(item));
  }
  return [];
}

function isQuarterLabel(value: string | null | undefined): boolean {
  if (!value) return false;
  return /^\d{4}-Q[1-4]$/i.test(value.trim());
}

function shouldPreferQuarterView(
  series: TrackingPayload | null,
  query: Record<string, string>,
  payload: Record<string, unknown>
): boolean {
  const selectedYears = parseYearsInput(payload.years).length
    ? parseYearsInput(payload.years)
    : parseYearsInput(query.years);
  if (selectedYears.length === 1) return true;

  const hasTaxonomySelection =
    (typeof payload.sector === "string" && payload.sector.trim().length > 0) ||
    (typeof payload.subsector === "string" && payload.subsector.trim().length > 0) ||
    (typeof payload.category === "string" && payload.category.trim().length > 0) ||
    (typeof query.sector === "string" && query.sector.trim().length > 0) ||
    (typeof query.subsector === "string" && query.subsector.trim().length > 0) ||
    (typeof query.category === "string" && query.category.trim().length > 0);

  if (!hasTaxonomySelection || !series) return false;
  const periods = Array.isArray(series.periods) ? series.periods : [];
  const labels = periods
    .map((period) => (typeof period?.label === "string" ? period.label.trim() : ""))
    .filter(Boolean);
  if (labels.length === 0) return false;
  const hasQuarter = labels.some((label) => isQuarterLabel(label));
  if (hasQuarter) return false;
  const uniqueYears = new Set(
    labels
      .map((label) => {
        const m = label.match(/(19|20)\d{2}/);
        return m ? m[0] : null;
      })
      .filter((year): year is string => Boolean(year))
  );
  return uniqueYears.size <= 1;
}

function toPeriodKeys(payload: TrackingPayload): string[] {
  const keys = Array.isArray(payload.periods)
    ? payload.periods
        .map((period) => (typeof period?.key === "string" ? period.key : null))
        .filter((key): key is string => Boolean(key))
    : [];
  return keys;
}

function mapEntityToMarket(
  entity: string,
  breakdown: string | undefined
): { marketEntity: string; label: string } {
  const safeEntity = (entity || "").trim();
  if (!safeEntity) return { marketEntity: safeEntity, label: "Sector" };
  if (breakdown === "subsector") {
    const market = resolveMarketLens({ subsector: safeEntity });
    return { marketEntity: market.market_subsector, label: "Segmento" };
  }
  if (breakdown === "category") {
    const market = resolveMarketLens({ category: safeEntity });
    return { marketEntity: market.market_category, label: "Categoría comercial" };
  }
  const market = resolveMarketLens({ sector: safeEntity });
  return { marketEntity: market.market_sector, label: "Macrosector" };
}

function isAlreadyMarketSeries(series: TrackingPayload): boolean {
  const label = typeof series.entity_label === "string" ? series.entity_label.trim().toLowerCase() : "";
  return (
    label === "macrosector" ||
    label === "segmento" ||
    label === "categoría comercial" ||
    label === "categoria comercial"
  );
}

function aggregateTrackingRowsToMarket(payload: TrackingPayload): TrackingPayload {
  const rows = Array.isArray(payload.entity_rows) ? payload.entity_rows : [];
  if (!rows.length) return payload;
  const breakdown = typeof payload.resolved_breakdown === "string" ? payload.resolved_breakdown : "sector";
  if (!["sector", "subsector", "category"].includes(breakdown)) return payload;

  const byEntity = new Map<
    string,
    {
      entity: string;
      metrics: Record<string, { valueSums: Record<string, number>; valueCounts: Record<string, number> }>;
    }
  >();

  for (const row of rows) {
    const mapped = mapEntityToMarket(typeof row.entity === "string" ? row.entity : "", breakdown);
    if (!mapped.marketEntity) continue;
    if (!byEntity.has(mapped.marketEntity)) {
      byEntity.set(mapped.marketEntity, { entity: mapped.marketEntity, metrics: {} });
    }
    const acc = byEntity.get(mapped.marketEntity)!;
    const metrics = row.metrics || {};
    for (const [metricKey, metricSeries] of Object.entries(metrics)) {
      if (!acc.metrics[metricKey]) {
        acc.metrics[metricKey] = { valueSums: {}, valueCounts: {} };
      }
      const values = metricSeries?.values || {};
      for (const [periodKey, raw] of Object.entries(values)) {
        if (typeof raw !== "number" || !Number.isFinite(raw)) continue;
        acc.metrics[metricKey].valueSums[periodKey] = (acc.metrics[metricKey].valueSums[periodKey] || 0) + raw;
        acc.metrics[metricKey].valueCounts[periodKey] = (acc.metrics[metricKey].valueCounts[periodKey] || 0) + 1;
      }
    }
  }

  const periodKeys = toPeriodKeys(payload);
  const aggregatedRows: TrackingRow[] = Array.from(byEntity.values())
    .map((entry) => {
      const metrics: Record<string, MetricSeries> = {};
      for (const [metricKey, stat] of Object.entries(entry.metrics)) {
        const values: Record<string, number | null> = {};
        for (const [periodKey, sum] of Object.entries(stat.valueSums)) {
          const count = stat.valueCounts[periodKey] || 0;
          values[periodKey] = count > 0 ? sum / count : null;
        }
        const deltas: Record<string, number | null> = {};
        for (let i = 1; i < periodKeys.length; i += 1) {
          const from = periodKeys[i - 1];
          const to = periodKeys[i];
          const fromValue = values[from];
          const toValue = values[to];
          deltas[`d_${from}_${to}`] =
            typeof fromValue === "number" && typeof toValue === "number" ? toValue - fromValue : null;
        }
        metrics[metricKey] = { values, deltas };
      }
      return { entity: entry.entity, metrics };
    })
    .sort((a, b) => a.entity.localeCompare(b.entity));

  const mappedLabel = mapEntityToMarket("x", breakdown).label;
  return {
    ...payload,
    entity_rows: aggregatedRows,
    entity_label: mappedLabel,
  };
}

function normalizeTrackingPayloadTaxonomy(
  payload: unknown,
  taxonomyView: "market" | "standard"
): unknown {
  if (taxonomyView !== "market" || !payload) return payload;

  if (Array.isArray(payload)) {
    return payload.map((item) => {
      if (!item || typeof item !== "object") return item;
      const record = item as Record<string, unknown>;
      if (record.bbs_tracking_series && typeof record.bbs_tracking_series === "object") {
        const series = record.bbs_tracking_series as TrackingPayload;
        if (isAlreadyMarketSeries(series)) return item;
        return {
          ...record,
          bbs_tracking_series: aggregateTrackingRowsToMarket(series),
        };
      }
      const series = record as TrackingPayload;
      if (isAlreadyMarketSeries(series)) return item;
      return aggregateTrackingRowsToMarket(series);
    });
  }

  if (typeof payload !== "object") return payload;
  const root = payload as Record<string, unknown>;
  if (root.bbs_tracking_series && typeof root.bbs_tracking_series === "object") {
    const series = root.bbs_tracking_series as TrackingPayload;
    if (isAlreadyMarketSeries(series)) return payload;
    return {
      ...root,
      bbs_tracking_series: aggregateTrackingRowsToMarket(series),
    };
  }
  const series = root as TrackingPayload;
  if (isAlreadyMarketSeries(series)) return payload;
  return aggregateTrackingRowsToMarket(series);
}

function filterTrackingBySelection(
  payload: unknown,
  taxonomyView: "market" | "standard",
  selection: { sector: string | null; subsector: string | null; category: string | null }
): unknown {
  if (taxonomyView !== "market" || !payload || typeof payload !== "object") return payload;
  if (!selection.sector && !selection.subsector && !selection.category) return payload;

  const applyOnSeries = (series: TrackingPayload): TrackingPayload => {
    const rows = Array.isArray(series.entity_rows) ? series.entity_rows : [];
    const breakdown = typeof series.resolved_breakdown === "string" ? series.resolved_breakdown : "sector";
    let filtered = rows;
    if (breakdown === "sector" && selection.sector) {
      filtered = rows.filter((row) => row.entity === selection.sector);
    } else if (breakdown === "subsector" && selection.subsector) {
      filtered = rows.filter((row) => row.entity === selection.subsector);
    } else if (breakdown === "category" && selection.category) {
      filtered = rows.filter((row) => row.entity === selection.category);
    }
    return { ...series, entity_rows: filtered };
  };

  if (Array.isArray(payload)) {
    return payload.map((item) => {
      if (!item || typeof item !== "object") return item;
      const record = item as Record<string, unknown>;
      if (record.bbs_tracking_series && typeof record.bbs_tracking_series === "object") {
        return {
          ...record,
          bbs_tracking_series: applyOnSeries(record.bbs_tracking_series as TrackingPayload),
        };
      }
      return applyOnSeries(record as TrackingPayload);
    });
  }

  const root = payload as Record<string, unknown>;
  if (root.bbs_tracking_series && typeof root.bbs_tracking_series === "object") {
    return {
      ...root,
      bbs_tracking_series: applyOnSeries(root.bbs_tracking_series as TrackingPayload),
    };
  }
  return applyOnSeries(root as TrackingPayload);
}

function emptyTrackingSeries() {
  return {
    ok: true,
    periods: [],
    entity_rows: [],
    secondary_rows: [],
    delta_columns: [],
    brand_rows: [],
    touchpoint_rows: [],
    meta: { source: "supabase", warning: "No studies allowed for current user scope." },
  };
}

function toSeriesObject(payload: unknown): TrackingPayload | null {
  if (!payload || typeof payload !== "object") return null;
  if (Array.isArray(payload)) {
    const first = payload[0];
    if (first && typeof first === "object" && "bbs_tracking_series" in (first as Record<string, unknown>)) {
      const series = (first as Record<string, unknown>).bbs_tracking_series;
      return series && typeof series === "object" ? (series as TrackingPayload) : null;
    }
    return (first as TrackingPayload) || null;
  }
  const root = payload as Record<string, unknown>;
  if (root.bbs_tracking_series && typeof root.bbs_tracking_series === "object") {
    return root.bbs_tracking_series as TrackingPayload;
  }
  return payload as TrackingPayload;
}

function hasMeaningfulEntities(payload: unknown): boolean {
  const series = toSeriesObject(payload);
  const rows = Array.isArray(series?.entity_rows) ? series.entity_rows : [];
  if (!rows.length) return false;
  return rows.some((row) => {
    const entity = typeof row.entity === "string" ? row.entity.trim().toLowerCase() : "";
    return entity.length > 0 && entity !== "unassigned";
  });
}

function countEntities(payload: unknown): number {
  const series = toSeriesObject(payload);
  const rows = Array.isArray(series?.entity_rows) ? series.entity_rows : [];
  return rows.length;
}

async function maybeQuarterFallbackFromLegacy(
  request: NextRequest,
  method: "GET" | "POST",
  query: Record<string, string>,
  payload: Record<string, unknown>,
  currentPayload: unknown
): Promise<unknown> {
  if (getDataSource() !== "supabase") return currentPayload;
  const currentSeries = toSeriesObject(currentPayload);
  if (!shouldPreferQuarterView(currentSeries, query, payload)) return currentPayload;
  const constrained = await constrainLegacyFallbackToSupabaseStudies(request, query, payload);
  const legacyPayload = await tryLegacyTrackingSeries(method, constrained.query, constrained.payload);
  if (!legacyPayload) return currentPayload;
  const legacySeries = toSeriesObject(legacyPayload);
  if (!legacySeries) return currentPayload;
  const legacyPeriods = Array.isArray(legacySeries.periods) ? legacySeries.periods : [];
  const hasQuarter = legacyPeriods.some((period) =>
    isQuarterLabel(typeof period?.label === "string" ? period.label : null)
  );
  return hasQuarter ? legacyPayload : currentPayload;
}

type StudyCatalogItem = {
  study_id: string;
  sector: string | null;
  subsector: string | null;
  category: string | null;
  market_sector: string | null;
  market_subsector: string | null;
  market_category: string | null;
};

async function fetchStudiesForTracking(request: NextRequest): Promise<StudyCatalogItem[]> {
  const response = await handleWithDataSource(
    request,
    "/filters/options/studies",
    "bbs_filters_options_studies",
    { query: {}, payload: {} },
    { method: "GET" }
  );
  if (!response.ok) return [];
  const json = (await response.json().catch(() => null)) as { items?: Array<Record<string, unknown>> } | null;
  const items = Array.isArray(json?.items) ? json.items : [];
  return items
    .map((row) => ({
      study_id: String(row.study_id || "").trim(),
      sector: typeof row.sector === "string" && row.sector.trim() ? row.sector.trim() : null,
      subsector: typeof row.subsector === "string" && row.subsector.trim() ? row.subsector.trim() : null,
      category: typeof row.category === "string" && row.category.trim() ? row.category.trim() : null,
      market_sector:
        typeof row.market_sector === "string" && row.market_sector.trim() ? row.market_sector.trim() : null,
      market_subsector:
        typeof row.market_subsector === "string" && row.market_subsector.trim()
          ? row.market_subsector.trim()
          : null,
      market_category:
        typeof row.market_category === "string" && row.market_category.trim()
          ? row.market_category.trim()
          : null,
    }))
    .filter((row) => row.study_id);
}

function parseStudyIdsFromInputs(query: Record<string, string>, payload: Record<string, unknown>): string[] {
  const fromPayload = parseStudyIdsInput(payload.study_ids) || [];
  if (fromPayload.length) return fromPayload;
  return parseStudyIdsInput(query.study_ids || query.studies) || [];
}

function normalizeTaxonomyViewFromInputs(query: Record<string, string>, payload: Record<string, unknown>) {
  const payloadView = typeof payload.taxonomy_view === "string" ? payload.taxonomy_view.toLowerCase() : null;
  if (payloadView === "standard") return "standard" as const;
  if (query.taxonomy_view === "standard") return "standard" as const;
  return "market" as const;
}

async function constrainLegacyFallbackToSupabaseStudies(
  request: NextRequest,
  query: Record<string, string>,
  payload: Record<string, unknown>
): Promise<{ query: Record<string, string>; payload: Record<string, unknown> }> {
  if (getDataSource() !== "supabase") return { query, payload };

  const studies = await fetchStudiesForTracking(request);
  if (!studies.length) return { query, payload };

  const requestedIds = parseStudyIdsFromInputs(query, payload);
  const taxonomyView = normalizeTaxonomyViewFromInputs(query, payload);
  const sector =
    (typeof payload.sector === "string" ? payload.sector : null) ||
    (typeof query.sector === "string" ? query.sector : null);
  const subsector =
    (typeof payload.subsector === "string" ? payload.subsector : null) ||
    (typeof query.subsector === "string" ? query.subsector : null);
  const category =
    (typeof payload.category === "string" ? payload.category : null) ||
    (typeof query.category === "string" ? query.category : null);

  const ids = studies
    .filter((row) => (requestedIds.length ? requestedIds.includes(row.study_id) : true))
    .filter((row) => {
      if (taxonomyView === "standard") {
        return (
          (sector ? row.sector === sector : true) &&
          (subsector ? row.subsector === subsector : true) &&
          (category ? row.category === category : true)
        );
      }
      return (
        (sector ? row.market_sector === sector : true) &&
        (subsector ? row.market_subsector === subsector : true) &&
        (category ? row.market_category === category : true)
      );
    })
    .map((row) => row.study_id);

  const finalIds = Array.from(new Set(ids.length ? ids : studies.map((row) => row.study_id)));
  return {
    query: {
      ...query,
      study_ids: finalIds.join(","),
      studies: finalIds.join(","),
    },
    payload: {
      ...payload,
      study_ids: finalIds,
    },
  };
}

function mergeTrackingRows(
  rows: TrackingRow[],
  periods: string[],
  mapEntity: (row: TrackingRow) => string | null
): TrackingRow[] {
  const byEntity = new Map<
    string,
    {
      entity: string;
      metrics: Record<string, { valueSums: Record<string, number>; valueCounts: Record<string, number> }>;
    }
  >();

  for (const row of rows) {
    const target = mapEntity(row);
    if (!target) continue;
    if (!byEntity.has(target)) {
      byEntity.set(target, { entity: target, metrics: {} });
    }
    const acc = byEntity.get(target)!;
    for (const [metricKey, metricSeries] of Object.entries(row.metrics || {})) {
      if (!acc.metrics[metricKey]) {
        acc.metrics[metricKey] = { valueSums: {}, valueCounts: {} };
      }
      for (const [periodKey, raw] of Object.entries(metricSeries?.values || {})) {
        if (typeof raw !== "number" || !Number.isFinite(raw)) continue;
        acc.metrics[metricKey].valueSums[periodKey] = (acc.metrics[metricKey].valueSums[periodKey] || 0) + raw;
        acc.metrics[metricKey].valueCounts[periodKey] = (acc.metrics[metricKey].valueCounts[periodKey] || 0) + 1;
      }
    }
  }

  return Array.from(byEntity.values())
    .map((entry) => {
      const metrics: Record<string, MetricSeries> = {};
      for (const [metricKey, stat] of Object.entries(entry.metrics)) {
        const values: Record<string, number | null> = {};
        for (const [periodKey, sum] of Object.entries(stat.valueSums)) {
          const count = stat.valueCounts[periodKey] || 0;
          values[periodKey] = count > 0 ? sum / count : null;
        }
        const deltas: Record<string, number | null> = {};
        for (let i = 1; i < periods.length; i += 1) {
          const from = periods[i - 1];
          const to = periods[i];
          const fromValue = values[from];
          const toValue = values[to];
          deltas[`d_${from}_${to}`] =
            typeof fromValue === "number" && typeof toValue === "number" ? toValue - fromValue : null;
        }
        metrics[metricKey] = { values, deltas };
      }
      return { entity: entry.entity, metrics };
    })
    .sort((a, b) => a.entity.localeCompare(b.entity));
}

async function rebuildSupabaseMarketSeries(
  request: NextRequest,
  payload: Record<string, unknown>,
  selection: { sector: string | null; subsector: string | null; category: string | null }
): Promise<TrackingPayload | null> {
  if (getDataSource() !== "supabase") return null;
  const requestedStudyIds = parseStudyIdsInput(payload.study_ids) ?? [];
  const studies = await fetchStudiesForTracking(request);
  const filteredStudies = studies
    .filter((row) => (requestedStudyIds.length ? requestedStudyIds.includes(row.study_id) : true))
    .filter((row) => (selection.sector ? row.market_sector === selection.sector : true))
    .filter((row) => (selection.subsector ? row.market_subsector === selection.subsector : true))
    .filter((row) => (selection.category ? row.market_category === selection.category : true));
  if (!filteredStudies.length) return null;

  const target =
    selection.subsector || selection.category
      ? {
          breakdown: "category" as const,
          label: "Categoría comercial",
          key: (row: StudyCatalogItem) => row.market_category || "Unassigned",
        }
      : selection.sector
        ? {
            breakdown: "subsector" as const,
            label: "Segmento",
            key: (row: StudyCatalogItem) => row.market_subsector || "Unassigned",
          }
        : {
            breakdown: "sector" as const,
            label: "Macrosector",
            key: (row: StudyCatalogItem) => row.market_sector || "Unassigned",
          };

  const groupedStudyIds = new Map<string, string[]>();
  for (const row of filteredStudies) {
    const key = target.key(row);
    if (!groupedStudyIds.has(key)) groupedStudyIds.set(key, []);
    groupedStudyIds.get(key)!.push(row.study_id);
  }

  type TaggedTrackingRow = TrackingRow & { __target_entity?: string | null };
  let template: TrackingPayload | null = null;
  const rows: TaggedTrackingRow[] = [];

  for (const [entity, studyIds] of groupedStudyIds.entries()) {
    const standardPayload: Record<string, unknown> = {
      ...payload,
      taxonomy_view: "standard",
      sector: null,
      subsector: null,
      category: null,
      study_ids: Array.from(new Set(studyIds)),
    };
    const resp = await handleWithDataSource(
      request,
      "/analytics/tracking/series",
      "bbs_tracking_series",
      { query: {}, payload: standardPayload },
      { method: "POST" }
    );
    if (!resp.ok) continue;
    const json = await resp.json().catch(() => null);
    const series = toSeriesObject(json);
    if (!series || !Array.isArray(series.entity_rows)) continue;
    if (!template) template = series;
    for (const row of series.entity_rows) {
      rows.push({
        entity: row.entity,
        metrics: row.metrics,
        __target_entity: entity,
      });
    }
  }

  if (!template || !rows.length) return null;
  const periods = toPeriodKeys(template);
  const entityRows = mergeTrackingRows(rows, periods, (row) => {
    const tagged = row as TaggedTrackingRow;
    return tagged.__target_entity || null;
  });
  return {
    ...template,
    resolved_breakdown: target.breakdown,
    entity_label: target.label,
    entity_rows: entityRows,
  };
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

async function applySupabaseMarketFallback(
  query: Record<string, string>,
  payload: Record<string, unknown>,
  selection: { sector: string | null; subsector: string | null; category: string | null },
  allowedStudyIds: string[] | null
) {
  // Keep Market Lens end-to-end. Standard fallback can collapse mixed mappings
  // (e.g. multiple market categories sharing one standard category).
  return { query, payload };
}

export async function GET(request: NextRequest) {
  const scopeContext = await getScopeContext(request);
  const taxonomyView = request.nextUrl.searchParams.get("taxonomy_view") === "standard" ? "standard" : "market";
  const selection = {
    sector: request.nextUrl.searchParams.get("sector"),
    subsector: request.nextUrl.searchParams.get("subsector"),
    category: request.nextUrl.searchParams.get("category"),
  };
  if (scopeContext.allowedStudyIds && scopeContext.allowedStudyIds.length === 0) {
    return NextResponse.json(emptyTrackingSeries());
  }
  const marketScoped = await applyMarketFilterToStudyIds({
    query: expandNseInQuery(Object.fromEntries(request.nextUrl.searchParams.entries())),
    payload: {},
    allowedStudyIds: scopeContext.allowedStudyIds,
    preserveTaxonomyFilters: true,
  });
  const scoped = withStudyScope(
    marketScoped.query,
    marketScoped.payload,
    scopeContext.allowedStudyIds
  );
  const supabaseFallback =
    taxonomyView === "market"
      ? await applySupabaseMarketFallback(
          scoped.query,
          scoped.payload,
          selection,
          scopeContext.allowedStudyIds
        )
      : { query: scoped.query, payload: scoped.payload };
  const queryString = new URLSearchParams(supabaseFallback.query).toString();
  const query = queryString ? `?${queryString}` : "";
  const response = await handleWithDataSource(
    request,
    `/analytics/tracking/series${query}`,
    "bbs_tracking_series",
    {
      query: supabaseFallback.query,
      payload: supabaseFallback.payload,
    },
    { method: "GET" }
  );
  if (!response.ok) {
    if (taxonomyView === "market" && getDataSource() === "supabase") {
      const constrained = await constrainLegacyFallbackToSupabaseStudies(
        request,
        supabaseFallback.query,
        supabaseFallback.payload
      );
      const legacyPayload = await tryLegacyTrackingSeries("GET", constrained.query, constrained.payload);
      if (legacyPayload) {
        const normalizedLegacy = normalizeTrackingPayloadTaxonomy(legacyPayload, taxonomyView);
        const filteredLegacy = filterTrackingBySelection(normalizedLegacy, taxonomyView, selection);
        return NextResponse.json(filteredLegacy);
      }
    }
    return response;
  }
  const raw = await response.json().catch(() => null);
  const preferred = await maybeQuarterFallbackFromLegacy(
    request,
    "GET",
    supabaseFallback.query,
    supabaseFallback.payload,
    raw
  );
  if (taxonomyView !== "market") {
    return NextResponse.json(preferred, { status: response.status });
  }
  const normalized = normalizeTrackingPayloadTaxonomy(preferred, taxonomyView);
  const filteredNormalized = filterTrackingBySelection(normalized, taxonomyView, selection);
  if (selection.sector && !selection.subsector && !selection.category) {
    if (countEntities(filteredNormalized) <= 1 && countEntities(normalized) > 1) {
      return NextResponse.json(normalized, { status: response.status });
    }
  }
  const rebuiltMarket = await rebuildSupabaseMarketSeries(
    request,
    supabaseFallback.payload,
    selection
  );
  if (rebuiltMarket && hasMeaningfulEntities(rebuiltMarket)) {
    const filteredRebuilt = filterTrackingBySelection(rebuiltMarket, taxonomyView, selection);
    if (hasMeaningfulEntities(filteredRebuilt)) {
      return NextResponse.json(filteredRebuilt, { status: response.status });
    }
  }
  return NextResponse.json(filteredNormalized, { status: response.status });
}

export async function POST(request: NextRequest) {
  const scopeContext = await getScopeContext(request);
  if (scopeContext.allowedStudyIds && scopeContext.allowedStudyIds.length === 0) {
    return NextResponse.json(emptyTrackingSeries());
  }
  const payload = expandNseInPayload((await request.json().catch(() => ({}))) as Record<string, unknown>);
  const taxonomyView =
    typeof payload.taxonomy_view === "string" && payload.taxonomy_view.toLowerCase() === "standard"
      ? "standard"
      : request.nextUrl.searchParams.get("taxonomy_view") === "standard"
        ? "standard"
        : "market";
  const selection = {
    sector: (typeof payload.sector === "string" ? payload.sector : null) ?? request.nextUrl.searchParams.get("sector"),
    subsector:
      (typeof payload.subsector === "string" ? payload.subsector : null) ??
      request.nextUrl.searchParams.get("subsector"),
    category:
      (typeof payload.category === "string" ? payload.category : null) ?? request.nextUrl.searchParams.get("category"),
  };
  const marketScoped = await applyMarketFilterToStudyIds({
    query: expandNseInQuery(Object.fromEntries(request.nextUrl.searchParams.entries())),
    payload,
    allowedStudyIds: scopeContext.allowedStudyIds,
    preserveTaxonomyFilters: true,
  });
  const queryWithPayload = { ...marketScoped.query };
  if (!queryWithPayload.taxonomy_view && typeof marketScoped.payload.taxonomy_view === "string") {
    queryWithPayload.taxonomy_view = marketScoped.payload.taxonomy_view;
  }
  if (!queryWithPayload.sector && typeof marketScoped.payload.sector === "string") {
    queryWithPayload.sector = marketScoped.payload.sector;
  }
  if (!queryWithPayload.subsector && typeof marketScoped.payload.subsector === "string") {
    queryWithPayload.subsector = marketScoped.payload.subsector;
  }
  if (!queryWithPayload.category && typeof marketScoped.payload.category === "string") {
    queryWithPayload.category = marketScoped.payload.category;
  }
  const scoped = withStudyScope(
    queryWithPayload,
    marketScoped.payload,
    scopeContext.allowedStudyIds
  );
  const supabaseFallback =
    taxonomyView === "market"
      ? await applySupabaseMarketFallback(
          scoped.query,
          scoped.payload,
          selection,
          scopeContext.allowedStudyIds
        )
      : { query: scoped.query, payload: scoped.payload };
  const queryString = new URLSearchParams(supabaseFallback.query).toString();
  const query = queryString ? `?${queryString}` : "";
  const response = await handleWithDataSource(
    request,
    `/analytics/tracking/series${query}`,
    "bbs_tracking_series",
    {
      query: supabaseFallback.query,
      payload: supabaseFallback.payload,
    },
    { method: "POST" }
  );
  if (!response.ok) {
    if (taxonomyView === "market" && getDataSource() === "supabase") {
      const constrained = await constrainLegacyFallbackToSupabaseStudies(
        request,
        supabaseFallback.query,
        supabaseFallback.payload
      );
      const legacyPayload = await tryLegacyTrackingSeries("POST", constrained.query, constrained.payload);
      if (legacyPayload) {
        const normalizedLegacy = normalizeTrackingPayloadTaxonomy(legacyPayload, taxonomyView);
        const filteredLegacy = filterTrackingBySelection(normalizedLegacy, taxonomyView, selection);
        return NextResponse.json(filteredLegacy);
      }
    }
    return response;
  }
  const raw = await response.json().catch(() => null);
  const preferred = await maybeQuarterFallbackFromLegacy(
    request,
    "POST",
    supabaseFallback.query,
    supabaseFallback.payload,
    raw
  );
  if (taxonomyView !== "market") {
    return NextResponse.json(preferred, { status: response.status });
  }
  const normalized = normalizeTrackingPayloadTaxonomy(preferred, taxonomyView);
  const filteredNormalized = filterTrackingBySelection(normalized, taxonomyView, selection);
  if (selection.sector && !selection.subsector && !selection.category) {
    if (countEntities(filteredNormalized) <= 1 && countEntities(normalized) > 1) {
      return NextResponse.json(normalized, { status: response.status });
    }
  }
  const rebuiltMarket = await rebuildSupabaseMarketSeries(
    request,
    supabaseFallback.payload,
    selection
  );
  if (rebuiltMarket && hasMeaningfulEntities(rebuiltMarket)) {
    const filteredRebuilt = filterTrackingBySelection(rebuiltMarket, taxonomyView, selection);
    if (hasMeaningfulEntities(filteredRebuilt)) {
      return NextResponse.json(filteredRebuilt, { status: response.status });
    }
  }
  return NextResponse.json(filteredNormalized, { status: response.status });
}



