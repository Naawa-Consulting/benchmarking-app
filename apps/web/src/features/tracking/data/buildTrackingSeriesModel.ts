import type { TrackingSeriesModel } from "../types";

function asObject(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

export function buildTrackingSeriesModel(payload: unknown): TrackingSeriesModel {
  const root = asObject(payload);
  const fallbackBrandRows = Array.isArray(root.brand_rows) ? (root.brand_rows as TrackingSeriesModel["brand_rows"]) : [];
  const fallbackTouchpointRows = Array.isArray(root.touchpoint_rows)
    ? (root.touchpoint_rows as TrackingSeriesModel["touchpoint_rows"])
    : [];
  const entityRows = Array.isArray(root.entity_rows)
    ? (root.entity_rows as TrackingSeriesModel["entity_rows"])
    : fallbackBrandRows.map((row) => ({ entity: row.brand, metrics: row.metrics }));
  const secondaryRows = Array.isArray(root.secondary_rows)
    ? (root.secondary_rows as TrackingSeriesModel["secondary_rows"])
    : fallbackTouchpointRows.map((row) => ({ entity: row.touchpoint, metrics: row.metrics }));

  return {
    ok: Boolean(root.ok),
    resolved_granularity: root.resolved_granularity === "quarter" ? "quarter" : "year",
    resolved_breakdown:
      root.resolved_breakdown === "sector" ||
      root.resolved_breakdown === "subsector" ||
      root.resolved_breakdown === "category"
        ? root.resolved_breakdown
        : "brand",
    entity_label: typeof root.entity_label === "string" ? root.entity_label : "Brand",
    periods: Array.isArray(root.periods) ? (root.periods as TrackingSeriesModel["periods"]) : [],
    delta_columns: Array.isArray(root.delta_columns)
      ? (root.delta_columns as TrackingSeriesModel["delta_columns"])
      : [],
    entity_rows: entityRows,
    secondary_rows: secondaryRows,
    brand_rows: fallbackBrandRows,
    touchpoint_rows: fallbackTouchpointRows,
    metric_meta_brand: asObject(root.metric_meta_brand) as TrackingSeriesModel["metric_meta_brand"],
    metric_meta_touchpoint: asObject(root.metric_meta_touchpoint) as TrackingSeriesModel["metric_meta_touchpoint"],
    meta: asObject(root.meta) as TrackingSeriesModel["meta"],
  };
}

export function filterTrackingSeriesByBrands(
  model: TrackingSeriesModel,
  selectedBrands: string[]
): TrackingSeriesModel {
  if (!selectedBrands.length) return model;
  if (model.resolved_breakdown !== "brand") return model;
  const keep = new Set(selectedBrands.map((item) => item.toLowerCase().trim()));
  return {
    ...model,
    entity_rows: model.entity_rows.filter((row) => keep.has(row.entity.toLowerCase().trim())),
    brand_rows: model.brand_rows.filter((row) => keep.has(row.brand.toLowerCase().trim())),
    secondary_rows: model.secondary_rows,
    touchpoint_rows: model.touchpoint_rows,
  };
}
