import type { TrackingBrandMetricKey, TrackingSeriesModel, TrackingTouchpointMetricKey } from "../types";

const BRAND_METRICS: TrackingBrandMetricKey[] = [
  "brand_awareness",
  "ad_awareness",
  "brand_consideration",
  "brand_purchase",
  "brand_satisfaction",
  "brand_recommendation",
  "csat",
  "nps",
];
const TOUCHPOINT_METRICS: TrackingTouchpointMetricKey[] = ["recall", "consideration", "purchase"];

function toCell(value: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return Number(value.toFixed(2));
}

export async function exportTrackingXlsx(model: TrackingSeriesModel) {
  const xlsx = await import("xlsx");
  const workbook = xlsx.utils.book_new();

  const entityRows = model.entity_rows.map((row) => {
    const output: Record<string, string | number | null> = { [model.entity_label]: row.entity };
    for (const metricKey of BRAND_METRICS) {
      const meta = model.metric_meta_brand[metricKey];
      if (!meta) continue;
      for (const period of model.periods) {
        output[`${meta.label} ${period.label}`] = toCell(row.metrics[metricKey]?.values?.[period.key] ?? null);
      }
      for (const delta of model.delta_columns) {
        output[`${meta.label} ${delta.label}`] = toCell(row.metrics[metricKey]?.deltas?.[delta.key] ?? null);
      }
    }
    return output;
  });

  const secondaryRows = model.secondary_rows.map((row) => {
    const output: Record<string, string | number | null> = { Touchpoint: row.entity };
    for (const metricKey of TOUCHPOINT_METRICS) {
      const meta = model.metric_meta_touchpoint[metricKey];
      if (!meta) continue;
      for (const period of model.periods) {
        output[`${meta.label} ${period.label}`] = toCell(row.metrics[metricKey]?.values?.[period.key] ?? null);
      }
      for (const delta of model.delta_columns) {
        output[`${meta.label} ${delta.label}`] = toCell(row.metrics[metricKey]?.deltas?.[delta.key] ?? null);
      }
    }
    return output;
  });

  const metadataRows = [
    { Field: "Resolved granularity", Value: model.resolved_granularity },
    { Field: "Resolved breakdown", Value: model.resolved_breakdown },
    { Field: "Entity label", Value: model.entity_label },
    { Field: "Periods", Value: model.periods.map((item) => item.label).join(", ") },
    { Field: "Studies considered", Value: (model.meta.studies_considered || []).join(", ") },
    { Field: "Studies used", Value: (model.meta.studies_used || []).join(", ") },
    { Field: "Warnings", Value: (model.meta.warnings || []).join(" | ") },
    { Field: "Exported At", Value: new Date().toISOString() },
  ];

  xlsx.utils.book_append_sheet(workbook, xlsx.utils.json_to_sheet(entityRows), "Comparison");
  xlsx.utils.book_append_sheet(workbook, xlsx.utils.json_to_sheet(secondaryRows), "Secondary");
  xlsx.utils.book_append_sheet(workbook, xlsx.utils.json_to_sheet(metadataRows), "Metadata");
  xlsx.writeFile(workbook, `tracking-series-${Date.now()}.xlsx`);
}
