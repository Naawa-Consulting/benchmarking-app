import type { TrackingComparisonModel, TrackingMetricKey } from "../types";

const METRIC_KEYS: TrackingMetricKey[] = [
  "brand_awareness",
  "ad_awareness",
  "brand_consideration",
  "brand_purchase",
  "brand_satisfaction",
  "brand_recommendation",
  "csat",
  "nps",
];

function toCell(value: number | null) {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return Number(value.toFixed(2));
}

export async function exportTrackingXlsx(model: TrackingComparisonModel) {
  const xlsx = await import("xlsx");
  const workbook = xlsx.utils.book_new();

  const dataRows = model.brands.map((brand) => {
    const row: Record<string, string | number | null> = { Brand: brand.brandName };
    for (const key of METRIC_KEYS) {
      if (!model.metricMeta[key].available) continue;
      row[`${model.metricMeta[key].label} Pre`] = toCell(brand.metrics[key].valueEarlier);
      row[`${model.metricMeta[key].label} Post`] = toCell(brand.metrics[key].valueLater);
      row[`${model.metricMeta[key].label} Delta pts`] = toCell(brand.metrics[key].deltaAbs);
      row[`${model.metricMeta[key].label} Delta %`] = toCell(brand.metrics[key].deltaRelPct);
    }
    return row;
  });

  const metadataRows = [
    { Field: "Pre Study ID", Value: model.preStudyId },
    { Field: "Pre Study Label", Value: model.preLabel },
    { Field: "Post Study ID", Value: model.postStudyId },
    { Field: "Post Study Label", Value: model.postLabel },
    { Field: "Sector", Value: model.activeFiltersSummary.sector || "" },
    { Field: "Subsector", Value: model.activeFiltersSummary.subsector || "" },
    { Field: "Category", Value: model.activeFiltersSummary.category || "" },
    { Field: "Gender", Value: model.activeFiltersSummary.gender.join(", ") || "All" },
    { Field: "NSE", Value: model.activeFiltersSummary.nse.join(", ") || "All" },
    { Field: "State", Value: model.activeFiltersSummary.state.join(", ") || "All" },
    { Field: "Age Range", Value: `${model.activeFiltersSummary.ageMin ?? ""}-${model.activeFiltersSummary.ageMax ?? ""}` },
    { Field: "Year Range", Value: `${model.activeFiltersSummary.quarterFrom ?? ""} -> ${model.activeFiltersSummary.quarterTo ?? ""}` },
    { Field: "KPI basis", Value: "Promedio de marcas visibles" },
    { Field: "Exported At", Value: new Date().toISOString() },
    { Field: "Warnings", Value: model.warnings.join(" | ") || "" },
  ];

  xlsx.utils.book_append_sheet(workbook, xlsx.utils.json_to_sheet(dataRows), "Tracking_Comparison");
  xlsx.utils.book_append_sheet(workbook, xlsx.utils.json_to_sheet(metadataRows), "Metadata");
  xlsx.writeFile(workbook, `tracking-comparison-${Date.now()}.xlsx`);
}
