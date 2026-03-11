export type TrackingMetricKey =
  | "brand_awareness"
  | "ad_awareness"
  | "brand_consideration"
  | "brand_purchase"
  | "brand_satisfaction"
  | "brand_recommendation"
  | "csat"
  | "nps";

export type TrackingMetricMeta = {
  key: TrackingMetricKey;
  label: string;
  unit: "%" | "pts";
  available: boolean;
};

export type TrackingBrandMetric = {
  valueEarlier: number | null;
  valueLater: number | null;
  deltaAbs: number | null;
  deltaRelPct: number | null;
};

export type TrackingBrandRow = {
  brandName: string;
  metrics: Record<TrackingMetricKey, TrackingBrandMetric>;
  summaryScore: number | null;
};

export type TrackingComparisonModel = {
  preLabel: string;
  postLabel: string;
  preStudyId: string;
  postStudyId: string;
  brands: TrackingBrandRow[];
  metricMeta: Record<TrackingMetricKey, TrackingMetricMeta>;
  warnings: string[];
  aggregationBasis: "mean_visible_brands";
  activeFiltersSummary: {
    sector: string | null;
    subsector: string | null;
    category: string | null;
    gender: string[];
    nse: string[];
    state: string[];
    ageMin: number | null;
    ageMax: number | null;
    quarterFrom: string | null;
    quarterTo: string | null;
  };
};
