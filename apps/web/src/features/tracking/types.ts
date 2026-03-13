export type TrackingBrandMetricKey =
  | "brand_awareness"
  | "ad_awareness"
  | "brand_consideration"
  | "brand_purchase"
  | "brand_satisfaction"
  | "brand_recommendation"
  | "csat"
  | "nps";

export type TrackingTouchpointMetricKey = "recall" | "consideration" | "purchase";
export type TrackingBreakdownLevel = "sector" | "subsector" | "category" | "brand";

export type TrackingPeriod = {
  key: string;
  label: string;
  order: number;
};

export type TrackingDeltaColumn = {
  key: string;
  from: string;
  to: string;
  label: string;
};

export type TrackingSeriesMetric = {
  values: Record<string, number | null>;
  deltas: Record<string, number | null>;
};

export type TrackingMetricMeta = {
  label: string;
  unit: "%" | "pts";
};

export type TrackingBrandRow = {
  brand: string;
  metrics: Record<TrackingBrandMetricKey, TrackingSeriesMetric>;
};

export type TrackingTouchpointRow = {
  touchpoint: string;
  metrics: Record<TrackingTouchpointMetricKey, TrackingSeriesMetric>;
};

export type TrackingEntityRow = {
  entity: string;
  metrics: Record<TrackingBrandMetricKey, TrackingSeriesMetric>;
};

export type TrackingSecondaryRow = {
  entity: string;
  metrics: Record<TrackingTouchpointMetricKey, TrackingSeriesMetric>;
};

export type TrackingSeriesModel = {
  ok: boolean;
  resolved_granularity: "year" | "quarter";
  resolved_breakdown: TrackingBreakdownLevel;
  entity_label: string;
  periods: TrackingPeriod[];
  delta_columns: TrackingDeltaColumn[];
  entity_rows: TrackingEntityRow[];
  secondary_rows: TrackingSecondaryRow[];
  brand_rows: TrackingBrandRow[];
  touchpoint_rows: TrackingTouchpointRow[];
  metric_meta_brand: Record<TrackingBrandMetricKey, TrackingMetricMeta>;
  metric_meta_touchpoint: Record<TrackingTouchpointMetricKey, TrackingMetricMeta>;
  meta: {
    warnings?: string[];
    studies_considered?: string[];
    studies_used?: string[];
    studies_with_data?: string[];
    response_mode?: string;
    cache_hit?: boolean;
  };
};
