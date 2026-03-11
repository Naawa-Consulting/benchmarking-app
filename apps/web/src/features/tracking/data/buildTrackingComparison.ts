import type { JourneyBrandAggregate, JourneyModel, JourneyStage } from "../../journey/data/journeySchema";
import type { TrackingBrandMetric, TrackingBrandRow, TrackingComparisonModel, TrackingMetricKey } from "../types";

type TrackingStudyInput = {
  studyId: string;
  studyLabel: string;
  model: JourneyModel;
};

type BuildTrackingComparisonOptions = {
  includeAdAwareness?: boolean;
  activeFiltersSummary: TrackingComparisonModel["activeFiltersSummary"];
};

const METRIC_ORDER: TrackingMetricKey[] = [
  "brand_awareness",
  "ad_awareness",
  "brand_consideration",
  "brand_purchase",
  "brand_satisfaction",
  "brand_recommendation",
  "csat",
  "nps",
];

const STAGE_BY_METRIC: Record<
  Exclude<TrackingMetricKey, "csat" | "nps">,
  Extract<JourneyStage, "Brand Awareness" | "Ad Awareness" | "Brand Consideration" | "Brand Purchase" | "Brand Satisfaction" | "Brand Recommendation">
> = {
  brand_awareness: "Brand Awareness",
  ad_awareness: "Ad Awareness",
  brand_consideration: "Brand Consideration",
  brand_purchase: "Brand Purchase",
  brand_satisfaction: "Brand Satisfaction",
  brand_recommendation: "Brand Recommendation",
};

const METRIC_LABELS: Record<TrackingMetricKey, { label: string; unit: "%" | "pts" }> = {
  brand_awareness: { label: "Brand Awareness", unit: "%" },
  ad_awareness: { label: "Ad Awareness", unit: "%" },
  brand_consideration: { label: "Brand Consideration", unit: "%" },
  brand_purchase: { label: "Brand Purchase", unit: "%" },
  brand_satisfaction: { label: "Brand Satisfaction", unit: "%" },
  brand_recommendation: { label: "Brand Recommendation", unit: "%" },
  csat: { label: "CSAT", unit: "pts" },
  nps: { label: "NPS", unit: "pts" },
};

function roundOrNull(value: number | null, digits = 1) {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return Number(value.toFixed(digits));
}

function getBrandMetricValue(brand: JourneyBrandAggregate | null, key: TrackingMetricKey): number | null {
  if (!brand) return null;
  if (key === "csat") return brand.csat.value == null ? null : brand.csat.value * 100;
  if (key === "nps") return brand.nps.value == null ? null : brand.nps.value * 100;
  const stage = STAGE_BY_METRIC[key];
  const aggregate = brand.stageAggregates.find((item) => item.stage === stage);
  return aggregate?.value == null ? null : aggregate.value * 100;
}

function buildMetricPair(valueEarlier: number | null, valueLater: number | null): TrackingBrandMetric {
  if (valueEarlier == null || valueLater == null) {
    return {
      valueEarlier: roundOrNull(valueEarlier),
      valueLater: roundOrNull(valueLater),
      deltaAbs: null,
      deltaRelPct: null,
    };
  }
  const deltaAbs = valueLater - valueEarlier;
  const deltaRelPct = valueEarlier > 0 ? (deltaAbs / valueEarlier) * 100 : null;
  return {
    valueEarlier: roundOrNull(valueEarlier),
    valueLater: roundOrNull(valueLater),
    deltaAbs: roundOrNull(deltaAbs),
    deltaRelPct: roundOrNull(deltaRelPct),
  };
}

function byBrandName(model: JourneyModel) {
  return new Map(model.brandStageAggregates.map((brand) => [brand.brandName, brand]));
}

function emptyMetricRecord(): Record<TrackingMetricKey, TrackingBrandMetric> {
  const result = {} as Record<TrackingMetricKey, TrackingBrandMetric>;
  for (const key of METRIC_ORDER) {
    result[key] = { valueEarlier: null, valueLater: null, deltaAbs: null, deltaRelPct: null };
  }
  return result;
}

function summaryScoreFromRow(row: TrackingBrandRow) {
  const values = METRIC_ORDER.map((metricKey) => row.metrics[metricKey].deltaAbs).filter(
    (value): value is number => typeof value === "number"
  );
  if (!values.length) return null;
  return Number((values.reduce((acc, value) => acc + value, 0) / values.length).toFixed(2));
}

export function buildTrackingComparison(
  studyA: TrackingStudyInput,
  studyB: TrackingStudyInput,
  options: BuildTrackingComparisonOptions
): TrackingComparisonModel {
  const includeAdAwareness = options.includeAdAwareness ?? true;
  const warnings: string[] = [];

  const brandsA = byBrandName(studyA.model);
  const brandsB = byBrandName(studyB.model);
  const allBrandNames = Array.from(new Set([...brandsA.keys(), ...brandsB.keys()])).sort((a, b) => a.localeCompare(b));

  const rows: TrackingBrandRow[] = allBrandNames.map((brandName) => {
    const left = brandsA.get(brandName) ?? null;
    const right = brandsB.get(brandName) ?? null;

    if (!left || !right) {
      warnings.push(
        `${brandName} is only available in ${left ? studyA.studyLabel : studyB.studyLabel}; missing values were left empty.`
      );
    }

    const metrics = emptyMetricRecord();
    for (const metricKey of METRIC_ORDER) {
      metrics[metricKey] = buildMetricPair(getBrandMetricValue(left, metricKey), getBrandMetricValue(right, metricKey));
    }

    const row: TrackingBrandRow = {
      brandName,
      metrics,
      summaryScore: null,
    };
    row.summaryScore = summaryScoreFromRow(row);
    return row;
  });

  const metricMeta = METRIC_ORDER.reduce<TrackingComparisonModel["metricMeta"]>((acc, key) => {
    acc[key] = {
      key,
      label: METRIC_LABELS[key].label,
      unit: METRIC_LABELS[key].unit,
      available: includeAdAwareness || key !== "ad_awareness",
    };
    return acc;
  }, {} as TrackingComparisonModel["metricMeta"]);

  return {
    preStudyId: studyA.studyId,
    postStudyId: studyB.studyId,
    preLabel: studyA.studyLabel,
    postLabel: studyB.studyLabel,
    brands: rows,
    metricMeta,
    warnings,
    aggregationBasis: "mean_visible_brands",
    activeFiltersSummary: options.activeFiltersSummary,
  };
}

type DemoPayloadScope = {
  gender: string[];
  nse: string[];
  state: string[];
};

type DemoPayload = {
  gender: string | null;
  nse: string | null;
  state: string | null;
};

function crossJoin(values: Array<Array<string | null>>) {
  return values.reduce<Array<Array<string | null>>>(
    (acc, list) => acc.flatMap((prefix) => list.map((value) => [...prefix, value])),
    [[]]
  );
}

export function buildMultiDemoPayloads(scope: DemoPayloadScope): DemoPayload[] {
  const genders = scope.gender.length ? scope.gender : [null];
  const nseValues = scope.nse.length ? scope.nse : [null];
  const states = scope.state.length ? scope.state : [null];

  const combos = crossJoin([genders, nseValues, states]).map(([gender, nse, state]) => ({
    gender,
    nse,
    state,
  }));

  const unique = new Map<string, DemoPayload>();
  for (const combo of combos) {
    unique.set(`${combo.gender ?? "*"}|${combo.nse ?? "*"}|${combo.state ?? "*"}`, combo);
  }
  return Array.from(unique.values());
}
