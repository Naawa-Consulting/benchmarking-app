export const JOURNEY_STAGES = [
  "Brand Awareness",
  "Ad Awareness",
  "Brand Consideration",
  "Brand Purchase",
  "Brand Satisfaction",
  "Brand Recommendation",
] as const;

export type JourneyStage = (typeof JOURNEY_STAGES)[number];

export type JourneyDims = {
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  demo?: string | null;
  time?: string | null;
};

export type JourneyStageRow = {
  studyId: string;
  brandId?: string | null;
  brandName: string;
  dims: JourneyDims;
  stage: JourneyStage;
  // Internally normalized to 0..1 (not 0..100).
  value: number;
  weight: number;
  baseN?: number | null;
  raw?: Record<string, unknown>;
};

export type JourneyStageAggregate = {
  stage: JourneyStage;
  value: number | null;
  stageCoverageStudies: number;
  stageCoverageWeight: number;
};

export type JourneyLinkAggregate = {
  fromStage: JourneyStage;
  toStage: JourneyStage;
  dropAbs: number | null;
  conversion: number | null;
  linkCoverageStudies: number;
  linkCoverageWeight: number;
};

export type JourneyMetricMeta = {
  metricType: "official" | "proxy";
  explanation: string;
};

export type JourneyMetricValue = {
  value: number | null;
  meta: JourneyMetricMeta;
};

export type JourneyBrandAggregate = {
  key: string;
  brandId?: string | null;
  brandName: string;
  dims: JourneyDims;
  stageAggregates: JourneyStageAggregate[];
  links: JourneyLinkAggregate[];
  totalConversion: number | null;
  csat: JourneyMetricValue;
  nps: JourneyMetricValue;
};

export type JourneyBenchmarkScope = "category" | "sector";

export type JourneyBenchmarkAggregate = {
  scope: JourneyBenchmarkScope;
  stageAggregates: JourneyStageAggregate[];
  links: JourneyLinkAggregate[];
  csat: JourneyMetricValue;
  nps: JourneyMetricValue;
};

export type JourneyGapByStage = {
  stage: JourneyStage;
  valueGap: number | null;
};

export type JourneyBrandGap = {
  key: string;
  brandName: string;
  gaps: JourneyGapByStage[];
};

export type JourneyStageRank = {
  key: string;
  brandName: string;
  rank: number;
  value: number;
};

export type JourneyCoverageSummary = {
  byStage: Array<{
    stage: JourneyStage;
    studies: number;
    weight: number;
  }>;
  byLink: Array<{
    fromStage: JourneyStage;
    toStage: JourneyStage;
    studies: number;
    weight: number;
  }>;
};

export type JourneyModelMetadata = {
  includeAdAwareness: boolean;
  warnings: string[];
  coverage: JourneyCoverageSummary;
};

export type JourneyModel = {
  stagesOrdered: JourneyStage[];
  rows: JourneyStageRow[];
  brandStageAggregates: JourneyBrandAggregate[];
  benchmarkStageAggregates: JourneyBenchmarkAggregate;
  stageGaps: JourneyBrandGap[];
  links: JourneyLinkAggregate[];
  ranksByStage: Record<JourneyStage, JourneyStageRank[]>;
  metadata: JourneyModelMetadata;
};

export type BuildJourneyOptions = {
  includeAdAwareness?: boolean;
  benchmarkScope?: JourneyBenchmarkScope;
};

export type BrandGroupDims = JourneyDims & {
  brandName: string;
  brandId?: string | null;
};

export const DEFAULT_BUILD_JOURNEY_OPTIONS: Required<BuildJourneyOptions> = {
  includeAdAwareness: true,
  benchmarkScope: "category",
};

export const STAGE_ALIAS_TO_CANONICAL: Record<string, JourneyStage> = {
  "brand awareness": "Brand Awareness",
  brandawareness: "Brand Awareness",
  awareness: "Brand Awareness",
  "ad awareness": "Ad Awareness",
  adawareness: "Ad Awareness",
  "brand consideration": "Brand Consideration",
  brandconsideration: "Brand Consideration",
  consideration: "Brand Consideration",
  "brand purchase": "Brand Purchase",
  brandpurchase: "Brand Purchase",
  purchase: "Brand Purchase",
  "brand satisfaction": "Brand Satisfaction",
  brandsatisfaction: "Brand Satisfaction",
  satisfaction: "Brand Satisfaction",
  "brand recommendation": "Brand Recommendation",
  brandrecommendation: "Brand Recommendation",
  recommendation: "Brand Recommendation",
};

