import {
  DEFAULT_BUILD_JOURNEY_OPTIONS,
  JOURNEY_STAGES,
  type BuildJourneyOptions,
  type JourneyBenchmarkAggregate,
  type JourneyBenchmarkScope,
  type JourneyBrandAggregate,
  type JourneyBrandGap,
  type JourneyCoverageSummary,
  type JourneyGapByStage,
  type JourneyIndexConfidence,
  type JourneyIndexEntry,
  type JourneyLinkAggregate,
  type JourneyMetricValue,
  type JourneyModel,
  type FunnelHealthEntry,
  type JourneyStage,
  type JourneyStageAggregate,
  type JourneyStageRank,
  type JourneyStageRow,
} from "./journeySchema";
import { normalizeJourneyResults } from "./journeyTransforms";

type StagePoint = { value: number; weight: number; studyId: string };

const OFFICIAL_CSAT_FIELDS = ["csat_score", "satisfaction_score", "csat"];
const OFFICIAL_PROMOTERS_FIELDS = ["promoters_pct", "nps_promoters_pct"];
const OFFICIAL_DETRACTORS_FIELDS = ["detractors_pct", "nps_detractors_pct"];
const OFFICIAL_NPS_FIELDS = ["nps", "nps_score"];
const JOURNEY_INDEX_WEIGHTS = { retention: 0.5, gap: 0.3, csat: 0.1, nps: 0.1 } as const;
const GAP_CLAMP = 0.15;
const FUNNEL_HEALTH_THRESHOLDS = { healthy: 10, moderate: 20 } as const;

const stageKey = (stage: JourneyStage) => stage;
const pairKey = (from: JourneyStage, to: JourneyStage) => `${from} -> ${to}`;
const isCoreConversionLink = (from: JourneyStage, to: JourneyStage) =>
  from !== "Ad Awareness" && to !== "Ad Awareness";
const toCoreConversionStages = (orderedStages: JourneyStage[]) =>
  orderedStages.filter((stage) => stage !== "Ad Awareness");

const toOrderedStages = (includeAdAwareness: boolean): JourneyStage[] =>
  includeAdAwareness ? [...JOURNEY_STAGES] : JOURNEY_STAGES.filter((stage) => stage !== "Ad Awareness");

const numberOrNull = (value: unknown): number | null => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value.replace("%", "").trim());
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
};

const weightedAverage = (points: Array<{ value: number; weight: number }>): number | null => {
  const valid = points.filter((point) => Number.isFinite(point.value) && point.weight > 0);
  if (!valid.length) return null;
  const totalWeight = valid.reduce((sum, point) => sum + point.weight, 0);
  if (totalWeight <= 0) return null;
  const weightedSum = valid.reduce((sum, point) => sum + point.value * point.weight, 0);
  return weightedSum / totalWeight;
};

const groupKey = (row: JourneyStageRow) =>
  JSON.stringify({
    brandName: row.brandName,
    brandId: row.brandId ?? null,
    sector: row.dims.sector ?? null,
    subsector: row.dims.subsector ?? null,
    category: row.dims.category ?? null,
    demo: row.dims.demo ?? null,
    time: row.dims.time ?? null,
  });

const benchmarkKey = (row: JourneyStageRow, scope: JourneyBenchmarkScope) =>
  JSON.stringify({
    sector: row.dims.sector ?? null,
    subsector: scope === "category" ? row.dims.subsector ?? null : null,
    category: scope === "category" ? row.dims.category ?? null : null,
    demo: row.dims.demo ?? null,
    time: row.dims.time ?? null,
  });

const computeStageAggregates = (
  rows: JourneyStageRow[],
  orderedStages: JourneyStage[]
): { stageAggregates: JourneyStageAggregate[]; byStageCoverage: Map<JourneyStage, { studies: Set<string>; weight: number }> } => {
  const byStage = new Map<JourneyStage, StagePoint[]>();
  const coverage = new Map<JourneyStage, { studies: Set<string>; weight: number }>();
  for (const stage of orderedStages) {
    byStage.set(stage, []);
    coverage.set(stage, { studies: new Set<string>(), weight: 0 });
  }
  for (const row of rows) {
    if (!byStage.has(row.stage)) continue;
    byStage.get(row.stage)?.push({ value: row.value, weight: row.weight, studyId: row.studyId });
    const bucket = coverage.get(row.stage);
    if (bucket) {
      bucket.studies.add(row.studyId);
      bucket.weight += row.weight;
    }
  }
  const stageAggregatesRaw = orderedStages.map((stage) => ({
    stage,
    value: weightedAverage((byStage.get(stage) || []).map((point) => ({ value: point.value, weight: point.weight }))),
    stageCoverageStudies: coverage.get(stage)?.studies.size || 0,
    stageCoverageWeight: coverage.get(stage)?.weight || 0,
  }));
  const values = new Map<JourneyStage, number | null>(
    stageAggregatesRaw.map((item) => [item.stage, item.value])
  );
  const awareness = values.get("Brand Awareness");
  if (typeof awareness === "number") {
    for (const stage of [
      "Brand Consideration",
      "Brand Purchase",
      "Brand Satisfaction",
      "Brand Recommendation",
    ] as const) {
      const current = values.get(stage);
      if (typeof current === "number" && current > awareness) {
        values.set(stage, awareness);
      }
    }
  }
  const purchase = values.get("Brand Purchase");
  if (typeof purchase === "number") {
    for (const stage of ["Brand Satisfaction", "Brand Recommendation"] as const) {
      const current = values.get(stage);
      if (typeof current === "number" && current > purchase) {
        values.set(stage, purchase);
      }
    }
  }
  const stageAggregates = stageAggregatesRaw.map((item) => ({
    ...item,
    value: values.get(item.stage) ?? null,
  }));
  return { stageAggregates, byStageCoverage: coverage };
};

const computeLinkAggregates = (
  rows: JourneyStageRow[],
  orderedStages: JourneyStage[],
  stageAggregates: JourneyStageAggregate[]
): JourneyLinkAggregate[] => {
  const byStudy = new Map<string, Map<JourneyStage, { value: number; weight: number }>>();
  const stageValueMap = new Map<JourneyStage, number | null>(
    stageAggregates.map((item) => [item.stage, item.value])
  );
  for (const row of rows) {
    if (!orderedStages.includes(row.stage)) continue;
    if (!byStudy.has(row.studyId)) byStudy.set(row.studyId, new Map());
    const stageMap = byStudy.get(row.studyId)!;
    const existing = stageMap.get(row.stage);
    if (!existing || row.weight > existing.weight) {
      stageMap.set(row.stage, { value: row.value, weight: row.weight });
    }
  }

  const links: JourneyLinkAggregate[] = [];
  for (let i = 0; i < orderedStages.length - 1; i += 1) {
    const fromStage = orderedStages[i];
    const toStage = orderedStages[i + 1];
    const fromStageValue = stageValueMap.get(fromStage);
    const toStageValue = stageValueMap.get(toStage);
    const studies = new Set<string>();
    let weightTotal = 0;
    let anomalyStudies = 0;

    for (const [studyId, stages] of byStudy) {
      const from = stages.get(fromStage);
      const to = stages.get(toStage);
      if (!from || !to) continue;
      const weight = Math.min(from.weight, to.weight);
      if (weight <= 0) continue;
      if (from.value <= 0 || to.value > from.value) {
        anomalyStudies += 1;
      }
      studies.add(studyId);
      weightTotal += weight;
    }

    const conversionRaw =
      typeof fromStageValue === "number" &&
      typeof toStageValue === "number" &&
      fromStageValue > 0
        ? toStageValue / fromStageValue
        : null;
    const conversionForIndex =
      typeof conversionRaw === "number" && conversionRaw <= 1 ? conversionRaw : null;
    const anomalyFlag =
      anomalyStudies > 0 ||
      (typeof conversionRaw === "number" && conversionRaw > 1) ||
      (typeof fromStageValue === "number" &&
        typeof toStageValue === "number" &&
        toStageValue > fromStageValue);

    links.push({
      fromStage,
      toStage,
      dropAbs:
        typeof fromStageValue === "number" && typeof toStageValue === "number"
          ? fromStageValue - toStageValue
          : null,
      conversion: conversionRaw,
      conversionForIndex,
      linkCoverageStudies: studies.size,
      linkCoverageWeight: weightTotal,
      anomalyFlag,
      anomalyStudies,
      excludedFromIndex: anomalyFlag,
    });
  }
  return links;
};

const computeTotalConversion = (stageAggregates: JourneyStageAggregate[], orderedStages: JourneyStage[]) => {
  const firstStage = orderedStages[0];
  const lastStage = orderedStages[orderedStages.length - 1];
  const firstValue = stageAggregates.find((item) => item.stage === firstStage)?.value ?? null;
  const lastValue = stageAggregates.find((item) => item.stage === lastStage)?.value ?? null;
  if (typeof firstValue !== "number" || typeof lastValue !== "number" || firstValue <= 0) return null;
  return lastValue / firstValue;
};

const getOfficialMetric = (rows: JourneyStageRow[], fields: string[]) => {
  const points: Array<{ value: number; weight: number }> = [];
  for (const row of rows) {
    const raw = row.raw || {};
    for (const field of fields) {
      if (!Object.hasOwn(raw, field)) continue;
      const valueRaw = numberOrNull(raw[field]);
      if (valueRaw == null) continue;
      // Normalize percentage-point inputs to 0..1 preserving sign.
      // Example: 53.5 -> 0.535, -15.8 -> -0.158.
      const value = Math.abs(valueRaw) > 1 ? valueRaw / 100 : valueRaw;
      points.push({ value, weight: row.weight });
      break;
    }
  }
  return weightedAverage(points);
};

const computeCsnMetrics = (rows: JourneyStageRow[], stageAggregates: JourneyStageAggregate[]) => {
  const officialCsat = getOfficialMetric(rows, OFFICIAL_CSAT_FIELDS);
  const officialNpsRaw = getOfficialMetric(rows, OFFICIAL_NPS_FIELDS);
  const officialPromoters = getOfficialMetric(rows, OFFICIAL_PROMOTERS_FIELDS);
  const officialDetractors = getOfficialMetric(rows, OFFICIAL_DETRACTORS_FIELDS);
  const stageSatisfaction = stageAggregates.find((item) => item.stage === "Brand Satisfaction")?.value ?? null;
  const stageRecommendation = stageAggregates.find((item) => item.stage === "Brand Recommendation")?.value ?? null;

  const csat: JourneyMetricValue =
    officialCsat != null
      ? {
          value: officialCsat,
          meta: {
            metricType: "official",
            explanation: "CSAT = (% values 4-5 - % values 1-2) among purchasers.",
          },
        }
      : {
          value: stageSatisfaction,
          meta: { metricType: "proxy", explanation: "CSAT proxy from Brand Satisfaction stage." },
        };

  let npsValue: number | null = null;
  let npsMeta: JourneyMetricValue["meta"] = {
    metricType: "proxy",
    explanation: "NPS proxy from Brand Recommendation stage.",
  };
  if (officialNpsRaw != null) {
    npsValue = officialNpsRaw;
    npsMeta = { metricType: "official", explanation: "Direct NPS field from source." };
  } else if (officialPromoters != null && officialDetractors != null) {
    npsValue = officialPromoters - officialDetractors;
    npsMeta = { metricType: "official", explanation: "NPS from promoters - detractors." };
  } else {
    npsValue = stageRecommendation;
  }

  const nps: JourneyMetricValue = { value: npsValue, meta: npsMeta };
  return { csat, nps };
};

const computeBenchmark = (
  rows: JourneyStageRow[],
  orderedStages: JourneyStage[],
  conversionStages: JourneyStage[],
  benchmarkScope: JourneyBenchmarkScope
): JourneyBenchmarkAggregate => {
  const scopedRows = rows;
  const { stageAggregates } = computeStageAggregates(scopedRows, orderedStages);
  const links = computeLinkAggregates(scopedRows, conversionStages, stageAggregates);
  const { csat, nps } = computeCsnMetrics(scopedRows, stageAggregates);
  return {
    scope: benchmarkScope,
    stageAggregates,
    links,
    csat,
    nps,
  };
};

const computeGaps = (
  brands: JourneyBrandAggregate[],
  benchmark: JourneyBenchmarkAggregate
): JourneyBrandGap[] => {
  const benchmarkByStage = new Map(benchmark.stageAggregates.map((item) => [item.stage, item.value]));
  return brands.map((brand) => {
    const gaps: JourneyGapByStage[] = brand.stageAggregates.map((stageAgg) => {
      const benchmarkValue = benchmarkByStage.get(stageAgg.stage) ?? null;
      return {
        stage: stageAgg.stage,
        valueGap:
          typeof stageAgg.value === "number" && typeof benchmarkValue === "number"
            ? stageAgg.value - benchmarkValue
            : null,
      };
    });
    return { key: brand.key, brandName: brand.brandName, gaps };
  });
};

const rankBrandsByStageValue = (
  brands: JourneyBrandAggregate[],
  stage: JourneyStage
): JourneyStageRank[] => {
  const ordered = brands
    .map((brand) => ({
      key: brand.key,
      brandName: brand.brandName,
      value: brand.stageAggregates.find((item) => item.stage === stage)?.value ?? null,
    }))
    .filter((item): item is { key: string; brandName: string; value: number } => typeof item.value === "number")
    .sort((a, b) => b.value - a.value);
  return ordered.map((item, index) => ({
    key: item.key,
    brandName: item.brandName,
    rank: index + 1,
    value: item.value,
  }));
};

const coverageSummary = (
  brands: JourneyBrandAggregate[],
  orderedStages: JourneyStage[]
): JourneyCoverageSummary => {
  const stageMap = new Map<JourneyStage, { studies: number; weight: number }>();
  const linkMap = new Map<string, { fromStage: JourneyStage; toStage: JourneyStage; studies: number; weight: number }>();
  for (const stage of orderedStages) stageMap.set(stage, { studies: 0, weight: 0 });
  for (const brand of brands) {
    for (const stage of brand.stageAggregates) {
      const bucket = stageMap.get(stage.stage);
      if (!bucket) continue;
      bucket.studies += stage.stageCoverageStudies;
      bucket.weight += stage.stageCoverageWeight;
    }
    for (const link of brand.links) {
      const key = pairKey(link.fromStage, link.toStage);
      if (!linkMap.has(key)) {
        linkMap.set(key, {
          fromStage: link.fromStage,
          toStage: link.toStage,
          studies: 0,
          weight: 0,
        });
      }
      const bucket = linkMap.get(key)!;
      bucket.studies += link.linkCoverageStudies;
      bucket.weight += link.linkCoverageWeight;
    }
  }
  return {
    byStage: orderedStages.map((stage) => ({
      stage,
      studies: stageMap.get(stage)?.studies || 0,
      weight: stageMap.get(stage)?.weight || 0,
    })),
    byLink: Array.from(linkMap.values()),
  };
};

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

const confidenceFromCoverage = (
  validStages: number,
  validLinks: number,
  stageCount: number,
  studiesCovered: number
): JourneyIndexConfidence => {
  if (validStages >= Math.min(4, stageCount) && validLinks >= 3 && studiesCovered >= 4) return "high";
  if (validStages >= Math.min(3, stageCount) && validLinks >= 2 && studiesCovered >= 2) return "med";
  return "low";
};

export function computeRetentionScore(brand: JourneyBrandAggregate): {
  value100: number | null;
  validLinks: number;
  studiesCovered: number;
} {
  const points = brand.links
    .filter(
      (link) =>
        isCoreConversionLink(link.fromStage, link.toStage) && typeof link.conversionForIndex === "number"
    )
    .map((link) => ({ value: link.conversionForIndex as number, weight: Math.max(1, link.linkCoverageWeight) }));
  const score = weightedAverage(points);
  const validLinks = points.length;
  const studiesCovered = brand.links.reduce((max, link) => Math.max(max, link.linkCoverageStudies), 0);
  return { value100: score == null ? null : clamp(score, 0, 1) * 100, validLinks, studiesCovered };
}

export function computeGapScore(
  brand: JourneyBrandAggregate,
  benchmark: JourneyBenchmarkAggregate,
  clampRange = GAP_CLAMP
): { value100: number | null; validStages: number } {
  const benchmarkByStage = new Map(benchmark.stageAggregates.map((item) => [item.stage, item.value]));
  const points: Array<{ value: number; weight: number }> = [];
  for (const stage of brand.stageAggregates) {
    const bench = benchmarkByStage.get(stage.stage);
    if (typeof stage.value !== "number" || typeof bench !== "number") continue;
    const normalized = clamp((stage.value - bench) / clampRange, -1, 1);
    points.push({ value: normalized, weight: Math.max(1, stage.stageCoverageWeight) });
  }
  const gap = weightedAverage(points);
  return { value100: gap == null ? null : ((gap + 1) / 2) * 100, validStages: points.length };
}

export function computeNpsScore(brand: JourneyBrandAggregate): number | null {
  const nps = brand.nps.value;
  if (typeof nps !== "number") return null;
  if (brand.nps.meta.metricType === "official") {
    return clamp(((nps * 100 + 100) / 2), 0, 100);
  }
  return clamp(nps * 100, 0, 100);
}

export function computeCsatScore(brand: JourneyBrandAggregate): number | null {
  const csat = brand.csat.value;
  if (typeof csat !== "number") return null;
  return clamp(csat * 100, 0, 100);
}

export function computeJourneyIndex(components: {
  retentionScore100: number | null;
  gapScore100: number | null;
  csatScore100: number | null;
  npsScore100: number | null;
}): {
  value: number | null;
  weightsApplied: { retention: number; gap: number; csat: number; nps: number };
  partial: boolean;
} {
  const entries = [
    { key: "retention" as const, value: components.retentionScore100, weight: JOURNEY_INDEX_WEIGHTS.retention },
    { key: "gap" as const, value: components.gapScore100, weight: JOURNEY_INDEX_WEIGHTS.gap },
    { key: "csat" as const, value: components.csatScore100, weight: JOURNEY_INDEX_WEIGHTS.csat },
    { key: "nps" as const, value: components.npsScore100, weight: JOURNEY_INDEX_WEIGHTS.nps },
  ].filter((item) => typeof item.value === "number");

  if (!entries.length) {
    return {
      value: null,
      weightsApplied: { retention: 0, gap: 0, csat: 0, nps: 0 },
      partial: true,
    };
  }

  const weightTotal = entries.reduce((sum, item) => sum + item.weight, 0);
  const normalized = entries.map((item) => ({ ...item, weight: item.weight / weightTotal }));
  const indexValue = normalized.reduce((sum, item) => sum + (item.value as number) * item.weight, 0);
  return {
    value: clamp(indexValue, 0, 100),
    weightsApplied: {
      retention: normalized.find((item) => item.key === "retention")?.weight || 0,
      gap: normalized.find((item) => item.key === "gap")?.weight || 0,
      csat: normalized.find((item) => item.key === "csat")?.weight || 0,
      nps: normalized.find((item) => item.key === "nps")?.weight || 0,
    },
    partial: entries.length < 3,
  };
}

export function computeFunnelHealth(
  brand: JourneyBrandAggregate,
  benchmark: JourneyBenchmarkAggregate
): FunnelHealthEntry {
  const benchmarkLinkMap = new Map(
    benchmark.links.map((item) => [`${item.fromStage}->${item.toStage}`, item.dropAbs])
  );
  const validDrops = brand.links
    .filter(
      (link) => isCoreConversionLink(link.fromStage, link.toStage) && typeof link.dropAbs === "number"
    )
    .map((link) => ({ ...link, dropPts: (link.dropAbs as number) * 100 }));

  if (!validDrops.length) {
    return {
      status: "unknown",
      maxDropPts: null,
      link: null,
      benchMaxDropPts: null,
      confidence: "low",
      studiesCovered: 0,
    };
  }

  const maxDrop = validDrops.reduce((worst, current) => (current.dropPts > worst.dropPts ? current : worst), validDrops[0]);
  const benchDrop = benchmarkLinkMap.get(`${maxDrop.fromStage}->${maxDrop.toStage}`);
  const benchMaxDropPts = typeof benchDrop === "number" ? benchDrop * 100 : null;

  let status: FunnelHealthEntry["status"] = "healthy";
  if (maxDrop.dropPts >= FUNNEL_HEALTH_THRESHOLDS.moderate) status = "critical";
  else if (maxDrop.dropPts >= FUNNEL_HEALTH_THRESHOLDS.healthy) status = "moderate";

  const validStages = brand.stageAggregates.filter((stage) => typeof stage.value === "number").length;
  const confidence = confidenceFromCoverage(
    validStages,
    validDrops.length,
    brand.stageAggregates.length,
    maxDrop.linkCoverageStudies
  );

  return {
    status,
    maxDropPts: maxDrop.dropPts,
    link: { fromStage: maxDrop.fromStage, toStage: maxDrop.toStage },
    benchMaxDropPts,
    confidence,
    studiesCovered: maxDrop.linkCoverageStudies,
  };
}

export function buildJourneyModel(
  rawJourneyResults: unknown,
  _filters: Record<string, unknown> | null,
  options?: BuildJourneyOptions
): JourneyModel {
  const merged = {
    includeAdAwareness: options?.includeAdAwareness ?? DEFAULT_BUILD_JOURNEY_OPTIONS.includeAdAwareness ?? true,
    benchmarkScope: options?.benchmarkScope ?? DEFAULT_BUILD_JOURNEY_OPTIONS.benchmarkScope ?? "category",
    benchmarkRows: options?.benchmarkRows,
  };
  const rowsAll = normalizeJourneyResults(rawJourneyResults);
  const benchmarkRowsAll = options?.benchmarkRows ? normalizeJourneyResults(options.benchmarkRows) : rowsAll;
  const stagesOrdered = toOrderedStages(merged.includeAdAwareness);
  const conversionStages = toCoreConversionStages(stagesOrdered);
  const rows = rowsAll.filter((row) => stagesOrdered.includes(row.stage));
  const benchmarkRowsSource = benchmarkRowsAll.filter((row) => stagesOrdered.includes(row.stage));

  const grouped = new Map<string, JourneyStageRow[]>();
  for (const row of rows) {
    const key = groupKey(row);
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key)!.push(row);
  }

  const brandStageAggregates: JourneyBrandAggregate[] = [];
  const warnings: string[] = [];
  for (const [key, groupRows] of grouped) {
    const first = groupRows[0];
    const { stageAggregates } = computeStageAggregates(groupRows, stagesOrdered);
    const links = computeLinkAggregates(groupRows, conversionStages, stageAggregates);
    const totalConversion = computeTotalConversion(stageAggregates, conversionStages);
    const { csat, nps } = computeCsnMetrics(groupRows, stageAggregates);
    brandStageAggregates.push({
      key,
      brandId: first.brandId,
      brandName: first.brandName,
      dims: first.dims,
      stageAggregates,
      links,
      totalConversion,
      csat,
      nps,
    });
  }

  const benchmarkRows: JourneyStageRow[] = [];
  if (merged.benchmarkScope === "category") {
    const keys = new Set(benchmarkRowsSource.map((row) => benchmarkKey(row, "category")));
    for (const key of keys) {
      const scoped = benchmarkRowsSource.filter((row) => benchmarkKey(row, "category") === key);
      benchmarkRows.push(...scoped);
    }
  } else {
    benchmarkRows.push(...benchmarkRowsSource);
  }

  const benchmarkStageAggregates = computeBenchmark(
    benchmarkRows,
    stagesOrdered,
    conversionStages,
    merged.benchmarkScope
  );
  const stageGaps = computeGaps(brandStageAggregates, benchmarkStageAggregates);

  const ranksByStage = {} as Record<JourneyStage, JourneyStageRank[]>;
  for (const stage of stagesOrdered) {
    ranksByStage[stage] = rankBrandsByStageValue(brandStageAggregates, stage);
  }

  const links = benchmarkStageAggregates.links;
  const coverage = coverageSummary(brandStageAggregates, stagesOrdered);
  const journeyIndexByBrand: Record<string, JourneyIndexEntry> = {};
  const funnelHealthByBrand: Record<string, FunnelHealthEntry> = {};
  const indexExclusions: JourneyModel["metadata"]["indexExclusions"] = [];

  const benchmarkRetention = {
    value100: weightedAverage(
      benchmarkStageAggregates.links
        .filter(
          (link) =>
            isCoreConversionLink(link.fromStage, link.toStage) && typeof link.conversionForIndex === "number"
        )
        .map((link) => ({ value: link.conversionForIndex as number, weight: Math.max(1, link.linkCoverageWeight) }))
    ),
  };
  const benchmarkGap = { value100: 50 };
  const benchmarkCsat = (() => {
    const value = benchmarkStageAggregates.csat.value;
    if (typeof value !== "number") return null;
    return clamp(value * 100, 0, 100);
  })();
  const benchmarkNps = (() => {
    const value = benchmarkStageAggregates.nps.value;
    if (typeof value !== "number") return null;
    if (benchmarkStageAggregates.nps.meta.metricType === "official") return clamp(((value * 100 + 100) / 2), 0, 100);
    return clamp(value * 100, 0, 100);
  })();
  const benchmarkIndexComputation = computeJourneyIndex({
    retentionScore100: benchmarkRetention.value100 == null ? null : clamp(benchmarkRetention.value100, 0, 1) * 100,
    gapScore100: benchmarkGap.value100,
    csatScore100: benchmarkCsat,
    npsScore100: benchmarkNps,
  });
  const benchmarkFunnelHealth = computeFunnelHealth(
    {
      key: "__benchmark__",
      brandName: "Benchmark",
      stageAggregates: benchmarkStageAggregates.stageAggregates,
      links: benchmarkStageAggregates.links,
      totalConversion: null,
      dims: {},
      csat: benchmarkStageAggregates.csat,
      nps: benchmarkStageAggregates.nps,
    },
    benchmarkStageAggregates
  );

  for (const brand of brandStageAggregates) {
    const retention = computeRetentionScore(brand);
    const gap = computeGapScore(brand, benchmarkStageAggregates);
    const csatScore = computeCsatScore(brand);
    const npsScore = computeNpsScore(brand);
    const index = computeJourneyIndex({
      retentionScore100: retention.value100,
      gapScore100: gap.value100,
      csatScore100: csatScore,
      npsScore100: npsScore,
    });
    const validStages = brand.stageAggregates.filter((stage) => typeof stage.value === "number").length;
    const confidence = confidenceFromCoverage(
      validStages,
      retention.validLinks,
      stagesOrdered.length,
      retention.studiesCovered
    );
    const deltaVsBenchmark =
      typeof index.value === "number" && typeof benchmarkIndexComputation.value === "number"
        ? index.value - benchmarkIndexComputation.value
        : null;
    journeyIndexByBrand[brand.key] = {
      value: index.value == null ? null : Number(index.value.toFixed(1)),
      rank: null,
      deltaVsBenchmark: deltaVsBenchmark == null ? null : Number(deltaVsBenchmark.toFixed(1)),
      confidence,
      validStages,
      validLinks: retention.validLinks,
      studiesCovered: retention.studiesCovered,
      components: {
        retentionScore100: retention.value100 == null ? null : Number(retention.value100.toFixed(1)),
        gapScore100: gap.value100 == null ? null : Number(gap.value100.toFixed(1)),
        csatScore100: csatScore == null ? null : Number(csatScore.toFixed(1)),
        npsScore100: npsScore == null ? null : Number(npsScore.toFixed(1)),
        weightsApplied: index.weightsApplied,
        partial: index.partial,
      },
    };
    for (const link of brand.links) {
      if (!link.excludedFromIndex) continue;
      indexExclusions.push({
        brandKey: brand.key,
        brandName: brand.brandName,
        fromStage: link.fromStage,
        toStage: link.toStage,
        reason: "Conversion anomaly (>100% or non-monotonic stage relation).",
        anomalyStudies: link.anomalyStudies,
      });
    }
    funnelHealthByBrand[brand.key] = computeFunnelHealth(brand, benchmarkStageAggregates);
  }

  const rankedJourneyIndex = Object.entries(journeyIndexByBrand)
    .filter(([, entry]) => typeof entry.value === "number")
    .sort((a, b) => (b[1].value as number) - (a[1].value as number));
  rankedJourneyIndex.forEach(([key], index) => {
    journeyIndexByBrand[key].rank = index + 1;
  });

  const benchmarkJourneyIndex: JourneyIndexEntry = {
    value: benchmarkIndexComputation.value == null ? null : Number(benchmarkIndexComputation.value.toFixed(1)),
    rank: 1,
    deltaVsBenchmark: 0,
    confidence: confidenceFromCoverage(
      benchmarkStageAggregates.stageAggregates.filter((stage) => typeof stage.value === "number").length,
      benchmarkStageAggregates.links.filter(
        (link) =>
          isCoreConversionLink(link.fromStage, link.toStage) && typeof link.conversionForIndex === "number"
      ).length,
      stagesOrdered.length,
      Math.max(...benchmarkStageAggregates.links.map((link) => link.linkCoverageStudies), 0)
    ),
    validStages: benchmarkStageAggregates.stageAggregates.filter((stage) => typeof stage.value === "number").length,
    validLinks: benchmarkStageAggregates.links.filter(
      (link) =>
        isCoreConversionLink(link.fromStage, link.toStage) && typeof link.conversionForIndex === "number"
    ).length,
    studiesCovered: Math.max(...benchmarkStageAggregates.links.map((link) => link.linkCoverageStudies), 0),
    components: {
      retentionScore100:
        benchmarkRetention.value100 == null ? null : Number((clamp(benchmarkRetention.value100, 0, 1) * 100).toFixed(1)),
      gapScore100: benchmarkGap.value100,
      csatScore100: benchmarkCsat == null ? null : Number(benchmarkCsat.toFixed(1)),
      npsScore100: benchmarkNps == null ? null : Number(benchmarkNps.toFixed(1)),
      weightsApplied: benchmarkIndexComputation.weightsApplied,
      partial: benchmarkIndexComputation.partial,
    },
  };

  for (const stage of stagesOrdered) {
    const coverageEntry = coverage.byStage.find((item) => item.stage === stage);
    if (!coverageEntry) continue;
    if (coverageEntry.studies === 0) {
      warnings.push(`${stage} missing in 100% of studies for the current selection.`);
    }
  }
  if (indexExclusions.length) {
    warnings.push(
      `${indexExclusions.length} conversion segments were excluded from Journey Index due to data-quality anomalies.`
    );
  }

  return {
    stagesOrdered,
    rows,
    brandStageAggregates,
    benchmarkStageAggregates,
    stageGaps,
    links,
    ranksByStage,
    journeyIndexByBrand,
    benchmarkJourneyIndex,
    funnelHealthByBrand,
    benchmarkFunnelHealth,
    metadata: {
      includeAdAwareness: merged.includeAdAwareness,
      warnings,
      coverage,
      indexExclusions,
    },
  };
}
