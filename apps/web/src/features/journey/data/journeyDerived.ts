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
  type JourneyLinkAggregate,
  type JourneyMetricValue,
  type JourneyModel,
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

const stageKey = (stage: JourneyStage) => stage;
const pairKey = (from: JourneyStage, to: JourneyStage) => `${from} -> ${to}`;

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
  const stageAggregates = orderedStages.map((stage) => ({
    stage,
    value: weightedAverage((byStage.get(stage) || []).map((point) => ({ value: point.value, weight: point.weight }))),
    stageCoverageStudies: coverage.get(stage)?.studies.size || 0,
    stageCoverageWeight: coverage.get(stage)?.weight || 0,
  }));
  return { stageAggregates, byStageCoverage: coverage };
};

const computeLinkAggregates = (rows: JourneyStageRow[], orderedStages: JourneyStage[]): JourneyLinkAggregate[] => {
  const byStudy = new Map<string, Map<JourneyStage, { value: number; weight: number }>>();
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
    const pointsDrop: Array<{ value: number; weight: number }> = [];
    const pointsConv: Array<{ value: number; weight: number }> = [];
    const studies = new Set<string>();
    let weightTotal = 0;

    for (const [studyId, stages] of byStudy) {
      const from = stages.get(fromStage);
      const to = stages.get(toStage);
      if (!from || !to) continue;
      const weight = Math.min(from.weight, to.weight);
      if (weight <= 0) continue;
      pointsDrop.push({ value: from.value - to.value, weight });
      if (from.value > 0) pointsConv.push({ value: to.value / from.value, weight });
      studies.add(studyId);
      weightTotal += weight;
    }

    links.push({
      fromStage,
      toStage,
      dropAbs: weightedAverage(pointsDrop),
      conversion: weightedAverage(pointsConv),
      linkCoverageStudies: studies.size,
      linkCoverageWeight: weightTotal,
    });
  }
  return links;
};

const computeTotalConversion = (rows: JourneyStageRow[], orderedStages: JourneyStage[]) => {
  const firstStage = orderedStages[0];
  const lastStage = orderedStages[orderedStages.length - 1];
  const byStudy = new Map<string, { first?: JourneyStageRow; last?: JourneyStageRow }>();
  for (const row of rows) {
    if (!byStudy.has(row.studyId)) byStudy.set(row.studyId, {});
    const bucket = byStudy.get(row.studyId)!;
    if (row.stage === firstStage) bucket.first = row;
    if (row.stage === lastStage) bucket.last = row;
  }
  const points: Array<{ value: number; weight: number }> = [];
  for (const pair of byStudy.values()) {
    if (!pair.first || !pair.last) continue;
    if (pair.first.value <= 0) continue;
    const weight = Math.min(pair.first.weight, pair.last.weight);
    points.push({ value: pair.last.value / pair.first.value, weight });
  }
  return weightedAverage(points);
};

const getOfficialMetric = (rows: JourneyStageRow[], fields: string[]) => {
  const points: Array<{ value: number; weight: number }> = [];
  for (const row of rows) {
    const raw = row.raw || {};
    for (const field of fields) {
      if (!Object.hasOwn(raw, field)) continue;
      const valueRaw = numberOrNull(raw[field]);
      if (valueRaw == null) continue;
      const value = valueRaw > 1 ? valueRaw / 100 : valueRaw;
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
      ? { value: officialCsat, meta: { metricType: "official", explanation: "Direct CSAT field from source." } }
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
  benchmarkScope: JourneyBenchmarkScope
): JourneyBenchmarkAggregate => {
  const scopedRows = rows;
  const { stageAggregates } = computeStageAggregates(scopedRows, orderedStages);
  const links = computeLinkAggregates(scopedRows, orderedStages);
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

export function buildJourneyModel(
  rawJourneyResults: unknown,
  _filters: Record<string, unknown> | null,
  options?: BuildJourneyOptions
): JourneyModel {
  const merged = { ...DEFAULT_BUILD_JOURNEY_OPTIONS, ...(options || {}) };
  const rowsAll = normalizeJourneyResults(rawJourneyResults);
  const stagesOrdered = toOrderedStages(merged.includeAdAwareness);
  const rows = rowsAll.filter((row) => stagesOrdered.includes(row.stage));

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
    const links = computeLinkAggregates(groupRows, stagesOrdered);
    const totalConversion = computeTotalConversion(groupRows, stagesOrdered);
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
    const keys = new Set(rows.map((row) => benchmarkKey(row, "category")));
    for (const key of keys) {
      const scoped = rows.filter((row) => benchmarkKey(row, "category") === key);
      benchmarkRows.push(...scoped);
    }
  } else {
    benchmarkRows.push(...rows);
  }

  const benchmarkStageAggregates = computeBenchmark(benchmarkRows, stagesOrdered, merged.benchmarkScope);
  const stageGaps = computeGaps(brandStageAggregates, benchmarkStageAggregates);

  const ranksByStage = {} as Record<JourneyStage, JourneyStageRank[]>;
  for (const stage of stagesOrdered) {
    ranksByStage[stage] = rankBrandsByStageValue(brandStageAggregates, stage);
  }

  const links = benchmarkStageAggregates.links;
  const coverage = coverageSummary(brandStageAggregates, stagesOrdered);

  for (const stage of stagesOrdered) {
    const coverageEntry = coverage.byStage.find((item) => item.stage === stage);
    if (!coverageEntry) continue;
    if (coverageEntry.studies === 0) {
      warnings.push(`${stage} missing in 100% of studies for the current selection.`);
    }
  }

  return {
    stagesOrdered,
    rows,
    brandStageAggregates,
    benchmarkStageAggregates,
    stageGaps,
    links,
    ranksByStage,
    metadata: {
      includeAdAwareness: merged.includeAdAwareness,
      warnings,
      coverage,
    },
  };
}

