import type {
  JourneyBenchmarkAggregate,
  JourneyBrandAggregate,
  JourneyIndexEntry,
  JourneyModel,
  JourneyStage,
} from "../data/journeySchema";

export type HeatmapColumn =
  | {
      key: string;
      label: string;
      group: "stage";
      stage: JourneyStage;
      clamp: number;
    }
  | {
      key: string;
      label: string;
      group: "conversion";
      fromStage: JourneyStage;
      toStage: JourneyStage;
      clamp: number;
    }
  | {
      key: "csat" | "nps" | "journey_index";
      label: string;
      group: "metric";
      clamp: number;
    };

export type HeatmapCell = {
  value: number | null;
  benchmarkValue: number | null;
  delta: number | null;
  coverageStudies: number;
  totalStudies: number;
  coverageSample: number;
  missing: boolean;
  anomalyFlag?: boolean;
  excludedFromIndex?: boolean;
};

export type HeatmapRow = {
  key: string;
  brandName: string;
  isBenchmark: boolean;
  cells: Record<string, HeatmapCell>;
};

export type JourneyHeatmapMatrix = {
  columns: HeatmapColumn[];
  rows: HeatmapRow[];
  meta: { includeAdAwareness: boolean };
};

type BuildJourneyHeatmapOptions = {
  benchmarkMode?: "selection" | "global";
  selectionBenchmark?: {
    aggregate: JourneyBenchmarkAggregate;
    journeyIndex: JourneyIndexEntry;
  };
  globalBenchmark?: {
    aggregate: JourneyBenchmarkAggregate;
    journeyIndex: JourneyIndexEntry;
  };
  selectionBenchmarkSampleN?: number;
  globalBenchmarkSampleN?: number;
};

const stageVal = (brand: JourneyBrandAggregate, stage: JourneyStage) =>
  brand.stageAggregates.find((item) => item.stage === stage) ?? null;

const convVal = (brand: JourneyBrandAggregate, fromStage: JourneyStage, toStage: JourneyStage) =>
  brand.links.find((item) => item.fromStage === fromStage && item.toStage === toStage) ?? null;
const isAdAwarenessStage = (stage: JourneyStage) => stage === "Ad Awareness";

const stageLabel = (stage: JourneyStage) => {
  if (stage === "Brand Awareness") return "Brand Awareness";
  if (stage === "Ad Awareness") return "Ad Awareness";
  if (stage === "Brand Consideration") return "Brand Consideration";
  if (stage === "Brand Purchase") return "Brand Purchase";
  if (stage === "Brand Satisfaction") return "Brand Satisfaction";
  return "Brand Recommendation";
};

export function buildJourneyHeatmap(
  model: JourneyModel,
  selectedBrands: string[],
  maxBrands = 10,
  options?: BuildJourneyHeatmapOptions
): JourneyHeatmapMatrix {
  const totalStudies = new Set(model.rows.map((row) => row.studyId)).size;
  const studySampleById = new Map<string, number>();
  const brandSampleByName = new Map<string, number>();
  for (const row of model.rows) {
    const sample = row.basePopulationN ?? row.baseN ?? null;
    if (typeof sample !== "number" || sample <= 0) continue;
    const studyPrev = studySampleById.get(row.studyId) ?? 0;
    if (sample > studyPrev) studySampleById.set(row.studyId, sample);
    const brandPrev = brandSampleByName.get(row.brandName) ?? 0;
    if (sample > brandPrev) brandSampleByName.set(row.brandName, sample);
  }
  const selectionSampleN = Array.from(studySampleById.values()).reduce((sum, n) => sum + n, 0);
  const selected = model.brandStageAggregates.filter(
    (item) => selectedBrands.length === 0 || selectedBrands.includes(item.brandName)
  );
  const getJourneyIndex = (brand: JourneyBrandAggregate) => {
    const value = model.journeyIndexByBrand[brand.key]?.value;
    return typeof value === "number" ? value : Number.NEGATIVE_INFINITY;
  };
  const sorted = selected
    .slice()
    .sort(
      (a, b) =>
        getJourneyIndex(b) - getJourneyIndex(a) ||
        (b.totalConversion ?? 0) - (a.totalConversion ?? 0) ||
        a.brandName.localeCompare(b.brandName)
    );
  const visible = sorted.slice(0, maxBrands);

  const columns: HeatmapColumn[] = [];
  for (const stage of model.stagesOrdered) {
    columns.push({
      key: `stage:${stage}`,
      label: stageLabel(stage),
      group: "stage",
      stage,
      clamp: 0.15,
    });
  }
  const conversionStages = model.stagesOrdered.filter((stage) => !isAdAwarenessStage(stage));
  for (let i = 0; i < conversionStages.length - 1; i += 1) {
    const fromStage = conversionStages[i];
    const toStage = conversionStages[i + 1];
    columns.push({
      key: `conv:${fromStage}->${toStage}`,
      label: `${stageLabel(fromStage)} -> ${stageLabel(toStage)}`,
      group: "conversion",
      fromStage,
      toStage,
      clamp: 0.1,
    });
  }
  columns.push({ key: "csat", label: "CSAT", group: "metric", clamp: 0.15 });
  columns.push({ key: "nps", label: "NPS", group: "metric", clamp: 0.15 });
  columns.push({ key: "journey_index", label: "Journey Index", group: "metric", clamp: 20 });

  const selectionBenchmark = options?.selectionBenchmark ?? {
    aggregate: model.benchmarkStageAggregates,
    journeyIndex: model.benchmarkJourneyIndex,
  };
  const globalBenchmark = options?.globalBenchmark ?? selectionBenchmark;
  const selectionBenchmarkSampleN = options?.selectionBenchmarkSampleN ?? selectionSampleN;
  const globalBenchmarkSampleN = options?.globalBenchmarkSampleN ?? selectionBenchmarkSampleN;
  const activeBenchmark = (options?.benchmarkMode ?? "selection") === "global" ? globalBenchmark : selectionBenchmark;

  const activeBenchmarkStageMap = new Map(
    activeBenchmark.aggregate.stageAggregates.map((item) => [item.stage, item])
  );
  const activeBenchmarkLinkMap = new Map(
    activeBenchmark.aggregate.links.map((item) => [`${item.fromStage}->${item.toStage}`, item])
  );

  const createBenchmarkCells = (
    aggregate: JourneyBenchmarkAggregate,
    journeyIndex: JourneyIndexEntry,
    benchmarkSampleN: number
  ): Record<string, HeatmapCell> => {
    const stageMap = new Map(aggregate.stageAggregates.map((item) => [item.stage, item]));
    const linkMap = new Map(aggregate.links.map((item) => [`${item.fromStage}->${item.toStage}`, item]));
    const cells: Record<string, HeatmapCell> = {};
    for (const col of columns) {
      if (col.group === "stage") {
        const stage = stageMap.get(col.stage);
        cells[col.key] = {
          value: stage?.value ?? null,
          benchmarkValue: stage?.value ?? null,
          delta: 0,
          coverageStudies: stage?.stageCoverageStudies ?? 0,
          totalStudies,
          coverageSample: benchmarkSampleN,
          missing: stage?.value == null,
          anomalyFlag: false,
          excludedFromIndex: false,
        };
        continue;
      }
      if (col.group === "conversion") {
        const link = linkMap.get(`${col.fromStage}->${col.toStage}`);
        cells[col.key] = {
          value: link?.conversion ?? null,
          benchmarkValue: link?.conversion ?? null,
          delta: 0,
          coverageStudies: link?.linkCoverageStudies ?? 0,
          totalStudies,
          coverageSample: benchmarkSampleN,
          missing: link?.conversion == null,
          anomalyFlag: link?.anomalyFlag ?? false,
          excludedFromIndex: link?.excludedFromIndex ?? false,
        };
        continue;
      }
      if (col.key === "journey_index") {
        cells[col.key] = {
          value: typeof journeyIndex.value === "number" ? journeyIndex.value / 100 : null,
          benchmarkValue: typeof journeyIndex.value === "number" ? journeyIndex.value / 100 : null,
          delta: 0,
          coverageStudies: journeyIndex.studiesCovered,
          totalStudies,
          coverageSample: benchmarkSampleN,
          missing: journeyIndex.value == null,
          anomalyFlag: false,
          excludedFromIndex: false,
        };
      } else if (col.key === "csat") {
        cells[col.key] = {
          value: aggregate.csat.value,
          benchmarkValue: aggregate.csat.value,
          delta: 0,
          coverageStudies: stageMap.get("Brand Satisfaction")?.stageCoverageStudies ?? 0,
          totalStudies,
          coverageSample: benchmarkSampleN,
          missing: aggregate.csat.value == null,
          anomalyFlag: false,
          excludedFromIndex: false,
        };
      } else {
        cells[col.key] = {
          value: aggregate.nps.value,
          benchmarkValue: aggregate.nps.value,
          delta: 0,
          coverageStudies: stageMap.get("Brand Recommendation")?.stageCoverageStudies ?? 0,
          totalStudies,
          coverageSample: benchmarkSampleN,
          missing: aggregate.nps.value == null,
          anomalyFlag: false,
          excludedFromIndex: false,
        };
      }
    }
    return cells;
  };

  const rows: HeatmapRow[] = [
    {
      key: "benchmark-global",
      brandName: "Global Benchmark",
      isBenchmark: true,
      cells: createBenchmarkCells(globalBenchmark.aggregate, globalBenchmark.journeyIndex, globalBenchmarkSampleN),
    },
    {
      key: "benchmark-selection",
      brandName: "Selection Benchmark",
      isBenchmark: true,
      cells: createBenchmarkCells(
        selectionBenchmark.aggregate,
        selectionBenchmark.journeyIndex,
        selectionBenchmarkSampleN
      ),
    },
  ];

  for (const brand of visible) {
    const cells: Record<string, HeatmapCell> = {};
    for (const col of columns) {
      if (col.group === "stage") {
        const val = stageVal(brand, col.stage);
        const b = activeBenchmarkStageMap.get(col.stage);
        const value = val?.value ?? null;
        const benchValue = b?.value ?? null;
        cells[col.key] = {
          value,
          benchmarkValue: benchValue,
          delta: typeof value === "number" && typeof benchValue === "number" ? value - benchValue : null,
          coverageStudies: val?.stageCoverageStudies ?? 0,
          totalStudies,
          coverageSample: brandSampleByName.get(brand.brandName) ?? 0,
          missing: value == null,
          anomalyFlag: false,
          excludedFromIndex: false,
        };
        continue;
      }
      if (col.group === "conversion") {
        const val = convVal(brand, col.fromStage, col.toStage);
        const b = activeBenchmarkLinkMap.get(`${col.fromStage}->${col.toStage}`);
        const value = val?.conversion ?? null;
        const benchValue = b?.conversion ?? null;
        cells[col.key] = {
          value,
          benchmarkValue: benchValue,
          delta: typeof value === "number" && typeof benchValue === "number" ? value - benchValue : null,
          coverageStudies: val?.linkCoverageStudies ?? 0,
          totalStudies,
          coverageSample: brandSampleByName.get(brand.brandName) ?? 0,
          missing: value == null,
          anomalyFlag: val?.anomalyFlag ?? false,
          excludedFromIndex: val?.excludedFromIndex ?? false,
        };
        continue;
      }
      if (col.key === "journey_index") {
        const indexEntry = model.journeyIndexByBrand[brand.key];
        const benchmarkIndex = activeBenchmark.journeyIndex.value;
        const value = typeof indexEntry?.value === "number" ? indexEntry.value / 100 : null;
        const benchValue = typeof benchmarkIndex === "number" ? benchmarkIndex / 100 : null;
        cells[col.key] = {
          value,
          benchmarkValue: benchValue,
          delta: typeof value === "number" && typeof benchValue === "number" ? value - benchValue : null,
          coverageStudies: indexEntry?.studiesCovered ?? 0,
          totalStudies,
          coverageSample: brandSampleByName.get(brand.brandName) ?? 0,
          missing: value == null,
          anomalyFlag: false,
          excludedFromIndex: false,
        };
      } else if (col.key === "csat") {
        const value = brand.csat.value;
        const benchValue = activeBenchmark.aggregate.csat.value;
        cells[col.key] = {
          value,
          benchmarkValue: benchValue,
          delta: typeof value === "number" && typeof benchValue === "number" ? value - benchValue : null,
          coverageStudies: stageVal(brand, "Brand Satisfaction")?.stageCoverageStudies ?? 0,
          totalStudies,
          coverageSample: brandSampleByName.get(brand.brandName) ?? 0,
          missing: value == null,
          anomalyFlag: false,
          excludedFromIndex: false,
        };
      } else {
        const value = brand.nps.value;
        const benchValue = activeBenchmark.aggregate.nps.value;
        cells[col.key] = {
          value,
          benchmarkValue: benchValue,
          delta: typeof value === "number" && typeof benchValue === "number" ? value - benchValue : null,
          coverageStudies: stageVal(brand, "Brand Recommendation")?.stageCoverageStudies ?? 0,
          totalStudies,
          coverageSample: brandSampleByName.get(brand.brandName) ?? 0,
          missing: value == null,
          anomalyFlag: false,
          excludedFromIndex: false,
        };
      }
    }
    rows.push({
      key: brand.key,
      brandName: brand.brandName,
      isBenchmark: false,
      cells,
    });
  }

  return {
    columns,
    rows,
    meta: { includeAdAwareness: model.metadata.includeAdAwareness },
  };
}
