import type { JourneyBrandAggregate, JourneyModel, JourneyStage } from "../data/journeySchema";

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
      key: "csat" | "nps";
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
  missing: boolean;
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

const stageVal = (brand: JourneyBrandAggregate, stage: JourneyStage) =>
  brand.stageAggregates.find((item) => item.stage === stage) ?? null;

const convVal = (brand: JourneyBrandAggregate, fromStage: JourneyStage, toStage: JourneyStage) =>
  brand.links.find((item) => item.fromStage === fromStage && item.toStage === toStage) ?? null;

export function buildJourneyHeatmap(
  model: JourneyModel,
  selectedBrands: string[],
  maxBrands = 10
): JourneyHeatmapMatrix {
  const totalStudies = new Set(model.rows.map((row) => row.studyId)).size;
  const selected = model.brandStageAggregates.filter(
    (item) => selectedBrands.length === 0 || selectedBrands.includes(item.brandName)
  );
  const sorted = selected
    .slice()
    .sort(
      (a, b) =>
        (stageVal(b, model.stagesOrdered[0])?.value ?? 0) - (stageVal(a, model.stagesOrdered[0])?.value ?? 0)
    );
  const visible = sorted.slice(0, maxBrands);

  const columns: HeatmapColumn[] = [];
  for (const stage of model.stagesOrdered) {
    columns.push({
      key: `stage:${stage}`,
      label: stage
        .replace("Brand ", "")
        .replace("Ad Awareness", "Ad Awr")
        .replace("Consideration", "Consid")
        .replace("Satisfaction", "Sat")
        .replace("Recommendation", "Rec"),
      group: "stage",
      stage,
      clamp: 0.15,
    });
  }
  for (let i = 0; i < model.stagesOrdered.length - 1; i += 1) {
    const fromStage = model.stagesOrdered[i];
    const toStage = model.stagesOrdered[i + 1];
    columns.push({
      key: `conv:${fromStage}->${toStage}`,
      label: `${fromStage.replace("Brand ", "").replace("Ad Awareness", "Ad")}->${toStage.replace("Brand ", "").replace("Ad Awareness", "Ad")}`,
      group: "conversion",
      fromStage,
      toStage,
      clamp: 0.1,
    });
  }
  columns.push({ key: "csat", label: "CSAT", group: "metric", clamp: 0.15 });
  columns.push({ key: "nps", label: "NPS", group: "metric", clamp: 0.15 });

  const benchmarkStageMap = new Map(
    model.benchmarkStageAggregates.stageAggregates.map((item) => [item.stage, item])
  );
  const benchmarkLinkMap = new Map(
    model.benchmarkStageAggregates.links.map((item) => [`${item.fromStage}->${item.toStage}`, item])
  );

  const benchmarkCells: Record<string, HeatmapCell> = {};
  for (const col of columns) {
    if (col.group === "stage") {
      const stage = benchmarkStageMap.get(col.stage);
      benchmarkCells[col.key] = {
        value: stage?.value ?? null,
        benchmarkValue: stage?.value ?? null,
        delta: 0,
        coverageStudies: stage?.stageCoverageStudies ?? 0,
        totalStudies,
        missing: stage?.value == null,
      };
      continue;
    }
    if (col.group === "conversion") {
      const link = benchmarkLinkMap.get(`${col.fromStage}->${col.toStage}`);
      benchmarkCells[col.key] = {
        value: link?.conversion ?? null,
        benchmarkValue: link?.conversion ?? null,
        delta: 0,
        coverageStudies: link?.linkCoverageStudies ?? 0,
        totalStudies,
        missing: link?.conversion == null,
      };
      continue;
    }
    if (col.key === "csat") {
      benchmarkCells[col.key] = {
        value: model.benchmarkStageAggregates.csat.value,
        benchmarkValue: model.benchmarkStageAggregates.csat.value,
        delta: 0,
        coverageStudies: benchmarkStageMap.get("Brand Satisfaction")?.stageCoverageStudies ?? 0,
        totalStudies,
        missing: model.benchmarkStageAggregates.csat.value == null,
      };
    } else {
      benchmarkCells[col.key] = {
        value: model.benchmarkStageAggregates.nps.value,
        benchmarkValue: model.benchmarkStageAggregates.nps.value,
        delta: 0,
        coverageStudies: benchmarkStageMap.get("Brand Recommendation")?.stageCoverageStudies ?? 0,
        totalStudies,
        missing: model.benchmarkStageAggregates.nps.value == null,
      };
    }
  }

  const rows: HeatmapRow[] = [
    {
      key: "benchmark",
      brandName: "Benchmark",
      isBenchmark: true,
      cells: benchmarkCells,
    },
  ];

  for (const brand of visible) {
    const cells: Record<string, HeatmapCell> = {};
    for (const col of columns) {
      if (col.group === "stage") {
        const val = stageVal(brand, col.stage);
        const b = benchmarkStageMap.get(col.stage);
        const value = val?.value ?? null;
        const benchValue = b?.value ?? null;
        cells[col.key] = {
          value,
          benchmarkValue: benchValue,
          delta: typeof value === "number" && typeof benchValue === "number" ? value - benchValue : null,
          coverageStudies: val?.stageCoverageStudies ?? 0,
          totalStudies,
          missing: value == null,
        };
        continue;
      }
      if (col.group === "conversion") {
        const val = convVal(brand, col.fromStage, col.toStage);
        const b = benchmarkLinkMap.get(`${col.fromStage}->${col.toStage}`);
        const value = val?.conversion ?? null;
        const benchValue = b?.conversion ?? null;
        cells[col.key] = {
          value,
          benchmarkValue: benchValue,
          delta: typeof value === "number" && typeof benchValue === "number" ? value - benchValue : null,
          coverageStudies: val?.linkCoverageStudies ?? 0,
          totalStudies,
          missing: value == null,
        };
        continue;
      }
      if (col.key === "csat") {
        const value = brand.csat.value;
        const benchValue = model.benchmarkStageAggregates.csat.value;
        cells[col.key] = {
          value,
          benchmarkValue: benchValue,
          delta: typeof value === "number" && typeof benchValue === "number" ? value - benchValue : null,
          coverageStudies: stageVal(brand, "Brand Satisfaction")?.stageCoverageStudies ?? 0,
          totalStudies,
          missing: value == null,
        };
      } else {
        const value = brand.nps.value;
        const benchValue = model.benchmarkStageAggregates.nps.value;
        cells[col.key] = {
          value,
          benchmarkValue: benchValue,
          delta: typeof value === "number" && typeof benchValue === "number" ? value - benchValue : null,
          coverageStudies: stageVal(brand, "Brand Recommendation")?.stageCoverageStudies ?? 0,
          totalStudies,
          missing: value == null,
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

