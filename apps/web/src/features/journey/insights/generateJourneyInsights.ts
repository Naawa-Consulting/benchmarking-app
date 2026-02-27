import type { JourneyBrandAggregate, JourneyModel, JourneyStage } from "../data/journeySchema";

export type JourneyInsight = {
  id: string;
  title: string;
  description: string;
  severity: "info" | "positive" | "warning";
  relatedStage?: JourneyStage;
  relatedLink?: { fromStage: JourneyStage; toStage: JourneyStage };
  stats?: Record<string, number | string | null>;
  coverage: { level: "high" | "med" | "low"; studies: number; totalStudies: number };
  score: number;
};

type InsightOptions = {
  maxItems?: number;
  focusBrandName?: string | null;
  compareBrandName?: string | null;
};

const pct = (value: number | null | undefined) =>
  typeof value === "number" ? `${(value * 100).toFixed(1)} pts` : "--";

const coverageLevel = (studies: number, totalStudies: number) => {
  const ratio = totalStudies > 0 ? studies / totalStudies : 0;
  if (ratio >= 0.75) return "high" as const;
  if (ratio >= 0.45) return "med" as const;
  return "low" as const;
};

const coverageWeight = (level: "high" | "med" | "low") => {
  if (level === "high") return 1;
  if (level === "med") return 0.7;
  return 0.4;
};

const stageValue = (brand: JourneyBrandAggregate, stage: JourneyStage) =>
  brand.stageAggregates.find((item) => item.stage === stage)?.value ?? null;

export function generateJourneyInsights(
  model: JourneyModel,
  selectedBrands: string[],
  _benchmarkId = "category",
  options?: InsightOptions
): JourneyInsight[] {
  const maxItems = options?.maxItems ?? 8;
  const totalStudies = new Set(model.rows.map((row) => row.studyId)).size;
  if (!model.brandStageAggregates.length || !model.stagesOrdered.length) return [];

  const focusBrand =
    model.brandStageAggregates.find((item) => item.brandName === options?.focusBrandName) ||
    model.brandStageAggregates.find((item) => selectedBrands.includes(item.brandName)) ||
    model.brandStageAggregates[0];
  if (!focusBrand) return [];

  const benchmarkStageMap = new Map(
    model.benchmarkStageAggregates.stageAggregates.map((item) => [item.stage, item.value])
  );
  const benchmarkLinkMap = new Map(
    model.benchmarkStageAggregates.links.map((item) => [`${item.fromStage}->${item.toStage}`, item])
  );

  const candidates: JourneyInsight[] = [];

  for (const link of focusBrand.links) {
    if (typeof link.dropAbs !== "number") continue;
    const cov = coverageLevel(link.linkCoverageStudies, totalStudies);
    const bench = benchmarkLinkMap.get(`${link.fromStage}->${link.toStage}`);
    const benchDrop = bench?.dropAbs ?? null;
    const score = Math.abs(link.dropAbs) * coverageWeight(cov);
    candidates.push({
      id: `drop-${focusBrand.key}-${link.fromStage}-${link.toStage}`,
      title: "Mayor caida del funnel",
      description: `${link.fromStage} -> ${link.toStage}: -${pct(link.dropAbs)} (vs bench -${pct(benchDrop)})`,
      severity: "warning",
      relatedLink: { fromStage: link.fromStage, toStage: link.toStage },
      coverage: { level: cov, studies: link.linkCoverageStudies, totalStudies },
      stats: { brand: focusBrand.brandName, drop: link.dropAbs, benchmarkDrop: benchDrop },
      score,
    });
  }

  for (const stage of focusBrand.stageAggregates) {
    if (typeof stage.value !== "number") continue;
    const bench = benchmarkStageMap.get(stage.stage);
    if (typeof bench !== "number") continue;
    const delta = stage.value - bench;
    const cov = coverageLevel(stage.stageCoverageStudies, totalStudies);
    const score = Math.abs(delta) * coverageWeight(cov);
    candidates.push({
      id: `stage-${focusBrand.key}-${stage.stage}`,
      title: delta >= 0 ? "Fortaleza vs benchmark" : "Oportunidad vs benchmark",
      description: `${stage.stage}: ${delta >= 0 ? "+" : ""}${pct(delta)} vs benchmark`,
      severity: delta >= 0 ? "positive" : "warning",
      relatedStage: stage.stage,
      coverage: { level: cov, studies: stage.stageCoverageStudies, totalStudies },
      stats: { brand: focusBrand.brandName, delta, stageValue: stage.value, benchmarkValue: bench },
      score,
    });
  }

  for (const link of focusBrand.links) {
    if (typeof link.conversion !== "number") continue;
    const bench = benchmarkLinkMap.get(`${link.fromStage}->${link.toStage}`)?.conversion ?? null;
    if (typeof bench !== "number") continue;
    const delta = link.conversion - bench;
    const cov = coverageLevel(link.linkCoverageStudies, totalStudies);
    const score = Math.abs(delta) * coverageWeight(cov);
    candidates.push({
      id: `conv-${focusBrand.key}-${link.fromStage}-${link.toStage}`,
      title: delta >= 0 ? "Conversion destacada" : "Conversion por debajo de benchmark",
      description: `${link.fromStage} -> ${link.toStage}: ${delta >= 0 ? "+" : ""}${pct(delta)} vs bench`,
      severity: delta >= 0 ? "positive" : "warning",
      relatedLink: { fromStage: link.fromStage, toStage: link.toStage },
      coverage: { level: cov, studies: link.linkCoverageStudies, totalStudies },
      stats: { brand: focusBrand.brandName, conversion: link.conversion, benchmarkConversion: bench, delta },
      score,
    });
  }

  if (selectedBrands.length >= 2) {
    let bestDiff: {
      stage: JourneyStage;
      brandA: string;
      brandB: string;
      diff: number;
      studies: number;
    } | null = null;
    const selected = model.brandStageAggregates.filter((item) => selectedBrands.includes(item.brandName));
    for (const stage of model.stagesOrdered) {
      for (let i = 0; i < selected.length; i += 1) {
        for (let j = i + 1; j < selected.length; j += 1) {
          const a = selected[i];
          const b = selected[j];
          const av = stageValue(a, stage);
          const bv = stageValue(b, stage);
          if (typeof av !== "number" || typeof bv !== "number") continue;
          const diff = Math.abs(av - bv);
          const studies = Math.min(
            a.stageAggregates.find((item) => item.stage === stage)?.stageCoverageStudies || 0,
            b.stageAggregates.find((item) => item.stage === stage)?.stageCoverageStudies || 0
          );
          if (!bestDiff || diff > bestDiff.diff) {
            bestDiff = { stage, brandA: a.brandName, brandB: b.brandName, diff, studies };
          }
        }
      }
    }
    if (bestDiff) {
      const cov = coverageLevel(bestDiff.studies, totalStudies);
      candidates.push({
        id: `diff-${bestDiff.stage}-${bestDiff.brandA}-${bestDiff.brandB}`,
        title: "Mayor diferenciacion entre marcas",
        description: `${bestDiff.stage}: ${bestDiff.brandA} vs ${bestDiff.brandB} = ${pct(bestDiff.diff)}`,
        severity: "info",
        relatedStage: bestDiff.stage,
        coverage: { level: cov, studies: bestDiff.studies, totalStudies },
        stats: { brandA: bestDiff.brandA, brandB: bestDiff.brandB, diff: bestDiff.diff },
        score: bestDiff.diff * coverageWeight(cov),
      });
    }
  }

  const csatBench = model.benchmarkStageAggregates.csat.value;
  const npsBench = model.benchmarkStageAggregates.nps.value;
  if (typeof focusBrand.csat.value === "number") {
    const csatDelta = typeof csatBench === "number" ? focusBrand.csat.value - csatBench : null;
    const cov = coverageLevel(
      focusBrand.stageAggregates.find((item) => item.stage === "Brand Satisfaction")?.stageCoverageStudies || 0,
      totalStudies
    );
    candidates.push({
      id: `csat-${focusBrand.key}`,
      title: "CSAT highlight",
      description: `${focusBrand.brandName}: ${pct(focusBrand.csat.value)}${typeof csatDelta === "number" ? ` (${csatDelta >= 0 ? "+" : ""}${pct(csatDelta)} vs bench)` : ""}`,
      severity: typeof csatDelta === "number" && csatDelta < 0 ? "warning" : "positive",
      relatedStage: "Brand Satisfaction",
      coverage: {
        level: cov,
        studies:
          focusBrand.stageAggregates.find((item) => item.stage === "Brand Satisfaction")?.stageCoverageStudies || 0,
        totalStudies,
      },
      stats: { brand: focusBrand.brandName, csat: focusBrand.csat.value, benchmark: csatBench, type: focusBrand.csat.meta.metricType },
      score: Math.abs(csatDelta ?? focusBrand.csat.value) * coverageWeight(cov),
    });
  }
  if (typeof focusBrand.nps.value === "number") {
    const npsDelta = typeof npsBench === "number" ? focusBrand.nps.value - npsBench : null;
    const cov = coverageLevel(
      focusBrand.stageAggregates.find((item) => item.stage === "Brand Recommendation")?.stageCoverageStudies || 0,
      totalStudies
    );
    candidates.push({
      id: `nps-${focusBrand.key}`,
      title: "NPS highlight",
      description: `${focusBrand.brandName}: ${pct(focusBrand.nps.value)}${typeof npsDelta === "number" ? ` (${npsDelta >= 0 ? "+" : ""}${pct(npsDelta)} vs bench)` : ""}`,
      severity: typeof npsDelta === "number" && npsDelta < 0 ? "warning" : "positive",
      relatedStage: "Brand Recommendation",
      coverage: {
        level: cov,
        studies:
          focusBrand.stageAggregates.find((item) => item.stage === "Brand Recommendation")?.stageCoverageStudies || 0,
        totalStudies,
      },
      stats: { brand: focusBrand.brandName, nps: focusBrand.nps.value, benchmark: npsBench, type: focusBrand.nps.meta.metricType },
      score: Math.abs(npsDelta ?? focusBrand.nps.value) * coverageWeight(cov),
    });
  }

  if (options?.focusBrandName && options?.compareBrandName) {
    const focus = model.brandStageAggregates.find((item) => item.brandName === options.focusBrandName);
    const compare = model.brandStageAggregates.find((item) => item.brandName === options.compareBrandName);
    if (focus && compare) {
      for (const stage of model.stagesOrdered) {
        const fv = stageValue(focus, stage);
        const cv = stageValue(compare, stage);
        if (typeof fv !== "number" || typeof cv !== "number") continue;
        const diff = fv - cv;
        const coverageStudies = Math.min(
          focus.stageAggregates.find((item) => item.stage === stage)?.stageCoverageStudies || 0,
          compare.stageAggregates.find((item) => item.stage === stage)?.stageCoverageStudies || 0
        );
        const cov = coverageLevel(coverageStudies, totalStudies);
        candidates.push({
          id: `focus-compare-${stage}`,
          title: "Focus vs compare",
          description: `${stage}: ${focus.brandName} ${diff >= 0 ? "+" : ""}${pct(diff)} vs ${compare.brandName}`,
          severity: diff >= 0 ? "positive" : "warning",
          relatedStage: stage,
          coverage: { level: cov, studies: coverageStudies, totalStudies },
          stats: { brand: focus.brandName, compareBrand: compare.brandName, diff },
          score: Math.abs(diff) * coverageWeight(cov),
        });
      }
    }
  }

  for (const warning of model.metadata.warnings) {
    candidates.push({
      id: `warn-${warning}`,
      title: "Coverage warning",
      description: warning,
      severity: "warning",
      coverage: { level: "low", studies: 0, totalStudies },
      score: 0.2,
    });
  }

  const seen = new Set<string>();
  return candidates
    .filter((item) => {
      if (seen.has(item.description)) return false;
      seen.add(item.description);
      return true;
    })
    .sort((a, b) => b.score - a.score)
    .slice(0, Math.max(4, maxItems));
}
