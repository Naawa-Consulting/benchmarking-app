"use client";

import { useEffect, useMemo, useRef, useState, useTransition } from "react";
import { createPortal } from "react-dom";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

import { postJourneyTableMultiDetailed } from "../../lib/api";
import { useScope } from "../../components/layout/ScopeProvider";
import { buildJourneyModel } from "../../features/journey/data/journeyDerived";
import { runJourneyDataSanityChecks } from "../../features/journey/data/journeyDataSanity";
import HeroSankey from "../../features/journey/components/HeroSankey";
import JourneyInsights from "../../features/journey/components/JourneyInsights";
import JourneyHeatmapTable from "../../features/journey/components/JourneyHeatmapTable";
import { generateJourneyInsights } from "../../features/journey/insights/generateJourneyInsights";
import type { JourneyInsight } from "../../features/journey/insights/generateJourneyInsights";
import { buildJourneyHeatmap } from "../../features/journey/heatmap/buildJourneyHeatmap";
import TimeScrubber from "../../features/journey/components/TimeScrubber";
import FocusBar from "../../features/journey/components/FocusBar";
import type { JourneyModel } from "../../features/journey/data/journeySchema";

type TableRow = {
  [key: string]: unknown;
  study_id: string;
  sector: string | null;
  subsector: string | null;
  category: string | null;
  brand: string;
  brand_awareness: number | null;
  ad_awareness: number | null;
  brand_consideration: number | null;
  brand_purchase: number | null;
  brand_satisfaction: number | null;
  brand_recommendation: number | null;
};

type JourneyTableMultiPayload = {
  rows?: TableRow[];
  selection_rows?: TableRow[];
  global_rows?: TableRow[];
  summary_global?: Record<string, unknown>;
  summary_selection?: Record<string, unknown>;
  meta?: {
    cache_hit?: boolean;
    total_ms?: number;
    query_ms?: number;
    studies_processed?: number;
    response_mode?: string;
  };
};

const firstOrNull = (values: string[]) => (values.length ? values[0] : null);
const pct = (value: number | null | undefined) =>
  typeof value === "number" ? `${value >= 0 ? "+" : ""}${(value * 100).toFixed(1)} pts` : "n/a";

const extractYear = (value: string | null | undefined): string | null => {
  if (!value) return null;
  const match = String(value).match(/(19|20)\d{2}/);
  return match ? match[0] : null;
};

const quarterOrder = (value: string): number => {
  const normalized = value.trim();
  const yearQuarter = normalized.match(/(\d{4})\D*Q([1-4])/i);
  if (yearQuarter) return Number(yearQuarter[1]) * 10 + Number(yearQuarter[2]);
  const quarterYear = normalized.match(/Q([1-4])\D*(\d{4})/i);
  if (quarterYear) return Number(quarterYear[2]) * 10 + Number(quarterYear[1]);
  const yearOnly = normalized.match(/(19|20)\d{2}/);
  if (yearOnly) return Number(yearOnly[0]) * 10;
  return Number.MAX_SAFE_INTEGER;
};

const buildBenchmarkOnlyInsights = (
  selectionModel: JourneyModel,
  globalModel: JourneyModel
): JourneyInsight[] => {
  const totalStudies = new Set(selectionModel.rows.map((row) => row.studyId)).size;
  const selectionStageMap = new Map(
    selectionModel.benchmarkStageAggregates.stageAggregates.map((stage) => [stage.stage, stage])
  );
  const globalStageMap = new Map(
    globalModel.benchmarkStageAggregates.stageAggregates.map((stage) => [stage.stage, stage])
  );
  const globalLinkMap = new Map(
    globalModel.benchmarkStageAggregates.links.map((link) => [`${link.fromStage}->${link.toStage}`, link])
  );

  let biggestDrop = selectionModel.benchmarkStageAggregates.links[0] ?? null;
  for (const link of selectionModel.benchmarkStageAggregates.links) {
    if (
      typeof link.dropAbs === "number" &&
      (!biggestDrop || (typeof biggestDrop.dropAbs === "number" ? link.dropAbs > biggestDrop.dropAbs : true))
    ) {
      biggestDrop = link;
    }
  }

  let strongestStage: { stage: string; delta: number; studies: number } | null = null;
  let weakestStage: { stage: string; delta: number; studies: number } | null = null;
  for (const stage of selectionModel.benchmarkStageAggregates.stageAggregates) {
    const global = globalStageMap.get(stage.stage);
    if (typeof stage.value !== "number" || typeof global?.value !== "number") continue;
    const delta = stage.value - global.value;
    if (!strongestStage || delta > strongestStage.delta) {
      strongestStage = { stage: stage.stage, delta, studies: stage.stageCoverageStudies };
    }
    if (!weakestStage || delta < weakestStage.delta) {
      weakestStage = { stage: stage.stage, delta, studies: stage.stageCoverageStudies };
    }
  }

  const csatSelection = selectionModel.benchmarkStageAggregates.csat.value;
  const csatGlobal = globalModel.benchmarkStageAggregates.csat.value;
  const npsSelection = selectionModel.benchmarkStageAggregates.nps.value;
  const npsGlobal = globalModel.benchmarkStageAggregates.nps.value;

  const biggestDropBench =
    biggestDrop && globalLinkMap.get(`${biggestDrop.fromStage}->${biggestDrop.toStage}`)?.dropAbs;

  return [
    {
      id: "drop-benchmark-selection-vs-global",
      title: "Mayor caida del funnel",
      description: biggestDrop
        ? `${biggestDrop.fromStage} -> ${biggestDrop.toStage}: -${pct(biggestDrop.dropAbs)} (vs Global -${pct(
            biggestDropBench ?? null
          )})`
        : "No hay suficiente informacion para calcular caidas.",
      severity: "warning",
      relatedLink: biggestDrop ? { fromStage: biggestDrop.fromStage, toStage: biggestDrop.toStage } : undefined,
      coverage: {
        level:
          totalStudies > 0 && biggestDrop
            ? biggestDrop.linkCoverageStudies / totalStudies >= 0.75
              ? "high"
              : biggestDrop.linkCoverageStudies / totalStudies >= 0.45
                ? "med"
                : "low"
            : "low",
        studies: biggestDrop?.linkCoverageStudies ?? 0,
        totalStudies,
      },
      stats: {
        drop: biggestDrop?.dropAbs ?? null,
        benchmarkDrop: biggestDropBench ?? null,
      },
      score: Math.abs(biggestDrop?.dropAbs ?? 0),
    },
    {
      id: "stage-strength-benchmark-selection-vs-global",
      title: "Fortaleza vs benchmark",
      description: strongestStage
        ? `${strongestStage.stage}: ${pct(strongestStage.delta)} vs Global Benchmark`
        : "No hay suficiente informacion para identificar fortaleza.",
      severity: "positive",
      relatedStage: strongestStage?.stage as JourneyInsight["relatedStage"],
      coverage: {
        level:
          totalStudies > 0 && strongestStage
            ? strongestStage.studies / totalStudies >= 0.75
              ? "high"
              : strongestStage.studies / totalStudies >= 0.45
                ? "med"
                : "low"
            : "low",
        studies: strongestStage?.studies ?? 0,
        totalStudies,
      },
      stats: {
        delta: strongestStage?.delta ?? null,
      },
      score: Math.abs(strongestStage?.delta ?? 0),
    },
    {
      id: "stage-opportunity-benchmark-selection-vs-global",
      title: "Oportunidad vs benchmark",
      description: weakestStage
        ? `${weakestStage.stage}: ${pct(weakestStage.delta)} vs Global Benchmark`
        : "No hay suficiente informacion para identificar oportunidad.",
      severity: "warning",
      relatedStage: weakestStage?.stage as JourneyInsight["relatedStage"],
      coverage: {
        level:
          totalStudies > 0 && weakestStage
            ? weakestStage.studies / totalStudies >= 0.75
              ? "high"
              : weakestStage.studies / totalStudies >= 0.45
                ? "med"
                : "low"
            : "low",
        studies: weakestStage?.studies ?? 0,
        totalStudies,
      },
      stats: {
        delta: weakestStage?.delta ?? null,
      },
      score: Math.abs(weakestStage?.delta ?? 0),
    },
    {
      id: "csat-benchmark-selection-vs-global",
      title: "CSAT highlight",
      description:
        typeof csatSelection === "number" && typeof csatGlobal === "number"
          ? `Selection Benchmark: ${(csatSelection * 100).toFixed(1)}% (${pct(csatSelection - csatGlobal)} vs Global)`
          : "CSAT no disponible para comparacion.",
      severity:
        typeof csatSelection === "number" && typeof csatGlobal === "number" && csatSelection - csatGlobal < 0
          ? "warning"
          : "positive",
      relatedStage: "Brand Satisfaction",
      coverage: {
        level: "med",
        studies: selectionStageMap.get("Brand Satisfaction")?.stageCoverageStudies ?? 0,
        totalStudies,
      },
      stats: {
        csat: csatSelection ?? null,
        benchmark: csatGlobal ?? null,
      },
      score: Math.abs((csatSelection ?? 0) - (csatGlobal ?? 0)),
    },
    {
      id: "nps-benchmark-selection-vs-global",
      title: "NPS highlight",
      description:
        typeof npsSelection === "number" && typeof npsGlobal === "number"
          ? `Selection Benchmark: ${(npsSelection * 100).toFixed(1)}% (${pct(npsSelection - npsGlobal)} vs Global)`
          : "NPS no disponible para comparacion.",
      severity:
        typeof npsSelection === "number" && typeof npsGlobal === "number" && npsSelection - npsGlobal < 0
          ? "warning"
          : "positive",
      relatedStage: "Brand Recommendation",
      coverage: {
        level: "med",
        studies: selectionStageMap.get("Brand Recommendation")?.stageCoverageStudies ?? 0,
        totalStudies,
      },
      stats: {
        nps: npsSelection ?? null,
        benchmark: npsGlobal ?? null,
      },
      score: Math.abs((npsSelection ?? 0) - (npsGlobal ?? 0)),
    },
  ];
};

export default function JourneyPage() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { scope, setScope, dateOptions } = useScope();

  const advancedOpen = searchParams.get("scope_advanced") === "1";
  const [advancedSlot, setAdvancedSlot] = useState<HTMLElement | null>(null);
  const [includeAdAwareness, setIncludeAdAwareness] = useState(false);
  const [focusBrand, setFocusBrand] = useState<string | null>(null);
  const [compareBrand, setCompareBrand] = useState<string | null>(null);
  const [timeMode, setTimeMode] = useState(false);
  const [selectedTimeBucket, setSelectedTimeBucket] = useState<string | null>(null);
  const [isPlaying, setIsPlaying] = useState(false);
  const [playSpeed, setPlaySpeed] = useState<0.5 | 1 | 2>(1);
  const [insightsState, setInsightsState] = useState<ReturnType<typeof generateJourneyInsights>>([]);
  const [isInsightsPending, startInsightsTransition] = useTransition();
  const [selectionRows, setSelectionRows] = useState<TableRow[]>([]);
  const [coreRows, setCoreRows] = useState<TableRow[]>([]);
  const [detailRows, setDetailRows] = useState<TableRow[] | null>(null);
  const [coreLoading, setCoreLoading] = useState(false);
  const [selectionLoading, setSelectionLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [coreMessage, setCoreMessage] = useState<string | null>(null);
  const [selectionMessage, setSelectionMessage] = useState<string | null>(null);
  const [detailMessage, setDetailMessage] = useState<string | null>(null);
  const [benchmarkMode, setBenchmarkMode] = useState<"selection" | "global">("selection");
  const [brandsEnabled, setBrandsEnabled] = useState(false);
  const coreReqSeqRef = useRef(0);
  const selectionReqSeqRef = useRef(0);
  const detailReqSeqRef = useRef(0);
  const coreAbortRef = useRef<AbortController | null>(null);
  const selectionAbortRef = useRef<AbortController | null>(null);
  const detailAbortRef = useRef<AbortController | null>(null);

  const activeSelectionRows = useMemo(() => {
    if (brandsEnabled && detailRows && detailRows.length) return detailRows;
    if (selectionRows.length) return selectionRows;
    return coreRows;
  }, [brandsEnabled, coreRows, detailRows, selectionRows]);
  const globalBenchmarkRows = coreRows;

  const availableBrands = useMemo(
    () => Array.from(new Set(activeSelectionRows.map((row) => row.brand).filter(Boolean))).sort(),
    [activeSelectionRows]
  );

  const selectedBrands = useMemo(() => {
    if (!scope.brands.length) return [];
    const set = new Set(availableBrands);
    return scope.brands.filter((brand) => set.has(brand));
  }, [availableBrands, scope.brands]);

  const timeBuckets = useMemo(
    () => {
      const set = new Set<string>();
      for (const quarter of dateOptions.quarters || []) {
        const year = extractYear(quarter);
        if (year) set.add(year);
      }
      return Array.from(set).sort((a, b) => Number(a) - Number(b));
    },
    [dateOptions.quarters]
  );

  const yearQuarterRange = useMemo(() => {
    const map = new Map<string, { from: string; to: string }>();
    const sorted = [...(dateOptions.quarters || [])].sort((a, b) => quarterOrder(a) - quarterOrder(b));
    for (const quarter of sorted) {
      const year = extractYear(quarter);
      if (!year) continue;
      const existing = map.get(year);
      if (!existing) {
        map.set(year, { from: quarter, to: quarter });
      } else {
        map.set(year, { from: existing.from, to: quarter });
      }
    }
    return map;
  }, [dateOptions.quarters]);

  const fetchInputs = useMemo(() => {
    const animatedYearRange = timeMode && selectedTimeBucket ? yearQuarterRange.get(selectedTimeBucket) : null;
    const effectiveQuarterFrom = animatedYearRange?.from ?? scope.quarterFrom;
    const effectiveQuarterTo = animatedYearRange?.to ?? scope.quarterTo;
    const payload = {
      study_ids: scope.studyIds,
      sector: scope.sector,
      subsector: scope.subsector,
      category: scope.category,
      gender: firstOrNull(scope.gender),
      nse: firstOrNull(scope.nse),
      state: firstOrNull(scope.state),
      age_min: scope.ageMin,
      age_max: scope.ageMax,
      date_grain: scope.timeGranularity,
      quarter_from: effectiveQuarterFrom,
      quarter_to: effectiveQuarterTo,
    };
    const fingerprint = JSON.stringify({
      payload,
      includeAdAwareness,
      selectedTimeBucket: timeMode ? selectedTimeBucket : null,
    });
    return { payload, fingerprint };
  }, [includeAdAwareness, scope, selectedTimeBucket, timeMode, yearQuarterRange]);

  useEffect(() => {
    const includeAd = searchParams.get("include_ad_awareness");
    setIncludeAdAwareness(includeAd === "1");
    setBrandsEnabled(searchParams.get("journey_brands") === "1");
  }, [searchParams]);

  useEffect(() => {
    if (brandsEnabled) return;
    if (!scope.brands.length) return;
    setScope({ brands: [] });
  }, [brandsEnabled, scope.brands.length, setScope]);

  useEffect(() => {
    if (typeof document === "undefined") return;
    if (!advancedOpen) {
      setAdvancedSlot(null);
      return;
    }
    const syncSlot = () => setAdvancedSlot(document.getElementById("journey-advanced-controls-slot"));
    syncSlot();
    const frame = window.requestAnimationFrame(syncSlot);
    return () => window.cancelAnimationFrame(frame);
  }, [advancedOpen]);

  useEffect(() => {
    if (!timeMode) {
      setIsPlaying(false);
      return;
    }
    if (!timeBuckets.length) {
      setSelectedTimeBucket(null);
      setIsPlaying(false);
      return;
    }
    if (!selectedTimeBucket || !timeBuckets.includes(selectedTimeBucket)) {
      setSelectedTimeBucket(timeBuckets[0]);
    }
  }, [selectedTimeBucket, timeBuckets, timeMode]);

  const setIncludeAdAwarenessAndQuery = (nextIncludeAdAwareness: boolean) => {
    setIncludeAdAwareness(nextIncludeAdAwareness);
    const params = new URLSearchParams(searchParams.toString());
    if (nextIncludeAdAwareness) params.set("include_ad_awareness", "1");
    else params.delete("include_ad_awareness");
    router.replace(`${pathname}${params.toString() ? `?${params.toString()}` : ""}`, { scroll: false });
  };

  const setJourneyBrandsEnabledAndQuery = (nextEnabled: boolean) => {
    setBrandsEnabled(nextEnabled);
    if (!nextEnabled && scope.brands.length) {
      setScope({ brands: [] });
    }
    const params = new URLSearchParams(searchParams.toString());
    if (nextEnabled) params.set("journey_brands", "1");
    else params.delete("journey_brands");
    router.replace(`${pathname}${params.toString() ? `?${params.toString()}` : ""}`, { scroll: false });
  };

  const selectionJourneyModel = useMemo(() => {
    const t0 = performance.now();
    const model = buildJourneyModel(activeSelectionRows, {
        studyIds: scope.studyIds,
        sector: scope.sector,
        subsector: scope.subsector,
        category: scope.category,
        gender: scope.gender,
        nse: scope.nse,
        state: scope.state,
        ageMin: scope.ageMin,
        ageMax: scope.ageMax,
        quarterFrom: scope.quarterFrom,
        quarterTo: scope.quarterTo,
      },
      { includeAdAwareness, benchmarkScope: "category" }
    );
    if (process.env.NODE_ENV !== "production") {
      const t1 = performance.now();
      console.debug("[JourneyPerf] model_ms", Number((t1 - t0).toFixed(2)));
    }
    return model;
  }, [activeSelectionRows, includeAdAwareness, scope]);

  const globalBenchmarkJourneyModel = useMemo(
    () =>
      buildJourneyModel(
        activeSelectionRows,
        {
          studyIds: scope.studyIds,
          sector: scope.sector,
          subsector: scope.subsector,
          category: scope.category,
          gender: scope.gender,
          nse: scope.nse,
          state: scope.state,
          ageMin: scope.ageMin,
          ageMax: scope.ageMax,
          quarterFrom: scope.quarterFrom,
          quarterTo: scope.quarterTo,
        },
        {
          includeAdAwareness,
          benchmarkScope: "category",
          benchmarkRows: globalBenchmarkRows,
        }
      ),
    [activeSelectionRows, globalBenchmarkRows, includeAdAwareness, scope]
  );

  const benchmarkLabel = benchmarkMode === "global" ? "Global Benchmark" : "Selection Benchmark";
  const journeyModel = benchmarkMode === "global" ? globalBenchmarkJourneyModel : selectionJourneyModel;
  const benchmarkFunnelModel = useMemo<JourneyModel>(() => {
    const selectionBenchmarkBrandKey = "selection-benchmark";
    const selectionBenchmarkBrand = {
      key: selectionBenchmarkBrandKey,
      brandId: null,
      brandName: "Selection Benchmark",
      dims: {},
      stageAggregates: selectionJourneyModel.benchmarkStageAggregates.stageAggregates,
      links: selectionJourneyModel.benchmarkStageAggregates.links,
      totalConversion:
        selectionJourneyModel.benchmarkStageAggregates.links[
          selectionJourneyModel.benchmarkStageAggregates.links.length - 1
        ]?.conversion ?? null,
      csat: selectionJourneyModel.benchmarkStageAggregates.csat,
      nps: selectionJourneyModel.benchmarkStageAggregates.nps,
    };

    return {
      ...selectionJourneyModel,
      brandStageAggregates: [selectionBenchmarkBrand],
      benchmarkStageAggregates: globalBenchmarkJourneyModel.benchmarkStageAggregates,
      journeyIndexByBrand: {
        [selectionBenchmarkBrandKey]: selectionJourneyModel.benchmarkJourneyIndex,
      },
      benchmarkJourneyIndex: globalBenchmarkJourneyModel.benchmarkJourneyIndex,
      funnelHealthByBrand: {
        [selectionBenchmarkBrandKey]: selectionJourneyModel.benchmarkFunnelHealth,
      },
      benchmarkFunnelHealth: globalBenchmarkJourneyModel.benchmarkFunnelHealth,
      stageGaps: [],
    };
  }, [globalBenchmarkJourneyModel, selectionJourneyModel]);

  const journeyHeatmap = useMemo(() => {
    const t0 = performance.now();
    const matrix = buildJourneyHeatmap(
      journeyModel,
      selectedBrands,
      brandsEnabled ? Number.MAX_SAFE_INTEGER : 0,
      {
      benchmarkMode,
      selectionBenchmark: {
        aggregate: selectionJourneyModel.benchmarkStageAggregates,
        journeyIndex: selectionJourneyModel.benchmarkJourneyIndex,
      },
      globalBenchmark: {
        aggregate: globalBenchmarkJourneyModel.benchmarkStageAggregates,
        journeyIndex: globalBenchmarkJourneyModel.benchmarkJourneyIndex,
      },
      }
    );
    if (process.env.NODE_ENV !== "production") {
      const t1 = performance.now();
      console.debug("[JourneyPerf] heatmap_ms", Number((t1 - t0).toFixed(2)));
    }
    return matrix;
  }, [benchmarkMode, brandsEnabled, globalBenchmarkJourneyModel, journeyModel, selectedBrands, selectionJourneyModel]);

  const journeyInsights = insightsState;

  useEffect(() => {
    if (process.env.NODE_ENV === "production") return;
    try {
      runJourneyDataSanityChecks();
    } catch (error) {
      console.error(error);
    }
  }, []);

  useEffect(() => {
    startInsightsTransition(() => {
      if (!brandsEnabled) {
        setInsightsState(buildBenchmarkOnlyInsights(selectionJourneyModel, globalBenchmarkJourneyModel));
        return;
      }

      const selectedForInsights =
        focusBrand ?? selectedBrands[0] ?? journeyModel.brandStageAggregates[0]?.brandName ?? null;
      const insightBrands = selectedForInsights ? [selectedForInsights] : selectedBrands;
      setInsightsState(
        generateJourneyInsights(journeyModel, insightBrands, "category", {
          maxItems: 50,
          focusBrandName: selectedForInsights,
          compareBrandName: compareBrand,
        })
      );
    });
  }, [
    brandsEnabled,
    compareBrand,
    focusBrand,
    globalBenchmarkJourneyModel,
    journeyModel,
    selectedBrands,
    selectionJourneyModel,
    startInsightsTransition,
  ]);

  useEffect(() => {
    if (!focusBrand) return;
    if (scope.brands.includes(focusBrand)) return;
    setScope({ brands: [...scope.brands, focusBrand] });
  }, [focusBrand, scope.brands, setScope]);

  useEffect(() => {
    if (!focusBrand) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== "Escape") return;
      setFocusBrand(null);
      setCompareBrand(null);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [focusBrand]);

  useEffect(() => {
    if (!timeMode || !isPlaying || timeBuckets.length < 2) return;
    const intervalMs = Math.max(300, Math.round(750 / playSpeed));
    const timer = window.setInterval(() => {
      setSelectedTimeBucket((prev) => {
        const currentIndex = prev ? timeBuckets.indexOf(prev) : 0;
        const nextIndex = currentIndex + 1;
        if (nextIndex >= timeBuckets.length) {
          setIsPlaying(false);
          return prev;
        }
        return timeBuckets[nextIndex];
      });
    }, intervalMs);
    return () => window.clearInterval(timer);
  }, [isPlaying, playSpeed, timeBuckets, timeMode]);

  useEffect(() => {
    if (process.env.NODE_ENV === "production") return;
    if (!journeyModel.rows.length) return;
    const topBrands = journeyModel.brandStageAggregates
      .slice()
      .sort((a, b) => (b.totalConversion || 0) - (a.totalConversion || 0))
      .slice(0, 3)
      .map((item) => ({
        brand: item.brandName,
        conversion: item.totalConversion,
        csat: item.csat.value,
        csatType: item.csat.meta.metricType,
        nps: item.nps.value,
        npsType: item.nps.meta.metricType,
      }));
    console.debug("[JourneyModel] coverage and derived metrics", {
      rows: journeyModel.rows.length,
      stagesOrdered: journeyModel.stagesOrdered,
      coverageByStage: journeyModel.metadata.coverage.byStage,
      coverageByLink: journeyModel.metadata.coverage.byLink,
      warnings: journeyModel.metadata.warnings,
      topBrands,
    });
  }, [journeyModel]);

  useEffect(() => {
    const seq = coreReqSeqRef.current + 1;
    coreReqSeqRef.current = seq;
    coreAbortRef.current?.abort();
    const abortController = new AbortController();
    coreAbortRef.current = abortController;
    const startedAt = performance.now();

    setCoreLoading(true);
    setCoreMessage(null);

    postJourneyTableMultiDetailed(fetchInputs.payload, "all", "brand_awareness", "desc", {
      responseMode: "benchmark_global",
      signal: abortController.signal,
    })
      .then((result) => {
        if (abortController.signal.aborted || seq !== coreReqSeqRef.current) return;
        if (!result.ok) {
          const message =
            result.data && typeof result.data === "object" && "detail" in result.data
              ? String(result.data.detail)
              : result.error || "Unable to load global benchmark.";
          setCoreMessage(message);
          return;
        }
        const payload = (result.data || {}) as JourneyTableMultiPayload;
        const globalRows = Array.isArray(payload.global_rows)
          ? payload.global_rows
          : Array.isArray(payload.rows)
            ? payload.rows
            : [];
        setCoreRows(globalRows);
        if (process.env.NODE_ENV !== "production") {
          console.debug("[JourneyPerf] fetchGlobalMs", Number((performance.now() - startedAt).toFixed(2)), {
            requestSeq: seq,
            cacheHit: payload.meta?.cache_hit ?? false,
            rows: globalRows.length,
          });
        }
      })
      .catch((error) => {
        if (abortController.signal.aborted || seq !== coreReqSeqRef.current) return;
        setCoreMessage(error instanceof Error ? error.message : "Unable to load global benchmark.");
      })
      .finally(() => {
        if (abortController.signal.aborted || seq !== coreReqSeqRef.current) return;
        setCoreLoading(false);
      });

    return () => {
      abortController.abort();
    };
  }, [fetchInputs]);

  useEffect(() => {
    const seq = selectionReqSeqRef.current + 1;
    selectionReqSeqRef.current = seq;
    selectionAbortRef.current?.abort();
    const abortController = new AbortController();
    selectionAbortRef.current = abortController;
    const startedAt = performance.now();

    setSelectionLoading(true);
    setSelectionMessage(null);

    postJourneyTableMultiDetailed(fetchInputs.payload, "all", "brand_awareness", "desc", {
      responseMode: "benchmark_selection",
      signal: abortController.signal,
    })
      .then((result) => {
        if (abortController.signal.aborted || seq !== selectionReqSeqRef.current) return;
        if (!result.ok) {
          const message =
            result.data && typeof result.data === "object" && "detail" in result.data
              ? String(result.data.detail)
              : result.error || "Unable to load selection benchmark.";
          setSelectionMessage(message);
          return;
        }
        const payload = (result.data || {}) as JourneyTableMultiPayload;
        const rows = Array.isArray(payload.rows) ? payload.rows : [];
        setSelectionRows(rows);
        if (process.env.NODE_ENV !== "production") {
          console.debug("[JourneyPerf] fetchSelectionMs", Number((performance.now() - startedAt).toFixed(2)), {
            requestSeq: seq,
            cacheHit: payload.meta?.cache_hit ?? false,
            rows: rows.length,
          });
        }
      })
      .catch((error) => {
        if (abortController.signal.aborted || seq !== selectionReqSeqRef.current) return;
        setSelectionMessage(error instanceof Error ? error.message : "Unable to load selection benchmark.");
      })
      .finally(() => {
        if (abortController.signal.aborted || seq !== selectionReqSeqRef.current) return;
        setSelectionLoading(false);
      });

    return () => {
      abortController.abort();
    };
  }, [fetchInputs]);

  useEffect(() => {
    if (!brandsEnabled) {
      setDetailRows(null);
      setDetailLoading(false);
      setDetailMessage(null);
      return;
    }
    const seq = detailReqSeqRef.current + 1;
    detailReqSeqRef.current = seq;
    detailAbortRef.current?.abort();
    const abortController = new AbortController();
    detailAbortRef.current = abortController;
    const startedAt = performance.now();

    setDetailLoading(true);
    setDetailMessage(null);

    postJourneyTableMultiDetailed(fetchInputs.payload, "all", "brand_awareness", "desc", {
      responseMode: "full",
      signal: abortController.signal,
    })
      .then((result) => {
        if (abortController.signal.aborted || seq !== detailReqSeqRef.current) return;
        if (!result.ok) {
          const message =
            result.data && typeof result.data === "object" && "detail" in result.data
              ? String(result.data.detail)
              : result.error || "Unable to load brand detail.";
          setDetailMessage(message);
          return;
        }
        const payload = (result.data || {}) as JourneyTableMultiPayload;
        const rows = Array.isArray(payload.rows) ? payload.rows : [];
        setDetailRows(rows);
        if (process.env.NODE_ENV !== "production") {
          console.debug("[JourneyPerf] fetchDetailMs", Number((performance.now() - startedAt).toFixed(2)), {
            requestSeq: seq,
            cacheHit: payload.meta?.cache_hit ?? false,
            rows: rows.length,
          });
        }
      })
      .catch((error) => {
        if (abortController.signal.aborted || seq !== detailReqSeqRef.current) return;
        setDetailMessage(error instanceof Error ? error.message : "Unable to load brand detail.");
      })
      .finally(() => {
        if (abortController.signal.aborted || seq !== detailReqSeqRef.current) return;
        setDetailLoading(false);
      });

    return () => {
      abortController.abort();
    };
  }, [brandsEnabled, fetchInputs]);

  const advancedControlsContent =
    advancedOpen && advancedSlot ? (
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        <section className="rounded-2xl border border-ink/10 bg-white p-4">
          <p className="text-xs font-semibold uppercase tracking-[0.08em] text-slate">Funnel Structure</p>
          <button
            type="button"
            className={`mt-3 rounded-full border px-3 py-2 text-xs ${
              includeAdAwareness
                ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
                : "border-ink/10 bg-white text-slate"
            }`}
            onClick={() => setIncludeAdAwarenessAndQuery(!includeAdAwareness)}
          >
            Include Ad Awareness: {includeAdAwareness ? "On" : "Off"}
          </button>
          <div className="mt-3 flex flex-wrap items-center gap-2">
            <span className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate">Benchmark</span>
            {(["selection", "global"] as const).map((mode) => (
              <button
                key={mode}
                type="button"
                onClick={() => setBenchmarkMode(mode)}
                className={`rounded-full border px-3 py-1 text-xs ${
                  benchmarkMode === mode
                    ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
                    : "border-ink/10 text-slate"
                }`}
              >
                {mode === "selection" ? "Selection Benchmark" : "Global Benchmark"}
              </button>
            ))}
          </div>
          <div className="mt-3 flex flex-wrap items-center gap-2">
            <span className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate">Brands</span>
            {([false, true] as const).map((mode) => (
              <button
                key={mode ? "enabled" : "disabled"}
                type="button"
                onClick={() => setJourneyBrandsEnabledAndQuery(mode)}
                className={`rounded-full border px-3 py-1 text-xs ${
                  brandsEnabled === mode
                    ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
                    : "border-ink/10 text-slate"
                }`}
              >
                {mode ? "Enable" : "Disable"}
              </button>
            ))}
          </div>
        </section>

        <section className="rounded-2xl border border-ink/10 bg-white p-4 md:col-span-2 xl:col-span-1">
          <p className="mb-2 text-xs font-semibold uppercase tracking-[0.08em] text-slate">Time Animation</p>
          <TimeScrubber
            enabled={timeMode}
            timeBuckets={timeBuckets}
            selectedBucket={selectedTimeBucket}
            isPlaying={isPlaying}
            speed={playSpeed}
            onToggleEnabled={(next) => {
              setTimeMode(next);
              if (!next) {
                setSelectedTimeBucket(null);
                setIsPlaying(false);
              }
            }}
            onSelectBucket={(bucket) => {
              setSelectedTimeBucket(bucket);
              setIsPlaying(false);
            }}
            onPrev={() => {
              if (!timeBuckets.length) return;
              setIsPlaying(false);
              setSelectedTimeBucket((prev) => {
                const idx = prev ? timeBuckets.indexOf(prev) : 0;
                return timeBuckets[Math.max(0, idx - 1)];
              });
            }}
            onNext={() => {
              if (!timeBuckets.length) return;
              setIsPlaying(false);
              setSelectedTimeBucket((prev) => {
                const idx = prev ? timeBuckets.indexOf(prev) : -1;
                return timeBuckets[Math.min(timeBuckets.length - 1, idx + 1)];
              });
            }}
            onTogglePlay={() => setIsPlaying((prev) => !prev)}
            onSpeedChange={setPlaySpeed}
          />
        </section>

        <section className="rounded-2xl border border-ink/10 bg-white p-4 md:col-span-2 xl:col-span-1">
          <p className="mb-2 text-xs font-semibold uppercase tracking-[0.08em] text-slate">Focus Mode</p>
          <FocusBar
            availableBrands={availableBrands}
            focusBrand={focusBrand}
            compareBrand={compareBrand}
            onFocusBrandChange={(brand) => {
              setFocusBrand(brand);
              if (brand && compareBrand === brand) setCompareBrand(null);
            }}
            onCompareBrandChange={setCompareBrand}
            onClearFocus={() => {
              setFocusBrand(null);
              setCompareBrand(null);
            }}
          />
        </section>
      </div>
    ) : null;

  const isCoreUpdating = coreLoading;
  const isSelectionUpdating = selectionLoading;
  const isBrandUpdating = brandsEnabled && detailLoading;
  const isBenchmarkUpdating = isCoreUpdating || isSelectionUpdating;
  const isHeatmapUpdating = brandsEnabled ? isBrandUpdating : isBenchmarkUpdating;
  const statusMessage =
    coreMessage ||
    selectionMessage ||
    (brandsEnabled ? detailMessage : null) ||
    null;
  return (
    <main className="space-y-6">
      {advancedControlsContent && advancedSlot ? createPortal(advancedControlsContent, advancedSlot) : null}

      {statusMessage && (
        <section className="main-surface p-6">
          <p className="text-sm text-red-600">
            {statusMessage} Open Admin {">"} Data Validation {">"} Journey Data for raw validation tables.
          </p>
        </section>
      )}

      <section className="relative">
        <HeroSankey
          model={benchmarkFunnelModel}
          selectedBrandNames={["Selection Benchmark"]}
          benchmarkLabel="Global Benchmark"
          title="Benchmark Funnel"
          subtitle="Comparativa entre Selection Benchmark y Global Benchmark."
          primaryLegendLabel="Selection Benchmark"
          timeBucketLabel={timeMode ? selectedTimeBucket : null}
        />
        {isBenchmarkUpdating && (
          <div className="pointer-events-none absolute inset-0 z-20 flex items-center justify-center rounded-[2rem] border border-amber-500/25 bg-white/55 backdrop-blur-[1px]">
            <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-3 py-1 text-xs font-semibold text-amber-700">
              {isCoreUpdating ? "Updating Global..." : "Updating Selection..."}
            </span>
          </div>
        )}
      </section>

      {brandsEnabled && (
        <section className="relative">
          <HeroSankey
            model={journeyModel}
            selectedBrandNames={selectedBrands}
            focusBrandName={focusBrand}
            compareBrandName={compareBrand}
            timeBucketLabel={timeMode ? selectedTimeBucket : null}
            benchmarkLabel={benchmarkLabel}
          />
          {isBrandUpdating && (
            <div className="pointer-events-none absolute inset-0 z-20 flex items-center justify-center rounded-[2rem] border border-amber-500/25 bg-white/55 backdrop-blur-[1px]">
              <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-3 py-1 text-xs font-semibold text-amber-700">
                Updating Brands...
              </span>
            </div>
          )}
        </section>
      )}
      <JourneyInsights insights={journeyInsights} />
      {isInsightsPending && <p className="px-2 text-xs text-slate">Updating insights...</p>}
      <section className="relative">
        <JourneyHeatmapTable
          matrix={journeyHeatmap}
          benchmarkLabel={benchmarkLabel}
          focusedBrandName={focusBrand}
          onFocusBrand={(brand) => {
            setFocusBrand(brand);
            if (brand && compareBrand === brand) setCompareBrand(null);
          }}
        />
        {isHeatmapUpdating && (
          <div className="pointer-events-none absolute inset-0 z-20 flex items-center justify-center rounded-[2rem] border border-amber-500/25 bg-white/35">
            <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-3 py-1 text-xs font-semibold text-amber-700">
              {isBrandUpdating ? "Updating Brands..." : "Updating Benchmarks..."}
            </span>
          </div>
        )}
      </section>
    </main>
  );
}
