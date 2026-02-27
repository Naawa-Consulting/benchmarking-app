"use client";

import { useEffect, useMemo, useState, useTransition } from "react";
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
import { buildJourneyHeatmap } from "../../features/journey/heatmap/buildJourneyHeatmap";
import TimeScrubber from "../../features/journey/components/TimeScrubber";
import FocusBar from "../../features/journey/components/FocusBar";

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

const firstOrNull = (values: string[]) => (values.length ? values[0] : null);

export default function JourneyPage() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { scope, setScope, dateOptions } = useScope();

  const advancedOpen = searchParams.get("scope_advanced") === "1";
  const [advancedSlot, setAdvancedSlot] = useState<HTMLElement | null>(null);
  const [includeAdAwareness, setIncludeAdAwareness] = useState(true);
  const [focusBrand, setFocusBrand] = useState<string | null>(null);
  const [compareBrand, setCompareBrand] = useState<string | null>(null);
  const [timeMode, setTimeMode] = useState(false);
  const [selectedTimeBucket, setSelectedTimeBucket] = useState<string | null>(null);
  const [isPlaying, setIsPlaying] = useState(false);
  const [playSpeed, setPlaySpeed] = useState<0.5 | 1 | 2>(1);
  const [insightsState, setInsightsState] = useState<ReturnType<typeof generateJourneyInsights>>([]);
  const [isInsightsPending, startInsightsTransition] = useTransition();
  const [tableRows, setTableRows] = useState<TableRow[]>([]);
  const [tableState, setTableState] = useState<"idle" | "loading" | "error">("idle");
  const [tableMessage, setTableMessage] = useState<string | null>(null);

  const availableBrands = useMemo(
    () => Array.from(new Set(tableRows.map((row) => row.brand).filter(Boolean))).sort(),
    [tableRows]
  );

  const selectedBrands = useMemo(() => {
    if (!scope.brands.length) return [];
    const set = new Set(availableBrands);
    return scope.brands.filter((brand) => set.has(brand));
  }, [availableBrands, scope.brands]);

  const timeBuckets = useMemo(
    () => (Array.isArray(dateOptions.quarters) ? dateOptions.quarters.filter(Boolean) : []),
    [dateOptions.quarters]
  );

  useEffect(() => {
    const includeAd = searchParams.get("include_ad_awareness");
    setIncludeAdAwareness(includeAd == null ? true : includeAd !== "0");
  }, [searchParams]);

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
    if (!nextIncludeAdAwareness) params.set("include_ad_awareness", "0");
    else params.delete("include_ad_awareness");
    router.replace(`${pathname}${params.toString() ? `?${params.toString()}` : ""}`, { scroll: false });
  };

  const journeyModel = useMemo(() => {
    const t0 = performance.now();
    const model = buildJourneyModel(tableRows, {
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
  }, [includeAdAwareness, scope, tableRows]);

  const journeyHeatmap = useMemo(() => {
    const t0 = performance.now();
    const matrix = buildJourneyHeatmap(journeyModel, selectedBrands, 10);
    if (process.env.NODE_ENV !== "production") {
      const t1 = performance.now();
      console.debug("[JourneyPerf] heatmap_ms", Number((t1 - t0).toFixed(2)));
    }
    return matrix;
  }, [journeyModel, selectedBrands]);

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
      setInsightsState(
        generateJourneyInsights(journeyModel, selectedBrands, "category", {
          maxItems: 50,
          focusBrandName: focusBrand,
          compareBrandName: compareBrand,
        })
      );
    });
  }, [compareBrand, focusBrand, journeyModel, selectedBrands, startInsightsTransition]);

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
    setTableState("loading");
    setTableMessage(null);
    const effectiveQuarterFrom = timeMode && selectedTimeBucket ? selectedTimeBucket : scope.quarterFrom;
    const effectiveQuarterTo = timeMode && selectedTimeBucket ? selectedTimeBucket : scope.quarterTo;
    postJourneyTableMultiDetailed(
      {
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
      },
      "all"
    )
      .then((result) => {
        if (!result.ok) {
          setTableState("error");
          const message =
            result.data && typeof result.data === "object" && "detail" in result.data
              ? String(result.data.detail)
              : result.error || "Unable to load table.";
          setTableMessage(message);
          setTableRows([]);
          return;
        }
        const data = result.data as { rows?: TableRow[] };
        setTableRows(Array.isArray(data.rows) ? data.rows : []);
        setTableState("idle");
      })
      .catch((error) => {
        setTableState("error");
        setTableMessage(error instanceof Error ? error.message : "Unable to load table.");
        setTableRows([]);
      });
  }, [scope, timeMode, selectedTimeBucket]);

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

  return (
    <main className="space-y-6">
      {advancedControlsContent && advancedSlot ? createPortal(advancedControlsContent, advancedSlot) : null}

      {tableState === "error" && (
        <section className="main-surface p-6">
          <p className="text-sm text-red-600">
            {tableMessage || "Unable to load Journey results."} Open Admin {">"} Data Validation {">"} Journey Data
            for raw validation tables.
          </p>
        </section>
      )}

      <HeroSankey
        model={journeyModel}
        selectedBrandNames={selectedBrands}
        focusBrandName={focusBrand}
        compareBrandName={compareBrand}
        timeBucketLabel={timeMode ? selectedTimeBucket : null}
      />
      <JourneyInsights insights={journeyInsights} />
      {isInsightsPending && <p className="px-2 text-xs text-slate">Updating insights...</p>}
      <JourneyHeatmapTable
        matrix={journeyHeatmap}
        focusedBrandName={focusBrand}
        onFocusBrand={(brand) => {
          setFocusBrand(brand);
          if (brand && compareBrand === brand) setCompareBrand(null);
        }}
      />
    </main>
  );
}
