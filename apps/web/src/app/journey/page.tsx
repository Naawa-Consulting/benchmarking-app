"use client";

import Link from "next/link";
import { useEffect, useMemo, useState, useTransition } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import * as Popover from "@radix-ui/react-popover";
import { postJourneyTableMultiDetailed, postTouchpointsTableMultiDetailed } from "../../lib/api";
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

type TouchpointRow = {
  study_id: string;
  study_name?: string | null;
  sector: string | null;
  subsector: string | null;
  category: string | null;
  brand: string;
  touchpoint: string;
  recall: number | null;
};

const firstOrNull = (values: string[]) => (values.length ? values[0] : null);
const TIME_FIELD_CANDIDATES = ["quarter", "time", "wave", "period", "month", "year", "date"];
const getRowTimeValue = (row: TableRow): string | null => {
  for (const key of TIME_FIELD_CANDIDATES) {
    const value = row[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return null;
};

export default function JourneyPage() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { scope } = useScope();
  const [limitMode, setLimitMode] = useState<"top10" | "top25" | "all">("top10");
  const [touchpointLimit, setTouchpointLimit] = useState<"top10" | "top25" | "all">("top25");
  const [includeAdAwareness, setIncludeAdAwareness] = useState(true);
  const [brandsOpen, setBrandsOpen] = useState(false);
  const [brandSearch, setBrandSearch] = useState("");
  const [selectedBrands, setSelectedBrands] = useState<string[]>([]);
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
  const [tableSource, setTableSource] = useState<string | null>(null);
  const [touchpointRows, setTouchpointRows] = useState<TouchpointRow[]>([]);
  const [touchpointState, setTouchpointState] = useState<"idle" | "loading" | "error">("idle");
  const [touchpointMessage, setTouchpointMessage] = useState<string | null>(null);

  const availableBrands = useMemo(
    () => Array.from(new Set(tableRows.map((row) => row.brand).filter(Boolean))).sort(),
    [tableRows]
  );
  const filteredBrands = useMemo(() => {
    const needle = brandSearch.trim().toLowerCase();
    if (!needle) return availableBrands;
    return availableBrands.filter((brand) => brand.toLowerCase().includes(needle));
  }, [availableBrands, brandSearch]);
  const timeBuckets = useMemo(
    () =>
      Array.from(
        new Set(
          tableRows
            .map((row) => getRowTimeValue(row))
            .filter((value): value is string => typeof value === "string" && value.length > 0)
        )
      ).sort(),
    [tableRows]
  );

  useEffect(() => {
    const includeAd = searchParams.get("include_ad_awareness");
    const parsedIncludeAd = includeAd == null ? true : includeAd !== "0";
    setIncludeAdAwareness(parsedIncludeAd);
    const queryBrands = searchParams.get("journey_brands");
    if (queryBrands) {
      setSelectedBrands(
        queryBrands
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean)
      );
      return;
    }
    if (scope.brands.length) {
      setSelectedBrands(scope.brands);
    }
  }, [scope.brands, searchParams]);

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

  const updateJourneyQuery = useMemo(
    () => (nextBrands: string[], nextIncludeAdAwareness: boolean) => {
      const params = new URLSearchParams(searchParams.toString());
      if (nextBrands.length) params.set("journey_brands", nextBrands.join(","));
      else params.delete("journey_brands");
      if (!nextIncludeAdAwareness) params.set("include_ad_awareness", "0");
      else params.delete("include_ad_awareness");
      router.replace(`${pathname}${params.toString() ? `?${params.toString()}` : ""}`, { scroll: false });
    },
    [pathname, router, searchParams]
  );

  useEffect(() => {
    if (!availableBrands.length) return;
    const set = new Set(availableBrands);
    const valid = selectedBrands.filter((brand) => set.has(brand));
    if (valid.length === selectedBrands.length) return;
    setSelectedBrands(valid);
    updateJourneyQuery(valid, includeAdAwareness);
  }, [availableBrands, includeAdAwareness, selectedBrands, updateJourneyQuery]);

  const journeyModel = useMemo(
    () => {
      const t0 = performance.now();
      const model = buildJourneyModel(
        timeMode && selectedTimeBucket
          ? tableRows.filter((row) => getRowTimeValue(row) === selectedTimeBucket)
          : tableRows,
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
        { includeAdAwareness, benchmarkScope: "category" }
      );
      if (process.env.NODE_ENV !== "production") {
        const t1 = performance.now();
        console.debug("[JourneyPerf] model_ms", Number((t1 - t0).toFixed(2)));
      }
      return model;
    },
    [includeAdAwareness, scope, selectedTimeBucket, tableRows, timeMode]
  );
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
          maxItems: 8,
          focusBrandName: focusBrand,
          compareBrandName: compareBrand,
        })
      );
    });
  }, [compareBrand, focusBrand, journeyModel, selectedBrands, startInsightsTransition]);

  useEffect(() => {
    if (!focusBrand) return;
    if (selectedBrands.includes(focusBrand)) return;
    const next = [...selectedBrands, focusBrand];
    setSelectedBrands(next);
    updateJourneyQuery(next, includeAdAwareness);
  }, [focusBrand, includeAdAwareness, selectedBrands, updateJourneyQuery]);

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
    // Dev summary for Sprint 1 data layer validation.
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
        quarter_from: scope.quarterFrom,
        quarter_to: scope.quarterTo,
      },
      limitMode
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
        setTableSource("curated");
        setTableState("idle");
      })
      .catch((error) => {
        setTableState("error");
        setTableMessage(error instanceof Error ? error.message : "Unable to load table.");
        setTableRows([]);
      });
  }, [scope, limitMode]);

  useEffect(() => {
    setTouchpointState("loading");
    setTouchpointMessage(null);
    postTouchpointsTableMultiDetailed(
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
        quarter_from: scope.quarterFrom,
        quarter_to: scope.quarterTo,
      },
      touchpointLimit
    )
      .then((result) => {
        if (!result.ok) {
          setTouchpointState("error");
          const message =
            result.data && typeof result.data === "object" && "detail" in result.data
              ? String(result.data.detail)
              : result.error || "Unable to load touchpoints.";
          setTouchpointMessage(message);
          setTouchpointRows([]);
          return;
        }
        const data = result.data as { rows?: TouchpointRow[] };
        setTouchpointRows(Array.isArray(data.rows) ? data.rows : []);
        setTouchpointState("idle");
      })
      .catch((error) => {
        setTouchpointState("error");
        setTouchpointMessage(error instanceof Error ? error.message : "Unable to load touchpoints.");
        setTouchpointRows([]);
      });
  }, [scope, touchpointLimit]);

  return (
    <main className="space-y-6">
      <section className="main-surface p-6">
        <div className="flex flex-wrap items-center gap-2">
          <Popover.Root open={brandsOpen} onOpenChange={setBrandsOpen}>
            <Popover.Trigger asChild>
              <button
                type="button"
                className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm transition hover:bg-slate-50"
                aria-label="Journey brands"
              >
                Brands: {selectedBrands.length === 0 ? "All" : selectedBrands.length}
              </button>
            </Popover.Trigger>
            <Popover.Portal>
              <Popover.Content
                side="bottom"
                align="start"
                sideOffset={8}
                avoidCollisions
                className="z-[80] w-[320px] rounded-2xl border border-ink/10 bg-white p-3 shadow-xl"
              >
                <div className="space-y-3">
                  <div className="flex items-center justify-between text-[11px]">
                    <button
                      type="button"
                      className="text-[#008a67] hover:underline"
                      onClick={() => {
                        setSelectedBrands(filteredBrands);
                        updateJourneyQuery(filteredBrands, includeAdAwareness);
                      }}
                    >
                      Select all
                    </button>
                    <button
                      type="button"
                      className="text-slate hover:underline"
                      onClick={() => {
                        setSelectedBrands([]);
                        updateJourneyQuery([], includeAdAwareness);
                      }}
                    >
                      Clear
                    </button>
                  </div>
                  <input
                    className="w-full rounded-xl border border-ink/10 px-3 py-2 text-xs"
                    placeholder="Search brands"
                    value={brandSearch}
                    onChange={(event) => setBrandSearch(event.target.value)}
                  />
                  <div className="max-h-[260px] space-y-1 overflow-auto pr-1 text-xs">
                    {filteredBrands.map((brand) => (
                      <label key={brand} className="flex items-center gap-2 rounded-lg px-1 py-1 hover:bg-slate-50">
                        <input
                          type="checkbox"
                          checked={selectedBrands.includes(brand)}
                          onChange={() => {
                            const next = selectedBrands.includes(brand)
                              ? selectedBrands.filter((item) => item !== brand)
                              : [...selectedBrands, brand];
                            setSelectedBrands(next);
                            updateJourneyQuery(next, includeAdAwareness);
                          }}
                        />
                        <span>{brand}</span>
                      </label>
                    ))}
                  </div>
                </div>
              </Popover.Content>
            </Popover.Portal>
          </Popover.Root>
          <button
            type="button"
            className={`rounded-full border px-3 py-2 text-xs ${
              includeAdAwareness
                ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
                : "border-ink/10 bg-white text-slate"
            }`}
            onClick={() => {
              const next = !includeAdAwareness;
              setIncludeAdAwareness(next);
              updateJourneyQuery(selectedBrands, next);
            }}
          >
            Include Ad Awareness: {includeAdAwareness ? "On" : "Off"}
          </button>
        </div>
      </section>

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

      <HeroSankey
        model={journeyModel}
        selectedBrandNames={selectedBrands}
        focusBrandName={focusBrand}
        compareBrandName={compareBrand}
        timeBucketLabel={timeMode ? selectedTimeBucket : null}
      />
      <JourneyInsights insights={journeyInsights} />
      {isInsightsPending && (
        <p className="px-2 text-xs text-slate">Updating insights...</p>
      )}
      <JourneyHeatmapTable
        matrix={journeyHeatmap}
        focusedBrandName={focusBrand}
        onFocusBrand={(brand) => {
          setFocusBrand(brand);
          if (brand && compareBrand === brand) setCompareBrand(null);
        }}
      />

      <section className="main-surface p-6">
        <div className="flex items-center justify-between">
          <h3 className="text-xl font-semibold">Journey Results</h3>
          {tableSource && (
            <span className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-3 py-1 text-xs text-emerald-700">
              source: {tableSource}
            </span>
          )}
        </div>
        <div className="mt-4 flex flex-wrap items-center gap-2 text-xs">
          <span className="text-slate">View:</span>
          {(["top10", "top25", "all"] as const).map((mode) => (
            <button
              key={mode}
              type="button"
              onClick={() => setLimitMode(mode)}
              className={`rounded-full border px-3 py-1 ${
                limitMode === mode
                  ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
                  : "border-ink/10 text-slate"
              }`}
            >
              {mode === "top10" ? "Top 10" : mode === "top25" ? "Top 25" : "All"}
            </button>
          ))}
          <span className="text-[10px] text-slate">Sorted by Brand Awareness (desc)</span>
        </div>
        {tableState === "loading" && <p className="mt-4 text-sm text-slate">Loading curated results...</p>}
        {tableState === "error" && (
          <p className="mt-4 text-sm text-red-600">
            {tableMessage || "Results not published for this study yet. Go to Admin → Publish Journey Results."}{" "}
            <Link className="underline" href="/admin">
              Open Admin
            </Link>
          </p>
        )}
        {tableState !== "error" && tableRows.length > 0 && (
          <div className="mt-4 max-h-[520px] overflow-auto">
            <table className="w-full border-collapse text-sm">
              <thead>
                <tr className="border-b border-ink/10 text-left">
                  <th className="py-2 font-semibold">Study</th>
                  <th className="py-2 font-semibold">Sector</th>
                  <th className="py-2 font-semibold">Subsector</th>
                  <th className="py-2 font-semibold">Category</th>
                  <th className="py-2 font-semibold">Brand</th>
                  <th className="py-2 text-right font-semibold">Brand Awareness</th>
                  <th className="py-2 text-right font-semibold">Ad Awareness</th>
                  <th className="py-2 text-right font-semibold">Brand Consideration</th>
                  <th className="py-2 text-right font-semibold">Brand Purchase</th>
                  <th className="py-2 text-right font-semibold">Brand Satisfaction</th>
                  <th className="py-2 text-right font-semibold">Brand Recommendation</th>
                </tr>
              </thead>
              <tbody>
                {tableRows.map((row, index) => (
                  <tr key={`${row.study_id}-${row.brand}-${index}`} className="border-b border-ink/5">
                    <td className="py-2">{row.study_id}</td>
                    <td className="py-2">{row.sector || "--"}</td>
                    <td className="py-2">{row.subsector || "--"}</td>
                    <td className="py-2">{row.category || "--"}</td>
                    <td className="py-2">{row.brand}</td>
                    <td className="py-2 text-right">
                      {row.brand_awareness === null || row.brand_awareness === undefined
                        ? "--"
                        : `${row.brand_awareness}%`}
                    </td>
                    <td className="py-2 text-right">
                      {row.ad_awareness === null || row.ad_awareness === undefined ? "--" : `${row.ad_awareness}%`}
                    </td>
                    <td className="py-2 text-right">
                      {row.brand_consideration === null || row.brand_consideration === undefined
                        ? "--"
                        : `${row.brand_consideration}%`}
                    </td>
                    <td className="py-2 text-right">
                      {row.brand_purchase === null || row.brand_purchase === undefined
                        ? "--"
                        : `${row.brand_purchase}%`}
                    </td>
                    <td className="py-2 text-right">
                      {row.brand_satisfaction === null || row.brand_satisfaction === undefined
                        ? "--"
                        : `${row.brand_satisfaction}%`}
                    </td>
                    <td className="py-2 text-right">
                      {row.brand_recommendation === null || row.brand_recommendation === undefined
                        ? "--"
                        : `${row.brand_recommendation}%`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {tableState !== "error" && tableRows.length === 0 && (
          <p className="mt-4 text-sm text-slate">
            Results not published for this study yet. Go to Admin → Publish Journey Results.{" "}
            <Link className="underline" href="/admin">
              Open Admin
            </Link>
          </p>
        )}
      </section>

      <section className="main-surface p-6">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-xl font-semibold">Touchpoints (Recall)</h3>
            <p className="text-sm text-slate">Recall = % value == 1 within touchpoints module.</p>
          </div>
        </div>
        <div className="mt-4 flex flex-wrap items-center gap-2 text-xs">
          <span className="text-slate">View:</span>
          {(["top10", "top25", "all"] as const).map((mode) => (
            <button
              key={mode}
              type="button"
              onClick={() => setTouchpointLimit(mode)}
              className={`rounded-full border px-3 py-1 ${
                touchpointLimit === mode
                  ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
                  : "border-ink/10 text-slate"
              }`}
            >
              {mode === "top10" ? "Top 10" : mode === "top25" ? "Top 25" : "All"}
            </button>
          ))}
          <span className="text-[10px] text-slate">Sorted by Recall (desc)</span>
        </div>
        {touchpointState === "loading" && <p className="mt-4 text-sm text-slate">Loading touchpoints...</p>}
        {touchpointState === "error" && <p className="mt-4 text-sm text-red-600">{touchpointMessage}</p>}
        {touchpointState !== "error" && touchpointRows.length > 0 && (
          <div className="mt-4 max-h-[520px] overflow-auto">
            <table className="w-full border-collapse text-sm">
              <thead>
                <tr className="border-b border-ink/10 text-left">
                  <th className="py-2 font-semibold">Study</th>
                  <th className="py-2 font-semibold">Sector</th>
                  <th className="py-2 font-semibold">Subsector</th>
                  <th className="py-2 font-semibold">Category</th>
                  <th className="py-2 font-semibold">Brand</th>
                  <th className="py-2 font-semibold">Touchpoint</th>
                  <th className="py-2 text-right font-semibold">Recall %</th>
                </tr>
              </thead>
              <tbody>
                {touchpointRows.map((row, index) => (
                  <tr key={`${row.study_id}-${row.brand}-${row.touchpoint}-${index}`} className="border-b border-ink/5">
                    <td className="py-2">{row.study_id}</td>
                    <td className="py-2">{row.sector || "--"}</td>
                    <td className="py-2">{row.subsector || "--"}</td>
                    <td className="py-2">{row.category || "--"}</td>
                    <td className="py-2">{row.brand}</td>
                    <td className="py-2">{row.touchpoint}</td>
                    <td className="py-2 text-right">
                      {row.recall === null || row.recall === undefined ? "--" : `${row.recall}%`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {touchpointState !== "error" && touchpointRows.length === 0 && (
          <p className="mt-4 text-sm text-slate">
            No touchpoint data available yet. Map touchpoints rules and publish curated to see results.
          </p>
        )}
      </section>
    </main>
  );
}
