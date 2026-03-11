"use client";

import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

import { getFilterDateOptionsDetailed, postJourneyTableMultiDetailed } from "../../lib/api";
import { useScope } from "../../components/layout/ScopeProvider";
import { buildJourneyModel } from "../../features/journey/data/journeyDerived";
import {
  buildMultiDemoPayloads,
  buildTrackingComparison,
} from "../../features/tracking/data/buildTrackingComparison";
import TrackingStudyPicker from "../../features/tracking/components/TrackingStudyPicker";
import TrackingKpiStrip from "../../features/tracking/components/TrackingKpiStrip";
import TrackingComparisonTable from "../../features/tracking/components/TrackingComparisonTable";
import TrackingCharts from "../../features/tracking/components/TrackingCharts";
import { exportTrackingXlsx } from "../../features/tracking/export/exportTrackingXlsx";

type JourneyTableRow = Record<string, unknown>;

type StudyTimeInfo = {
  studyId: string;
  lastTimeOrder: number | null;
  lastTimeLabel: string | null;
};

const MAX_DEMO_COMBINATIONS = 24;

function parseTimeOrder(value: string): number | null {
  const normalized = value.trim();
  if (!normalized) return null;

  const yearQuarter = normalized.match(/(\d{4})\D*Q([1-4])/i);
  if (yearQuarter) return Number(yearQuarter[1]) * 10 + Number(yearQuarter[2]);

  const quarterYear = normalized.match(/Q([1-4])\D*(\d{4})/i);
  if (quarterYear) return Number(quarterYear[2]) * 10 + Number(quarterYear[1]);

  const yearMonth = normalized.match(/(\d{4})[-/](\d{1,2})/);
  if (yearMonth) return Number(yearMonth[1]) * 100 + Number(yearMonth[2]);

  const timestamp = Date.parse(normalized);
  if (!Number.isNaN(timestamp)) return timestamp;
  return null;
}

function resolveStudyTimeInfo(studyId: string, quarters: string[]): StudyTimeInfo {
  const withOrder = quarters
    .map((label) => ({ label, order: parseTimeOrder(label) }))
    .filter((item): item is { label: string; order: number } => item.order != null)
    .sort((a, b) => a.order - b.order);
  if (!withOrder.length) return { studyId, lastTimeOrder: null, lastTimeLabel: null };
  const latest = withOrder[withOrder.length - 1];
  return { studyId, lastTimeOrder: latest.order, lastTimeLabel: latest.label };
}

export default function TrackingPage() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { scope, setScope, studies, setTrackingBrandOptions } = useScope();

  const advancedOpen = searchParams.get("scope_advanced") === "1";
  const [advancedSlot, setAdvancedSlot] = useState<HTMLElement | null>(null);
  const [studyA, setStudyAState] = useState<string | null>(searchParams.get("study_a"));
  const [studyB, setStudyBState] = useState<string | null>(searchParams.get("study_b"));
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [comparisonModel, setComparisonModel] = useState<ReturnType<typeof buildTrackingComparison> | null>(null);
  const [orderWarning, setOrderWarning] = useState<string | null>(null);
  const [scopeHint, setScopeHint] = useState<string | null>(null);

  useEffect(() => {
    setStudyAState(searchParams.get("study_a"));
    setStudyBState(searchParams.get("study_b"));
  }, [searchParams]);

  useEffect(() => {
    if (typeof document === "undefined") return;
    if (!advancedOpen) {
      setAdvancedSlot(null);
      return;
    }
    const syncSlot = () => setAdvancedSlot(document.getElementById("tracking-advanced-controls-slot"));
    syncSlot();
    const frame = window.requestAnimationFrame(syncSlot);
    return () => window.cancelAnimationFrame(frame);
  }, [advancedOpen]);

  useEffect(() => {
    const params = new URLSearchParams(searchParams.toString());
    if (studyA) params.set("study_a", studyA);
    else params.delete("study_a");
    if (studyB) params.set("study_b", studyB);
    else params.delete("study_b");
    const current = searchParams.toString();
    const next = params.toString();
    if (current !== next) {
      router.replace(`${pathname}${next ? `?${next}` : ""}`, { scroll: false });
    }
  }, [pathname, router, searchParams, studyA, studyB]);

  const selectedStudyLabels = useMemo(() => {
    const byId = new Map(studies.map((study) => [study.study_id, study.study_name || study.study_id]));
    return {
      a: studyA ? byId.get(studyA) || studyA : null,
      b: studyB ? byId.get(studyB) || studyB : null,
    };
  }, [studies, studyA, studyB]);

  const selectedStudyA = useMemo(
    () => studies.find((study) => study.study_id === studyA) || null,
    [studies, studyA]
  );

  const studiesForB = useMemo(() => {
    if (!selectedStudyA?.category) return studies;
    return studies.filter((study) => study.category === selectedStudyA.category);
  }, [selectedStudyA, studies]);

  useEffect(() => {
    if (!selectedStudyA) return;
    if (!selectedStudyA.sector || !selectedStudyA.subsector || !selectedStudyA.category) {
      setScopeHint("Base A sin taxonomia completa; no se forzo Sector/Subsector/Category.");
      return;
    }
    if (
      scope.sector === selectedStudyA.sector &&
      scope.subsector === selectedStudyA.subsector &&
      scope.category === selectedStudyA.category
    ) {
      return;
    }
    setScope({
      sector: selectedStudyA.sector,
      subsector: selectedStudyA.subsector,
      category: selectedStudyA.category,
    });
    setScopeHint("Scope alineado con Base A (Sector/Subsector/Category).");
    const timer = window.setTimeout(() => setScopeHint(null), 3000);
    return () => window.clearTimeout(timer);
  }, [scope.category, scope.sector, scope.subsector, selectedStudyA, setScope]);

  const setStudyA = (nextStudyId: string | null) => {
    setStudyAState(nextStudyId);
    if (!nextStudyId) return;
    const nextA = studies.find((study) => study.study_id === nextStudyId);
    if (!nextA?.category || !studyB) return;
    const validB = studies.some((study) => study.study_id === studyB && study.category === nextA.category);
    if (!validB) {
      setStudyBState(null);
      setScopeHint("Base B se limpio porque no coincide con la categoria de Base A.");
    }
  };

  useEffect(() => {
    if (!studyA || !studyB || studyA === studyB) {
      setComparisonModel(null);
      setLoading(false);
      setError(null);
      setOrderWarning(null);
      setTrackingBrandOptions(null);
      return;
    }

    let canceled = false;

    const load = async () => {
      setLoading(true);
      setError(null);
      setOrderWarning(null);
      try {
        const demoPayloads = buildMultiDemoPayloads({
          gender: scope.gender,
          nse: scope.nse,
          state: scope.state,
        });
        if (demoPayloads.length > MAX_DEMO_COMBINATIONS) {
          throw new Error("Demasiadas combinaciones demo; refine filtros.");
        }

        const [timeAResult, timeBResult] = await Promise.all([
          getFilterDateOptionsDetailed([studyA]),
          getFilterDateOptionsDetailed([studyB]),
        ]);
        const timeA =
          timeAResult.ok && timeAResult.data && typeof timeAResult.data === "object"
            ? resolveStudyTimeInfo(
                studyA,
                Array.isArray((timeAResult.data as { quarters?: string[] }).quarters)
                  ? (timeAResult.data as { quarters?: string[] }).quarters || []
                  : []
              )
            : { studyId: studyA, lastTimeOrder: null, lastTimeLabel: null };
        const timeB =
          timeBResult.ok && timeBResult.data && typeof timeBResult.data === "object"
            ? resolveStudyTimeInfo(
                studyB,
                Array.isArray((timeBResult.data as { quarters?: string[] }).quarters)
                  ? (timeBResult.data as { quarters?: string[] }).quarters || []
                  : []
              )
            : { studyId: studyB, lastTimeOrder: null, lastTimeLabel: null };

        const requestForStudy = async (studyId: string) => {
          const results = await Promise.all(
            demoPayloads.map((demoPayload) =>
              postJourneyTableMultiDetailed(
                {
                  study_ids: [studyId],
                  sector: scope.sector,
                  subsector: scope.subsector,
                  category: scope.category,
                  gender: demoPayload.gender,
                  nse: demoPayload.nse,
                  state: demoPayload.state,
                  age_min: scope.ageMin,
                  age_max: scope.ageMax,
                  date_grain: scope.timeGranularity,
                  quarter_from: scope.quarterFrom,
                  quarter_to: scope.quarterTo,
                },
                "all"
              )
            )
          );

          const failed = results.find((result) => !result.ok);
          if (failed) throw new Error(failed.error || "Unable to load study payloads.");

          const mergedRows = results.flatMap((result) => ((result.data as { rows?: JourneyTableRow[] })?.rows || []));
          return mergedRows;
        };

        const [rowsA, rowsB] = await Promise.all([requestForStudy(studyA), requestForStudy(studyB)]);
        const includeAdAwareness = searchParams.get("include_ad_awareness") !== "0";
        const modelA = buildJourneyModel(rowsA, null, { includeAdAwareness, benchmarkScope: "category" });
        const modelB = buildJourneyModel(rowsB, null, { includeAdAwareness, benchmarkScope: "category" });

        const brandsA = new Set(modelA.brandStageAggregates.map((brand) => brand.brandName));
        const brandsB = new Set(modelB.brandStageAggregates.map((brand) => brand.brandName));
        const trackingBrandIntersection = Array.from(brandsA).filter((brandName) => brandsB.has(brandName)).sort((a, b) => a.localeCompare(b));
        setTrackingBrandOptions(trackingBrandIntersection);

        let pre = { studyId: studyA, label: selectedStudyLabels.a || studyA, model: modelA, time: timeA };
        let post = { studyId: studyB, label: selectedStudyLabels.b || studyB, model: modelB, time: timeB };
        if (timeA.lastTimeOrder != null && timeB.lastTimeOrder != null && timeA.lastTimeOrder > timeB.lastTimeOrder) {
          pre = { studyId: studyB, label: selectedStudyLabels.b || studyB, model: modelB, time: timeB };
          post = { studyId: studyA, label: selectedStudyLabels.a || studyA, model: modelA, time: timeA };
        }
        if (timeA.lastTimeOrder == null || timeB.lastTimeOrder == null) {
          setOrderWarning("Orden temporal no verificable con fecha mapeada. Se respeta Base A -> Base B.");
        }

        const comparison = buildTrackingComparison(
          { studyId: pre.studyId, studyLabel: pre.label, model: pre.model },
          { studyId: post.studyId, studyLabel: post.label, model: post.model },
          {
            includeAdAwareness,
            activeFiltersSummary: {
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
          }
        );

        const filteredByScopeBrands = scope.brands.length
          ? comparison.brands.filter((brand) => scope.brands.includes(brand.brandName))
          : comparison.brands;
        if (!canceled) {
          setComparisonModel({ ...comparison, brands: filteredByScopeBrands });
        }
      } catch (err) {
        if (!canceled) {
          setTrackingBrandOptions(null);
          setComparisonModel(null);
          setError(err instanceof Error ? err.message : "Unable to build tracking comparison.");
        }
      } finally {
        if (!canceled) setLoading(false);
      }
    };

    load();
    return () => {
      canceled = true;
    };
  }, [
    scope.ageMax,
    scope.ageMin,
    scope.brands,
    scope.category,
    scope.gender,
    scope.nse,
    scope.quarterFrom,
    scope.quarterTo,
    scope.sector,
    scope.state,
    scope.subsector,
    scope.timeGranularity,
    searchParams,
    selectedStudyLabels.a,
    selectedStudyLabels.b,
    setTrackingBrandOptions,
    studyA,
    studyB,
  ]);

  useEffect(() => () => setTrackingBrandOptions(null), [setTrackingBrandOptions]);

  const advancedControlsContent =
    advancedOpen && advancedSlot ? (
      <TrackingStudyPicker
        studies={studies}
        studiesForB={studiesForB}
        studyA={studyA}
        studyB={studyB}
        onStudyAChange={setStudyA}
        onStudyBChange={setStudyBState}
        orderedLabels={comparisonModel ? { pre: comparisonModel.preLabel, post: comparisonModel.postLabel } : null}
        orderingWarning={orderWarning}
        scopeHint={scopeHint}
      />
    ) : null;

  return (
    <main className="space-y-6">
      {advancedControlsContent && advancedSlot ? createPortal(advancedControlsContent, advancedSlot) : null}

      {studyA && studyB && studyA === studyB && (
        <section className="main-surface rounded-3xl p-5">
          <p className="text-sm text-rose-600">Base A y Base B deben ser diferentes.</p>
        </section>
      )}

      {loading && (
        <section className="main-surface rounded-3xl p-5">
          <p className="text-sm text-slate">Construyendo comparativo...</p>
        </section>
      )}

      {error && (
        <section className="main-surface rounded-3xl p-5">
          <p className="text-sm text-rose-600">{error}</p>
        </section>
      )}

      {comparisonModel && (
        <>
          {comparisonModel.brands.length === 0 && (
            <section className="main-surface rounded-3xl p-5">
              <p className="text-sm text-slate">
                No hay marcas compartidas entre Pre y Post para los filtros actuales.
              </p>
            </section>
          )}

          <section className="main-surface rounded-3xl p-5">
            <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
              <div>
                <h1 className="text-xl font-semibold text-ink">Tracking</h1>
                <p className="text-sm text-slate">
                  Pre: <span className="font-medium text-ink">{comparisonModel.preLabel}</span> | Post:{" "}
                  <span className="font-medium text-ink">{comparisonModel.postLabel}</span>
                </p>
              </div>
              <button
                type="button"
                className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm transition hover:bg-slate-50"
                onClick={() => void exportTrackingXlsx(comparisonModel)}
              >
                Exportar Excel
              </button>
            </div>
            {comparisonModel.warnings.length > 0 && (
              <div className="mb-4 rounded-xl border border-amber-300/70 bg-amber-50 p-3 text-xs text-amber-900">
                {comparisonModel.warnings.slice(0, 3).map((warning) => (
                  <p key={warning}>{warning}</p>
                ))}
              </div>
            )}
            <TrackingKpiStrip model={comparisonModel} />
          </section>

          {comparisonModel.brands.length > 0 && (
            <>
              <TrackingCharts model={comparisonModel} />
              <TrackingComparisonTable model={comparisonModel} />
            </>
          )}
        </>
      )}
    </main>
  );
}
