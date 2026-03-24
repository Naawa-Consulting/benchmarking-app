"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useSearchParams } from "next/navigation";

import { postTrackingSeriesDetailed } from "../../lib/api";
import { useScope } from "../../components/layout/ScopeProvider";
import TrackingKpiStrip from "../../features/tracking/components/TrackingKpiStrip";
import TrackingComparisonTable from "../../features/tracking/components/TrackingComparisonTable";
import TrackingCharts from "../../features/tracking/components/TrackingCharts";
import { exportTrackingXlsx } from "../../features/tracking/export/exportTrackingXlsx";
import {
  buildTrackingSeriesModel,
  filterTrackingSeriesByBrands,
} from "../../features/tracking/data/buildTrackingSeriesModel";
import type { TrackingSeriesModel } from "../../features/tracking/types";

type LoadState = "idle" | "loading" | "error";

export default function TrackingPage() {
  const searchParams = useSearchParams();
  const { scope, setTrackingBrandOptions } = useScope();

  const advancedOpen = searchParams.get("scope_advanced") === "1";
  const [advancedSlot, setAdvancedSlot] = useState<HTMLElement | null>(null);
  const [state, setState] = useState<LoadState>("idle");
  const [error, setError] = useState<string | null>(null);
  const [baseModel, setBaseModel] = useState<TrackingSeriesModel | null>(null);
  const [model, setModel] = useState<TrackingSeriesModel | null>(null);
  const [debouncedPayloadKey, setDebouncedPayloadKey] = useState("");
  const requestSeqRef = useRef(0);
  const abortRef = useRef<AbortController | null>(null);

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

  const trackingPayload = useMemo(
    () => ({
      study_ids: scope.studyIds.length ? scope.studyIds : null,
      brands: null,
      taxonomy_view: scope.taxonomyView,
      sector: scope.sector,
      subsector: scope.subsector,
      category: scope.category,
      years: scope.years.length ? scope.years : null,
      gender: scope.gender.length ? scope.gender : null,
      nse: scope.nse.length ? scope.nse : null,
      state: scope.state.length ? scope.state : null,
      age_min: scope.ageMin,
      age_max: scope.ageMax,
      date_grain: scope.timeGranularity,
    }),
    [
      scope.ageMax,
      scope.ageMin,
      scope.category,
      scope.years,
      scope.gender,
      scope.nse,
      scope.sector,
      scope.state,
      scope.studyIds,
      scope.subsector,
      scope.taxonomyView,
      scope.timeGranularity,
    ]
  );

  const payloadKey = useMemo(() => JSON.stringify(trackingPayload), [trackingPayload]);

  useEffect(() => {
    const timer = window.setTimeout(() => setDebouncedPayloadKey(payloadKey), 250);
    return () => window.clearTimeout(timer);
  }, [payloadKey]);

  useEffect(() => {
    if (!debouncedPayloadKey) return;
    const seq = requestSeqRef.current + 1;
    requestSeqRef.current = seq;
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    const load = async () => {
      setState("loading");
      setError(null);
      const payload = JSON.parse(debouncedPayloadKey);
      const result = await postTrackingSeriesDetailed(payload, { signal: controller.signal });
      if (controller.signal.aborted || seq !== requestSeqRef.current) return;
      if (!result.ok || !result.data) {
        setState("error");
        setError(result.error || "Unable to load trends series.");
        setTrackingBrandOptions(null);
        return;
      }

      const nextModel = buildTrackingSeriesModel(result.data);
      setBaseModel(nextModel);
      const scopedModel = filterTrackingSeriesByBrands(nextModel, scope.brands);
      setModel(scopedModel);
      if (process.env.NODE_ENV !== "production") {
        console.debug("[TrendsPerf] series", {
          granularity: scopedModel.resolved_granularity,
          breakdown: scopedModel.resolved_breakdown,
          periods: scopedModel.periods.length,
          primaryRows: scopedModel.entity_rows.length,
          secondaryRows: scopedModel.secondary_rows.length,
          meta: scopedModel.meta,
        });
      }
      setTrackingBrandOptions(
        nextModel.resolved_breakdown === "brand"
          ? nextModel.entity_rows.map((row) => row.entity)
          : null
      );
      setState("idle");
    };

    load().catch((err) => {
      if (controller.signal.aborted || seq !== requestSeqRef.current) return;
      setState("error");
      setError(err instanceof Error ? err.message : "Unable to load trends series.");
      setTrackingBrandOptions(null);
    });

    return () => controller.abort();
  }, [debouncedPayloadKey, setTrackingBrandOptions]);

  useEffect(() => () => setTrackingBrandOptions(null), [setTrackingBrandOptions]);

  useEffect(() => {
    if (!baseModel) {
      setModel(null);
      return;
    }
    setModel(filterTrackingSeriesByBrands(baseModel, scope.brands));
  }, [baseModel, scope.brands]);

  const advancedControlsContent =
    advancedOpen && advancedSlot ? (
      <section className="main-surface rounded-3xl p-4">
        <h3 className="text-sm font-semibold text-ink">Trends controls</h3>
        <p className="mt-1 text-xs text-slate">
          Comparacion automatica por{" "}
          <span className="font-medium text-ink">
            {model?.resolved_granularity === "quarter" ? "trimestre" : "ano"}
          </span>
          . Selecciona Sector/Subsector/Category en la barra de filtros global.
        </p>
        <p className="mt-1 text-xs text-slate">
          Desglose actual: <span className="font-medium text-ink">{model?.entity_label || "Brand"}</span>
        </p>
        {model?.meta?.warnings?.length ? (
          <p className="mt-2 text-xs text-amber-700">{model.meta.warnings[0]}</p>
        ) : null}
      </section>
    ) : null;

  const periodsLabel = useMemo(() => {
    if (!model?.periods?.length) return "-";
    return model.periods.map((item) => item.label).join(" | ");
  }, [model]);

  return (
    <main className="space-y-6">
      {advancedControlsContent && advancedSlot ? createPortal(advancedControlsContent, advancedSlot) : null}

      {state === "loading" && (
        <section className="main-surface rounded-3xl p-5">
          <p className="text-sm text-slate">Construyendo trends por periodos...</p>
        </section>
      )}

      {state === "error" && (
        <section className="main-surface rounded-3xl p-5">
          <p className="text-sm text-rose-600">{error || "Unable to load trends."}</p>
        </section>
      )}

      {model && (
        <>
          <section className="main-surface rounded-3xl p-5">
            <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
              <div>
                <h1 className="text-xl font-semibold text-ink">Trends</h1>
                <p className="text-sm text-slate">
                  {model.resolved_granularity === "year"
                    ? "Comparativa ano contra ano"
                    : "Comparativa trimestre contra trimestre"}{" "}
                  - {periodsLabel}
                </p>
                <p className="text-xs text-slate">Desglose actual: {model.entity_label}</p>
              </div>
              <button
                type="button"
                className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm transition hover:bg-slate-50"
                onClick={() => void exportTrackingXlsx(model)}
              >
                Exportar Excel
              </button>
            </div>
            <TrackingKpiStrip model={model} />
          </section>

          <TrackingCharts model={model} entity="primary" rowLabel={model.entity_label} />
          <TrackingComparisonTable model={model} entity="primary" rowLabel={model.entity_label} />

          <TrackingCharts model={model} entity="secondary" rowLabel="Touchpoint" />
          <TrackingComparisonTable model={model} entity="secondary" rowLabel="Touchpoint" />
        </>
      )}
    </main>
  );
}

