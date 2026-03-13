"use client";

import { useMemo, useState } from "react";
import ReactECharts from "echarts-for-react";

import type {
  TrackingBrandMetricKey,
  TrackingMetricMeta,
  TrackingSeriesMetric,
  TrackingSeriesModel,
  TrackingTouchpointMetricKey,
} from "../types";

type TrackingChartsProps = {
  model: TrackingSeriesModel;
  entity: "primary" | "secondary";
  rowLabel: string;
};

const BRAND_METRICS: TrackingBrandMetricKey[] = [
  "brand_awareness",
  "ad_awareness",
  "brand_consideration",
  "brand_purchase",
  "brand_satisfaction",
  "brand_recommendation",
  "csat",
  "nps",
];
const TOUCHPOINT_METRICS: TrackingTouchpointMetricKey[] = ["recall", "consideration", "purchase"];

export default function TrackingCharts({
  model,
  entity,
  rowLabel,
}: TrackingChartsProps) {
  const rows: Array<{ name: string; metrics: Record<string, TrackingSeriesMetric> }> =
    entity === "primary"
      ? model.entity_rows.map((row) => ({
          name: row.entity,
          metrics: row.metrics as unknown as Record<string, TrackingSeriesMetric>,
        }))
      : model.secondary_rows.map((row) => ({
          name: row.entity,
          metrics: row.metrics as unknown as Record<string, TrackingSeriesMetric>,
        }));
  const meta: Record<string, TrackingMetricMeta> =
    entity === "primary"
      ? (model.metric_meta_brand as unknown as Record<string, TrackingMetricMeta>)
      : (model.metric_meta_touchpoint as unknown as Record<string, TrackingMetricMeta>);
  const metricOptions = entity === "primary" ? BRAND_METRICS : TOUCHPOINT_METRICS;
  const [metric, setMetric] = useState<string>(metricOptions[0]);
  const effectiveMetric = metric;

  const periods = model.periods;
  const labels = rows.map((row) => row.name);
  const unit = meta[effectiveMetric]?.unit || "%";
  const metricLabel = meta[effectiveMetric]?.label || effectiveMetric;
  const periodColors = ["#93c5fd", "#60a5fa", "#38bdf8", "#0ea5e9", "#2563eb", "#1d4ed8"];
  const deltaColors = ["#a7f3d0", "#6ee7b7", "#34d399", "#fcd34d", "#fca5a5", "#f87171"];

  const evolutionOption = useMemo(
    () => ({
      animationDuration: 300,
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      legend: { top: 0, textStyle: { color: "#5b697f" } },
      grid: { top: 34, right: 20, bottom: 90, left: 52 },
      xAxis: { type: "category", data: labels, axisLabel: { color: "#1f2a3d", rotate: 24, interval: 0 } },
      yAxis: { type: "value", axisLabel: { color: "#5b697f", formatter: `{value}${unit}` } },
      series: periods.map((period, index) => ({
        name: period.label,
        type: "bar",
        barWidth: 12,
        itemStyle: { color: periodColors[index % periodColors.length], borderRadius: 6 },
        data: rows.map((row) => {
          const value = row.metrics[effectiveMetric]?.values?.[period.key];
          return typeof value === "number" ? value : 0;
        }),
      })),
    }),
    [effectiveMetric, labels, periods, rows, unit]
  );

  const deltaOption = useMemo(
    () => ({
      animationDuration: 300,
      tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
      legend: { top: 0, textStyle: { color: "#5b697f" } },
      grid: { top: 24, right: 20, bottom: 90, left: 52 },
      xAxis: { type: "category", data: labels, axisLabel: { color: "#1f2a3d", rotate: 24, interval: 0 } },
      yAxis: { type: "value", axisLabel: { color: "#5b697f", formatter: "{value} pts" } },
      series: model.delta_columns.map((delta, index) => ({
        name: delta.label,
        type: "bar",
        barWidth: 12,
        itemStyle: { color: deltaColors[index % deltaColors.length], borderRadius: 8 },
        data: rows.map((row) => {
          const value = row.metrics[effectiveMetric]?.deltas?.[delta.key];
          return typeof value === "number" ? value : 0;
        }),
      })),
    }),
    [effectiveMetric, labels, model.delta_columns, rows]
  );

  return (
    <section className="main-surface rounded-3xl p-5">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <h3 className="text-base font-semibold text-ink">
          {rowLabel} visual comparison
        </h3>
        <label className="inline-flex items-center gap-2 text-xs text-slate">
          Metric
          <select
            className="rounded-full border border-ink/10 bg-white px-3 py-1.5 text-xs font-medium text-ink"
            value={metric}
            onChange={(event) => setMetric(event.target.value)}
          >
            {metricOptions.map((key) => (
              <option key={key} value={key}>
                {meta[key as string]?.label || key}
              </option>
            ))}
          </select>
        </label>
      </div>
      <div className="space-y-4">
        <article className="rounded-2xl border border-ink/10 bg-white p-3">
          <p className="mb-2 text-xs font-semibold uppercase tracking-[0.08em] text-slate">
            Evolucion ({metricLabel})
          </p>
          <ReactECharts option={evolutionOption} style={{ height: 360 }} notMerge lazyUpdate />
        </article>
        <article className="rounded-2xl border border-ink/10 bg-white p-3">
          <p className="mb-2 text-xs font-semibold uppercase tracking-[0.08em] text-slate">
            Delta por periodos consecutivos ({metricLabel})
          </p>
          <ReactECharts option={deltaOption} style={{ height: 360 }} notMerge lazyUpdate />
        </article>
      </div>
    </section>
  );
}
