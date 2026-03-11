"use client";

import { useEffect, useMemo, useState } from "react";
import ReactECharts from "echarts-for-react";

import type { TrackingComparisonModel, TrackingMetricKey } from "../types";

type TrackingChartsProps = {
  model: TrackingComparisonModel;
};

const METRIC_OPTIONS: TrackingMetricKey[] = [
  "brand_awareness",
  "ad_awareness",
  "brand_consideration",
  "brand_purchase",
  "brand_satisfaction",
  "brand_recommendation",
  "csat",
  "nps",
];

export default function TrackingCharts({ model }: TrackingChartsProps) {
  const defaultMetric = useMemo(
    () => METRIC_OPTIONS.find((metric) => model.metricMeta[metric].available) ?? "brand_awareness",
    [model.metricMeta]
  );
  const [metric, setMetric] = useState<TrackingMetricKey>(defaultMetric);

  useEffect(() => {
    if (model.metricMeta[metric].available) return;
    setMetric(defaultMetric);
  }, [defaultMetric, metric, model.metricMeta]);

  const rows = model.brands;
  const labels = rows.map((row) => row.brandName);
  const earlier = rows.map((row) => row.metrics[metric].valueEarlier ?? 0);
  const later = rows.map((row) => row.metrics[metric].valueLater ?? 0);
  const delta = rows.map((row) => row.metrics[metric].deltaAbs ?? 0);

  const label = model.metricMeta[metric].label;
  const unit = model.metricMeta[metric].unit;

  const dumbbellOption = useMemo(
    () => ({
      animationDuration: 300,
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        valueFormatter: (value: number) => `${value.toFixed(1)}${unit}`,
      },
      legend: { top: 0, textStyle: { color: "#5b697f" } },
      grid: { top: 34, right: 20, bottom: 90, left: 52 },
      xAxis: {
        type: "category",
        data: labels,
        axisLabel: { color: "#1f2a3d", rotate: 24, interval: 0 },
      },
      yAxis: { type: "value", axisLabel: { color: "#5b697f" } },
      series: [
        {
          name: "Pre",
          type: "bar",
          barWidth: 12,
          itemStyle: { color: "#a5b4c7", borderRadius: 6 },
          data: earlier,
        },
        {
          name: "Post",
          type: "bar",
          barWidth: 12,
          itemStyle: { color: "#1cc6e0", borderRadius: 6 },
          data: later,
        },
      ],
    }),
    [earlier, labels, later, unit]
  );

  const deltaOption = useMemo(
    () => ({
      animationDuration: 300,
      tooltip: {
        trigger: "axis",
        axisPointer: { type: "shadow" },
        valueFormatter: (value: number) => `${value.toFixed(1)} pts`,
      },
      grid: { top: 24, right: 20, bottom: 90, left: 52 },
      xAxis: {
        type: "category",
        data: labels,
        axisLabel: { color: "#1f2a3d", rotate: 24, interval: 0 },
      },
      yAxis: { type: "value", axisLabel: { color: "#5b697f" } },
      series: [
        {
          type: "bar",
          barWidth: 14,
          data: delta.map((value) => ({
            value,
            itemStyle: {
              color: value > 0 ? "#24a178" : value < 0 ? "#d86a7c" : "#9da8b7",
              borderRadius: 8,
            },
          })),
        },
      ],
    }),
    [delta, labels]
  );

  return (
    <section className="main-surface rounded-3xl p-5">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <h3 className="text-base font-semibold text-ink">Visual Comparison</h3>
        <label className="inline-flex items-center gap-2 text-xs text-slate">
          Metric
          <select
            className="rounded-full border border-ink/10 bg-white px-3 py-1.5 text-xs font-medium text-ink"
            value={metric}
            onChange={(event) => setMetric(event.target.value as TrackingMetricKey)}
          >
            {METRIC_OPTIONS.filter((key) => model.metricMeta[key].available).map((key) => (
              <option key={key} value={key}>
                {model.metricMeta[key].label}
              </option>
            ))}
          </select>
        </label>
      </div>
      <div className="space-y-4">
        <article className="rounded-2xl border border-ink/10 bg-white p-3">
          <p className="mb-2 text-xs font-semibold uppercase tracking-[0.08em] text-slate">
            Pre vs Post ({label})
          </p>
          <ReactECharts option={dumbbellOption} style={{ height: 420 }} notMerge lazyUpdate />
        </article>
        <article className="rounded-2xl border border-ink/10 bg-white p-3">
          <p className="mb-2 text-xs font-semibold uppercase tracking-[0.08em] text-slate">
            Delta by Brand ({label})
          </p>
          <ReactECharts option={deltaOption} style={{ height: 420 }} notMerge lazyUpdate />
        </article>
      </div>
    </section>
  );
}
