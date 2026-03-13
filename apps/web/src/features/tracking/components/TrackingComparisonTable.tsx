"use client";

import { useMemo, useState } from "react";

import type {
  TrackingBrandMetricKey,
  TrackingMetricMeta,
  TrackingSeriesMetric,
  TrackingSeriesModel,
  TrackingTouchpointMetricKey,
} from "../types";

type TrackingComparisonTableProps = {
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

function percentile(values: number[], p: number) {
  if (!values.length) return 0;
  const sorted = values.slice().sort((a, b) => a - b);
  const index = (sorted.length - 1) * p;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return sorted[lower];
  const weight = index - lower;
  return sorted[lower] * (1 - weight) + sorted[upper] * weight;
}

function heatmapClass(value: number | null, p10: number, p90: number) {
  if (value == null) return "bg-white";
  const floor = Math.min(p10, 0);
  const ceil = Math.max(p90, 0);
  const span = Math.max(1e-6, ceil - floor);
  const normalized = (value - floor) / span;
  if (Math.abs(value) < span * 0.08) return "bg-slate-50";
  if (value > 0) {
    if (normalized > 0.85) return "bg-emerald-100";
    if (normalized > 0.65) return "bg-emerald-50";
    return "bg-emerald-50/60";
  }
  if (normalized < 0.15) return "bg-rose-100";
  if (normalized < 0.35) return "bg-rose-50";
  return "bg-rose-50/60";
}

function fmt(value: number | null, unit: string) {
  if (value == null) return "-";
  return `${value.toFixed(1)}${unit}`;
}

function fmtDelta(value: number | null) {
  if (value == null) return "-";
  return `${value > 0 ? "+" : ""}${value.toFixed(1)}`;
}

export default function TrackingComparisonTable({ model, entity, rowLabel }: TrackingComparisonTableProps) {
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

  const metricOptions = entity === "primary" ? BRAND_METRICS : TOUCHPOINT_METRICS;
  const meta: Record<string, TrackingMetricMeta> =
    entity === "primary"
      ? (model.metric_meta_brand as unknown as Record<string, TrackingMetricMeta>)
      : (model.metric_meta_touchpoint as unknown as Record<string, TrackingMetricMeta>);
  const [metric, setMetric] = useState<string>(metricOptions[0]);
  const effectiveMetric = metric;

  const deltaValues = useMemo(
    () =>
      rows.flatMap((row) =>
        model.delta_columns
          .map((delta) => row.metrics[effectiveMetric]?.deltas?.[delta.key])
          .filter((value): value is number => typeof value === "number")
      ),
    [effectiveMetric, model.delta_columns, rows]
  );
  const p10 = percentile(deltaValues, 0.1);
  const p90 = percentile(deltaValues, 0.9);
  const title = `${rowLabel} comparison`;

  return (
    <section className="main-surface rounded-3xl p-5">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-base font-semibold text-ink">{title}</h3>
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
      <div className="overflow-auto rounded-2xl border border-ink/10">
        <table className="min-w-[980px] border-collapse text-xs">
          <thead className="sticky top-0 z-20 bg-[#f7f8fa]">
            <tr>
              <th className="sticky left-0 z-30 border-b border-r border-ink/10 bg-[#f7f8fa] px-3 py-2 text-left text-[11px] font-semibold uppercase tracking-[0.08em] text-slate">
                {rowLabel}
              </th>
              {model.periods.map((period) => (
                <th key={period.key} className="border-b border-r border-ink/10 px-2 py-2 text-center text-[11px] font-semibold uppercase tracking-[0.08em] text-slate">
                  {period.label}
                </th>
              ))}
              {model.delta_columns.map((delta) => (
                <th key={delta.key} className="border-b border-r border-ink/10 px-2 py-2 text-center text-[11px] font-semibold uppercase tracking-[0.08em] text-slate">
                  {delta.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => {
              const metricData = row.metrics[effectiveMetric];
              return (
                <tr key={row.name} className="border-b border-ink/5">
                  <td className="sticky left-0 z-10 border-r border-ink/10 bg-white px-3 py-2 font-medium text-ink">
                    {row.name}
                  </td>
                  {model.periods.map((period) => (
                    <td key={`${row.name}-${period.key}`} className="border-r border-ink/10 px-2 py-2 text-right text-slate">
                      {fmt(
                        (metricData?.values?.[period.key] as number | null | undefined) ?? null,
                        meta[effectiveMetric]?.unit || "%"
                      )}
                    </td>
                  ))}
                  {model.delta_columns.map((delta) => {
                    const value = (metricData?.deltas?.[delta.key] as number | null | undefined) ?? null;
                    return (
                      <td
                        key={`${row.name}-${delta.key}`}
                        className={`border-r border-ink/10 px-2 py-2 text-right ${heatmapClass(value, p10, p90)}`}
                        title={value == null ? "No data" : `Delta: ${value.toFixed(1)}`}
                      >
                        {fmtDelta(value)}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </section>
  );
}
