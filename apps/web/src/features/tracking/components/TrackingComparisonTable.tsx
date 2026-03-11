import { Fragment, useMemo, useState } from "react";

import type { TrackingBrandRow, TrackingComparisonModel, TrackingMetricKey } from "../types";

type SortField = "valueEarlier" | "valueLater" | "deltaAbs" | "deltaRelPct";
type SortState = {
  metric: TrackingMetricKey;
  field: SortField;
  dir: "asc" | "desc";
};

type TrackingComparisonTableProps = {
  model: TrackingComparisonModel;
};

const SORT_ICON = "\u2195";

const METRIC_GROUPS: Array<{ key: TrackingMetricKey; full: string }> = [
  { key: "brand_awareness", full: "Brand Awareness" },
  { key: "ad_awareness", full: "Ad Awareness" },
  { key: "brand_consideration", full: "Brand Consideration" },
  { key: "brand_purchase", full: "Brand Purchase" },
  { key: "brand_satisfaction", full: "Brand Satisfaction" },
  { key: "brand_recommendation", full: "Brand Recommendation" },
  { key: "csat", full: "CSAT" },
  { key: "nps", full: "NPS" },
];

function formatMetric(value: number | null, unit: "%" | "pts") {
  if (value == null) return "-";
  return `${value.toFixed(1)}${unit}`;
}

function formatDeltaPts(value: number | null) {
  if (value == null) return "-";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}`;
}

function formatDeltaPct(value: number | null) {
  if (value == null) return "-";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)}%`;
}

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

function sortRows(rows: TrackingBrandRow[], sort: SortState) {
  return rows.slice().sort((a, b) => {
    const left = a.metrics[sort.metric][sort.field];
    const right = b.metrics[sort.metric][sort.field];
    const leftVal = typeof left === "number" ? left : Number.NEGATIVE_INFINITY;
    const rightVal = typeof right === "number" ? right : Number.NEGATIVE_INFINITY;
    if (leftVal === rightVal) return a.brandName.localeCompare(b.brandName);
    return sort.dir === "asc" ? leftVal - rightVal : rightVal - leftVal;
  });
}

export default function TrackingComparisonTable({ model }: TrackingComparisonTableProps) {
  const firstAvailableMetric = METRIC_GROUPS.find((group) => model.metricMeta[group.key].available)?.key ?? "brand_awareness";
  const [sortState, setSortState] = useState<SortState>({
    metric: firstAvailableMetric,
    field: "deltaAbs",
    dir: "desc",
  });

  const visibleMetrics = useMemo(
    () => METRIC_GROUPS.filter((group) => model.metricMeta[group.key].available),
    [model.metricMeta]
  );

  const rows = useMemo(() => sortRows(model.brands, sortState), [model.brands, sortState]);
  const heatmapRanges = useMemo(() => {
    const ranges = new Map<TrackingMetricKey, { absP10: number; absP90: number; relP10: number; relP90: number }>();
    for (const metric of visibleMetrics.map((item) => item.key)) {
      const absValues = model.brands
        .map((brand) => brand.metrics[metric].deltaAbs)
        .filter((value): value is number => typeof value === "number");
      const relValues = model.brands
        .map((brand) => brand.metrics[metric].deltaRelPct)
        .filter((value): value is number => typeof value === "number");
      ranges.set(metric, {
        absP10: percentile(absValues, 0.1),
        absP90: percentile(absValues, 0.9),
        relP10: percentile(relValues, 0.1),
        relP90: percentile(relValues, 0.9),
      });
    }
    return ranges;
  }, [model.brands, visibleMetrics]);

  const toggleSort = (metric: TrackingMetricKey, field: SortField) => {
    setSortState((prev) => {
      if (prev.metric === metric && prev.field === field) {
        return { ...prev, dir: prev.dir === "asc" ? "desc" : "asc" };
      }
      return { metric, field, dir: "desc" };
    });
  };

  return (
    <section className="main-surface rounded-3xl p-5">
      <div className="overflow-auto rounded-2xl border border-ink/10">
        <table className="min-w-[1280px] border-collapse text-xs">
          <thead className="sticky top-0 z-20 bg-[#f7f8fa]">
            <tr>
              <th
                rowSpan={2}
                className="sticky left-0 z-30 border-b border-r border-ink/10 bg-[#f7f8fa] px-3 py-2 text-left text-[11px] font-semibold uppercase tracking-[0.08em] text-slate"
              >
                Brand
              </th>
              {visibleMetrics.map((group) => (
                <th
                  key={group.key}
                  colSpan={4}
                  className="border-b border-r border-ink/10 px-2 py-2 text-center text-[11px] font-semibold uppercase tracking-[0.08em] text-slate"
                >
                  {group.full}
                </th>
              ))}
            </tr>
            <tr>
              {visibleMetrics.map((group) => (
                <Fragment key={`${group.key}-subheaders`}>
                  <th key={`${group.key}-earlier`} className="border-b border-r border-ink/10 px-2 py-1.5 text-slate">
                    Pre
                  </th>
                  <th key={`${group.key}-later`} className="border-b border-r border-ink/10 px-2 py-1.5 text-slate">
                    Post
                  </th>
                  <th key={`${group.key}-delta-abs`} className="border-b border-r border-ink/10 px-2 py-1.5 text-slate">
                    <button type="button" className="inline-flex items-center gap-1" onClick={() => toggleSort(group.key, "deltaAbs")}>
                      Delta pts <span aria-hidden>{SORT_ICON}</span>
                    </button>
                  </th>
                  <th key={`${group.key}-delta-rel`} className="border-b border-r border-ink/10 px-2 py-1.5 text-slate">
                    <button
                      type="button"
                      className="inline-flex items-center gap-1"
                      onClick={() => toggleSort(group.key, "deltaRelPct")}
                    >
                      Delta % <span aria-hidden>{SORT_ICON}</span>
                    </button>
                  </th>
                </Fragment>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.brandName} className="border-b border-ink/5">
                <td className="sticky left-0 z-10 border-r border-ink/10 bg-white px-3 py-2 font-medium text-ink">{row.brandName}</td>
                {visibleMetrics.map((group) => {
                  const metric = row.metrics[group.key];
                  const unit = model.metricMeta[group.key].unit;
                  const range = heatmapRanges.get(group.key) || {
                    absP10: -5,
                    absP90: 5,
                    relP10: -10,
                    relP90: 10,
                  };
                  return (
                    <Fragment key={`${row.brandName}-${group.key}`}>
                      <td key={`${row.brandName}-${group.key}-earlier`} className="border-r border-ink/10 px-2 py-2 text-right text-slate">
                        {formatMetric(metric.valueEarlier, unit)}
                      </td>
                      <td key={`${row.brandName}-${group.key}-later`} className="border-r border-ink/10 px-2 py-2 text-right text-slate">
                        {formatMetric(metric.valueLater, unit)}
                      </td>
                      <td
                        key={`${row.brandName}-${group.key}-delta-abs`}
                        className={`border-r border-ink/10 px-2 py-2 text-right ${heatmapClass(metric.deltaAbs, range.absP10, range.absP90)}`}
                        title={metric.deltaAbs == null ? "No data" : `Delta pts: ${metric.deltaAbs.toFixed(1)}`}
                      >
                        {formatDeltaPts(metric.deltaAbs)}
                      </td>
                      <td
                        key={`${row.brandName}-${group.key}-delta-rel`}
                        className={`border-r border-ink/10 px-2 py-2 text-right ${heatmapClass(metric.deltaRelPct, range.relP10, range.relP90)}`}
                        title={metric.deltaRelPct == null ? "No data" : `Delta %: ${metric.deltaRelPct.toFixed(1)}%`}
                      >
                        {formatDeltaPct(metric.deltaRelPct)}
                      </td>
                    </Fragment>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
