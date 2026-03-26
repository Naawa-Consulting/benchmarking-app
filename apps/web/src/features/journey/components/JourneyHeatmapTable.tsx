"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import type { HeatmapCell, HeatmapColumn, JourneyHeatmapMatrix } from "../heatmap/buildJourneyHeatmap";

type JourneyHeatmapTableProps = {
  matrix: JourneyHeatmapMatrix;
  focusedBrandName?: string | null;
  onFocusBrand?: (brandName: string | null) => void;
  benchmarkLabel?: string;
};

type HeatmapTab = "levels" | "conversion" | "gap";

const headerLabel = (col: HeatmapColumn) => col.label;

const clamp01 = (value: number) => Math.max(0, Math.min(1, value));

const pct = (value: number | null) => (typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "--");
const pts = (value: number | null) => {
  if (typeof value !== "number") return "--";
  return `${value >= 0 ? "+" : ""}${(value * 100).toFixed(1)} pts`;
};

const levelCellColor = (value: number | null) => {
  if (value == null) return "rgba(248,250,252,1)";
  const ratio = clamp01(value);
  const alpha = 0.06 + ratio * 0.28;
  return `rgba(16,185,129,${alpha.toFixed(3)})`;
};

const gapCellColor = (delta: number | null, clamp: number) => {
  if (delta == null) return "rgba(248,250,252,1)";
  const ratio = Math.max(-1, Math.min(1, delta / clamp));
  if (ratio >= 0) {
    const alpha = 0.08 + ratio * 0.26;
    return `rgba(16,185,129,${alpha.toFixed(3)})`;
  }
  const alpha = 0.08 + Math.abs(ratio) * 0.24;
  return `rgba(244,63,94,${alpha.toFixed(3)})`;
};

const conversionCellColor = (value: number | null) => {
  if (value == null) return "rgba(248,250,252,1)";
  const ratio = clamp01(value);
  const alpha = 0.05 + ratio * 0.24;
  return `rgba(5,150,105,${alpha.toFixed(3)})`;
};

const formatValue = (col: HeatmapColumn, value: number | null) => {
  if (value == null) return "--";
  if (col.key === "journey_index") return `${Math.round(value * 100)}`;
  if (col.key === "nps" || col.key === "csat") return `${(value * 100).toFixed(1)}%`;
  return pct(value);
};
const formatSample = (value: number) => Math.round(value).toLocaleString();

const buildTooltip = (
  tab: HeatmapTab,
  rowLabel: string,
  col: HeatmapColumn,
  cell: HeatmapCell | undefined,
  benchmarkLabel: string
) => {
  if (!cell) return "";
  if (cell.missing) return `${rowLabel} - ${col.label}\nNo disponible para esta seleccion.`;

  if (tab === "levels") {
    return [
      `${rowLabel} - ${col.label}`,
      `Value: ${formatValue(col, cell.value)}`,
      `Muestra entrevistada: ${formatSample(cell.coverageSample)} panelistas`,
    ].join("\n");
  }

  if (tab === "conversion") {
    const quality = cell.anomalyFlag
      ? [
          "Quality: anomaly detected (conversion >100% or non-monotonic stages).",
          cell.excludedFromIndex ? "Journey Index: this segment is excluded." : null,
        ]
          .filter(Boolean)
          .join("\n")
      : null;
    return [
      `${rowLabel} - ${col.label}`,
      `Conversion: ${pct(cell.value)}`,
      `Drop vs bench: ${pts(cell.delta)}`,
      `Muestra entrevistada: ${formatSample(cell.coverageSample)} panelistas`,
      quality,
    ].join("\n");
  }

  return [
    `${rowLabel} - ${col.label}`,
    `Value: ${formatValue(col, cell.value)}`,
    `${benchmarkLabel}: ${formatValue(col, cell.benchmarkValue)}`,
    `Delta: ${pts(cell.delta)}`,
    `Muestra entrevistada: ${formatSample(cell.coverageSample)} panelistas`,
  ].join("\n");
};

export default function JourneyHeatmapTable({
  matrix,
  focusedBrandName = null,
  onFocusBrand,
  benchmarkLabel = "Benchmark",
}: JourneyHeatmapTableProps) {
  const [sortKey, setSortKey] = useState<string>("stage:Brand Awareness");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [rowMode, setRowMode] = useState<"top25" | "all">("top25");
  const [activeTab, setActiveTab] = useState<HeatmapTab>("levels");
  const scrollerRef = useRef<HTMLDivElement | null>(null);
  const [showLeftShadow, setShowLeftShadow] = useState(false);
  const [showRightShadow, setShowRightShadow] = useState(false);
  const [scrollTop, setScrollTop] = useState(0);
  const ROW_HEIGHT = 33;
  const VIEWPORT_HEIGHT = 620;
  const benchmarkStickyBg = "#f1f5f9";

  const columns = useMemo(() => {
    const levels = matrix.columns.filter((col) => col.group === "stage" || col.group === "metric");
    const conversions = matrix.columns.filter((col) => col.group === "conversion");
    const gap = matrix.columns.filter((col) => col.group === "stage" || col.group === "metric" || col.group === "conversion");

    if (activeTab === "conversion") return conversions;
    if (activeTab === "gap") return gap;
    return levels;
  }, [activeTab, matrix.columns]);

  useEffect(() => {
    if (!columns.some((col) => col.key === sortKey)) {
      setSortKey(columns[0]?.key || "");
      setSortDir("desc");
    }
  }, [columns, sortKey]);

  const sortedRows = useMemo(() => {
    const benchmarkRows = matrix.rows.filter((row) => row.isBenchmark);
    const nonBenchmark = matrix.rows.filter((row) => !row.isBenchmark);
    const ordered = nonBenchmark.slice().sort((a, b) => {
      const av = a.cells[sortKey]?.value;
      const bv = b.cells[sortKey]?.value;
      const aNum = typeof av === "number" ? av : Number.NEGATIVE_INFINITY;
      const bNum = typeof bv === "number" ? bv : Number.NEGATIVE_INFINITY;
      return sortDir === "desc" ? bNum - aNum : aNum - bNum;
    });

    const body = rowMode === "all" ? ordered : ordered.slice(0, 25);
    return { benchmarkRows, body };
  }, [matrix.rows, rowMode, sortDir, sortKey]);

  const bodyWindow = useMemo(() => {
    const shouldVirtualize = rowMode === "all" && sortedRows.body.length > 120;
    if (!shouldVirtualize) {
      return {
        rows: sortedRows.body,
        topSpacer: 0,
        bottomSpacer: 0,
      };
    }
    const overscan = 12;
    const start = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - overscan);
    const visibleCount = Math.ceil(VIEWPORT_HEIGHT / ROW_HEIGHT) + overscan * 2;
    const end = Math.min(sortedRows.body.length, start + visibleCount);
    return {
      rows: sortedRows.body.slice(start, end),
      topSpacer: start * ROW_HEIGHT,
      bottomSpacer: Math.max(0, (sortedRows.body.length - end) * ROW_HEIGHT),
    };
  }, [rowMode, scrollTop, sortedRows.body]);

  const updateShadows = () => {
    const node = scrollerRef.current;
    if (!node) return;
    const max = Math.max(0, node.scrollWidth - node.clientWidth);
    setShowLeftShadow(node.scrollLeft > 2);
    setShowRightShadow(node.scrollLeft < max - 2);
  };

  useEffect(() => {
    updateShadows();
    const node = scrollerRef.current;
    if (!node) return;
    const onScroll = () => {
      updateShadows();
      setScrollTop(node.scrollTop);
    };
    node.addEventListener("scroll", onScroll, { passive: true });
    window.addEventListener("resize", onScroll);
    return () => {
      node.removeEventListener("scroll", onScroll);
      window.removeEventListener("resize", onScroll);
    };
  }, [columns, sortedRows]);

  const getCellBackground = (col: HeatmapColumn, cell: HeatmapCell | undefined) => {
    if (!cell || cell.missing) return "rgba(248,250,252,1)";
    if (activeTab === "gap") return gapCellColor(cell.delta, col.clamp);
    if (activeTab === "conversion") return conversionCellColor(cell.value);
    return levelCellColor(cell.value);
  };

  return (
    <section className="main-surface p-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <h3 className="text-xl font-semibold">Heatmap comparison</h3>
        <div className="flex items-center gap-2 text-xs">
          {([[
            "levels",
            "Levels",
          ], ["conversion", "Conversion Rates"], ["gap", "Gap vs Benchmark"]] as const).map(([value, label]) => (
            <button
              key={value}
              type="button"
              onClick={() => setActiveTab(value)}
              className={`rounded-full border px-3 py-1 ${
                activeTab === value
                  ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
                  : "border-ink/10 text-slate"
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      <div className="mt-3 flex items-center justify-between gap-3">
        <p className="text-xs text-slate">
          {activeTab === "levels"
            ? "Stage levels and experience metrics"
            : activeTab === "conversion"
              ? "Conversion rates between consecutive funnel stages"
              : "Delta vs benchmark (pts)"}
        </p>
        {matrix.rows.length > 26 && (
          <div className="flex items-center gap-2 text-xs">
            <button
              type="button"
              onClick={() => setRowMode("top25")}
              className={`rounded-full border px-3 py-1 ${
                rowMode === "top25"
                  ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
                  : "border-ink/10 text-slate"
              }`}
            >
              Top 25
            </button>
            <button
              type="button"
              onClick={() => setRowMode("all")}
              className={`rounded-full border px-3 py-1 ${
                rowMode === "all"
                  ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
                  : "border-ink/10 text-slate"
              }`}
            >
              All
            </button>
          </div>
        )}
      </div>

      <div className="relative mt-4 rounded-2xl border border-ink/10 bg-white">
        {showLeftShadow && <div className="pointer-events-none absolute bottom-0 left-0 top-0 z-30 w-4 bg-gradient-to-r from-slate-300/20 to-transparent" />}
        {showRightShadow && <div className="pointer-events-none absolute bottom-0 right-0 top-0 z-30 w-4 bg-gradient-to-l from-slate-300/20 to-transparent" />}

        <div ref={scrollerRef} className="max-h-[620px] overflow-auto rounded-2xl">
          <table className="w-full border-separate border-spacing-0 text-xs">
            <thead className="sticky top-0 z-20 bg-white">
              <tr className="border-b border-ink/10">
                <th className="sticky left-0 z-30 min-w-[190px] border-r border-ink/10 bg-white px-3 py-2 text-left">
                  Brand
                </th>
                {columns.map((col) => (
                  <th key={col.key} className="px-2 py-2 text-center align-top">
                    <button
                      type="button"
                      className="inline-flex max-w-[180px] items-start gap-1 rounded-full border border-transparent px-2 py-0.5 text-left leading-tight hover:border-ink/10"
                      onClick={() => {
                        if (sortKey === col.key) setSortDir((prev) => (prev === "desc" ? "asc" : "desc"));
                        else {
                          setSortKey(col.key);
                          setSortDir("desc");
                        }
                      }}
                      title={`Sort by ${col.label}`}
                    >
                      <span>{headerLabel(col)}</span>
                      {sortKey === col.key ? (
                        <span aria-hidden>{sortDir === "desc" ? "↓" : "↑"}</span>
                      ) : null}
                    </button>
                  </th>
                ))}
              </tr>
            </thead>

            {sortedRows.benchmarkRows.length > 0 && (
              <tbody>
                {sortedRows.benchmarkRows.map((benchmarkRow, idx) => (
                  <tr key={benchmarkRow.key} className="border-b border-ink/10">
                    <td
                      className="sticky left-0 z-[19] border-r border-ink/10 px-3 py-2 font-semibold text-ink"
                      style={{ top: `${33 + idx * 33}px`, backgroundColor: benchmarkStickyBg }}
                    >
                      {benchmarkRow.brandName}
                    </td>
                    {columns.map((col) => {
                      const cell = benchmarkRow.cells[col.key];
                      return (
                        <td
                          key={`${benchmarkRow.key}-${col.key}`}
                          className="sticky z-[18] min-w-[96px] px-2 py-2 text-center text-ink"
                          style={{ top: `${33 + idx * 33}px`, backgroundColor: benchmarkStickyBg }}
                          title={buildTooltip(activeTab, benchmarkRow.brandName, col, cell, benchmarkLabel)}
                        >
                          {cell?.missing ? <span className="text-slate">—</span> : <p>{formatValue(col, cell?.value ?? null)}</p>}
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            )}

            <tbody>
              {bodyWindow.topSpacer > 0 && (
                <tr>
                  <td colSpan={columns.length + 1} style={{ height: `${bodyWindow.topSpacer}px` }} />
                </tr>
              )}
              {bodyWindow.rows.map((row) => {
                const isFocused = !!focusedBrandName && row.brandName === focusedBrandName;
                const shouldDim = !!focusedBrandName && row.brandName !== focusedBrandName;
                return (
                  <tr
                    key={row.key}
                    className={`border-b border-ink/5 transition-opacity ${
                      shouldDim ? "opacity-55 hover:opacity-100" : "opacity-100"
                    }`}
                  >
                    <td
                      className={`sticky left-0 z-[12] border-r border-ink/10 bg-white px-3 py-2 ${
                        isFocused ? "shadow-[inset_3px_0_0_0_rgba(16,185,129,0.85)]" : ""
                      }`}
                    >
                      <span className={`block w-full text-left ${isFocused ? "font-semibold text-emerald-700" : "text-ink"}`}>
                        {row.brandName}
                      </span>
                    </td>

                    {columns.map((col) => {
                      const cell = row.cells[col.key];
                      const tooltip = buildTooltip(activeTab, row.brandName, col, cell, benchmarkLabel);
                      const background = getCellBackground(col, cell);
                      return (
                        <td
                          key={`${row.key}-${col.key}`}
                          className="min-w-[96px] px-2 py-2 text-center text-ink"
                          style={{ background }}
                          title={tooltip}
                        >
                          {cell?.missing ? (
                            <span className="text-slate">—</span>
                          ) : (
                            <div className="leading-tight">
                              <p>
                                {formatValue(col, cell?.value ?? null)}
                                {activeTab === "conversion" && cell?.anomalyFlag ? (
                                  <span className="ml-1 inline-block rounded-full border border-amber-500/40 bg-amber-500/15 px-1 py-0 text-[9px] text-amber-700">
                                    !
                                  </span>
                                ) : null}
                              </p>
                              {activeTab !== "levels" && (
                                <p className="text-[10px] text-slate">{cell?.delta == null ? "—" : pts(cell.delta)}</p>
                              )}
                            </div>
                          )}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
              {bodyWindow.bottomSpacer > 0 && (
                <tr>
                  <td colSpan={columns.length + 1} style={{ height: `${bodyWindow.bottomSpacer}px` }} />
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

