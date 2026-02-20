"use client";

import { useMemo, useState } from "react";

import type { HeatmapColumn, JourneyHeatmapMatrix } from "../heatmap/buildJourneyHeatmap";

type JourneyHeatmapTableProps = {
  matrix: JourneyHeatmapMatrix;
  focusedBrandName?: string | null;
  onFocusBrand?: (brandName: string | null) => void;
};

const pct = (value: number | null) => (typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "--");

const headerLabel = (col: HeatmapColumn) => col.label;

const cellBackground = (delta: number | null, clamp: number) => {
  if (delta == null) return "bg-slate-50";
  const ratio = Math.max(-1, Math.min(1, delta / clamp));
  if (ratio >= 0) {
    const alpha = 0.08 + ratio * 0.18;
    return `rgba(16,185,129,${alpha.toFixed(3)})`;
  }
  const alpha = 0.08 + Math.abs(ratio) * 0.18;
  return `rgba(244,63,94,${alpha.toFixed(3)})`;
};

export default function JourneyHeatmapTable({
  matrix,
  focusedBrandName = null,
  onFocusBrand,
}: JourneyHeatmapTableProps) {
  const [sortKey, setSortKey] = useState<string>("stage:Brand Awareness");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("desc");
  const [rowMode, setRowMode] = useState<"top25" | "all">("top25");

  const sortedRows = useMemo(() => {
    const benchmark = matrix.rows.find((row) => row.isBenchmark) || null;
    const nonBenchmark = matrix.rows.filter((row) => !row.isBenchmark);
    const ordered = nonBenchmark.slice().sort((a, b) => {
      const av = a.cells[sortKey]?.value;
      const bv = b.cells[sortKey]?.value;
      const aNum = typeof av === "number" ? av : Number.NEGATIVE_INFINITY;
      const bNum = typeof bv === "number" ? bv : Number.NEGATIVE_INFINITY;
      return sortDir === "desc" ? bNum - aNum : aNum - bNum;
    });
    const combined = benchmark ? [benchmark, ...ordered] : ordered;
    if (rowMode === "all") return combined;
    const head = combined.filter((row) => row.isBenchmark);
    const body = combined.filter((row) => !row.isBenchmark).slice(0, 25);
    return [...head, ...body];
  }, [matrix.rows, rowMode, sortDir, sortKey]);

  return (
    <section className="main-surface p-6">
      <div className="flex items-center justify-between gap-3">
        <h3 className="text-xl font-semibold">Heatmap comparison</h3>
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
      <div className="mt-4 overflow-auto rounded-2xl border border-ink/10">
        <table className="w-full border-collapse text-xs">
          <thead className="sticky top-0 z-10 bg-white">
            <tr className="border-b border-ink/10">
              <th className="sticky left-0 z-20 min-w-[180px] border-r border-ink/10 bg-white px-3 py-2 text-left">
                Brand
              </th>
              {matrix.columns.map((col) => (
                <th key={col.key} className="whitespace-nowrap px-2 py-2 text-center">
                  <button
                    type="button"
                    className="inline-flex items-center gap-1 rounded-full border border-transparent px-2 py-0.5 hover:border-ink/10"
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
                    {sortKey === col.key ? <span>{sortDir === "desc" ? "↓" : "↑"}</span> : null}
                  </button>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((row) => (
              <tr
                key={row.key}
                className={`border-b border-ink/5 transition-opacity ${
                  focusedBrandName && !row.isBenchmark && row.brandName !== focusedBrandName ? "opacity-45" : "opacity-100"
                }`}
              >
                <td className="sticky left-0 z-[1] border-r border-ink/10 bg-white px-3 py-2">
                  <button
                    type="button"
                    onClick={() => {
                      if (!onFocusBrand || row.isBenchmark) return;
                      onFocusBrand(row.brandName === focusedBrandName ? null : row.brandName);
                    }}
                    className={`w-full text-left ${
                      row.isBenchmark
                        ? "font-semibold text-ink"
                        : row.brandName === focusedBrandName
                          ? "font-semibold text-emerald-700"
                          : "text-ink"
                    }`}
                  >
                    {row.brandName}
                  </button>
                </td>
                {matrix.columns.map((col) => {
                  const cell = row.cells[col.key];
                  const styleBg = cell ? cellBackground(cell.delta, col.clamp) : "transparent";
                  const tooltip = cell
                    ? [
                        `${row.brandName} - ${col.label}`,
                        `Value: ${pct(cell.value)}`,
                        `Benchmark: ${pct(cell.benchmarkValue)}`,
                        `Delta: ${pct(cell.delta)}`,
                        `Coverage: ${cell.coverageStudies}/${cell.totalStudies} studies`,
                        cell.missing ? "No disponible para esta seleccion." : "",
                      ]
                        .filter(Boolean)
                        .join("\n")
                    : "";
                  return (
                    <td
                      key={`${row.key}-${col.key}`}
                      className="min-w-[92px] px-2 py-2 text-center text-ink"
                      style={{ background: styleBg }}
                      title={tooltip}
                    >
                      {cell?.missing ? (
                        <span className="text-slate">—</span>
                      ) : (
                        <div className="leading-tight">
                          <p>{pct(cell?.value ?? null)}</p>
                          {!row.isBenchmark && (
                            <p className="text-[10px] text-slate">{cell?.delta == null ? "—" : `${cell.delta >= 0 ? "+" : ""}${pct(cell.delta)}`}</p>
                          )}
                        </div>
                      )}
                    </td>
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
