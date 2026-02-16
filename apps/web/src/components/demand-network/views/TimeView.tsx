"use client";

import ReactECharts from "echarts-for-react";
import { useEffect, useMemo, useState } from "react";

import { buildAggregatedLinks, extractTimeBuckets, getLinkId, getMetricValue, linkInBucket } from "./helpers";
import type { DNViewCommonProps } from "./types";

export default function TimeView({
  nodes,
  links,
  metricMode,
  height,
  onHoverLink,
  onHoverNode,
  onSelectNode,
}: DNViewCommonProps) {
  const [index, setIndex] = useState(0);
  const [playing, setPlaying] = useState(false);
  const buckets = useMemo(() => extractTimeBuckets(links), [links]);

  useEffect(() => {
    if (!playing || buckets.length <= 1) return;
    const timer = setInterval(() => {
      setIndex((prev) => (prev + 1) % buckets.length);
    }, 650);
    return () => clearInterval(timer);
  }, [buckets.length, playing]);

  useEffect(() => {
    if (index >= buckets.length) {
      setIndex(0);
    }
  }, [buckets.length, index]);

  const activeBucket = buckets[index] || null;
  const activeLinks = useMemo(() => {
    if (!activeBucket) return links;
    return links.filter((link) => linkInBucket(link, activeBucket));
  }, [activeBucket, links]);

  const aggregated = useMemo(() => buildAggregatedLinks(activeLinks), [activeLinks]);

  const totalsByBucket = useMemo(() => {
    if (!buckets.length) return [{ label: "Current", value: aggregated.reduce((sum, link) => sum + getMetricValue(link, metricMode), 0) }];
    return buckets.map((bucket) => {
      const sum = buildAggregatedLinks(links.filter((link) => linkInBucket(link, bucket))).reduce(
        (acc, link) => acc + getMetricValue(link, metricMode),
        0
      );
      return { label: bucket, value: Number(sum.toFixed(4)) };
    });
  }, [aggregated, buckets, links, metricMode]);

  const topLinks = useMemo(
    () =>
      aggregated
        .slice()
        .sort((a, b) => getMetricValue(b, metricMode) - getMetricValue(a, metricMode))
        .slice(0, 8),
    [aggregated, metricMode]
  );

  const nodeById = useMemo(() => new Map(nodes.map((node) => [node.id, node])), [nodes]);

  return (
    <div className="space-y-3 rounded-[2rem] border border-ink/10 bg-slate-50/60 p-3 sm:p-4" style={{ height }}>
      <div className="flex flex-wrap items-center gap-2 text-xs text-slate">
        <button
          type="button"
          className="rounded-full border border-ink/10 px-3 py-1 hover:border-ink/20"
          onClick={() => setPlaying((prev) => !prev)}
          disabled={buckets.length <= 1}
          title={buckets.length <= 1 ? "No time buckets in current in-memory dataset" : "Play/pause"}
        >
          {playing ? "Pause" : "Play"}
        </button>
        <input
          className="w-56"
          type="range"
          min={0}
          max={Math.max(0, buckets.length - 1)}
          value={Math.min(index, Math.max(0, buckets.length - 1))}
          onChange={(event) => setIndex(Number(event.target.value))}
          disabled={buckets.length <= 1}
        />
        <span className="rounded-full border border-ink/10 px-3 py-1">
          {activeBucket ? `Bucket: ${activeBucket}` : "Current scope snapshot"}
        </span>
      </div>

      <div className="grid h-[calc(100%-2.1rem)] gap-3 lg:grid-cols-[1.4fr_1fr]">
        <div className="rounded-2xl border border-ink/10 bg-white/90 p-2">
          <ReactECharts
            option={{
              animation: false,
              grid: { left: 36, right: 16, top: 14, bottom: 34 },
              xAxis: {
                type: "category",
                data: totalsByBucket.map((item) => item.label),
                axisLabel: { color: "#475569", fontSize: 10 },
              },
              yAxis: {
                type: "value",
                axisLabel: { color: "#64748b", fontSize: 10 },
                splitLine: { lineStyle: { color: "rgba(15,23,42,0.08)" } },
              },
              series: [
                {
                  type: "line",
                  smooth: true,
                  symbol: "circle",
                  symbolSize: 6,
                  lineStyle: { width: 2, color: "#06b6d4" },
                  itemStyle: { color: "#0891b2" },
                  data: totalsByBucket.map((item) => item.value),
                },
              ],
            }}
            style={{ height: "100%", width: "100%" }}
            opts={{ renderer: "canvas", useDirtyRect: false } as any}
          />
        </div>

        <div className="rounded-2xl border border-ink/10 bg-white/90 p-3">
          <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate">Top links in slice</p>
          <div className="h-full space-y-1 overflow-auto pr-1">
            {topLinks.map((link) => {
              const source = nodeById.get(link.source);
              const target = nodeById.get(link.target);
              return (
                <button
                  key={getLinkId(link)}
                  type="button"
                  className="w-full rounded-xl border border-ink/10 px-2 py-1 text-left text-[11px] text-slate hover:border-ink/20"
                  onMouseEnter={() => {
                    onHoverNode(null);
                    onHoverLink({
                      id: getLinkId(link),
                      source: link.source,
                      target: link.target,
                      type: link.type,
                      w_recall_raw: link.w_recall_raw,
                      w_consideration_raw: link.w_consideration_raw,
                      w_purchase_raw: link.w_purchase_raw,
                      n_base: link.n_base,
                      countStudies: link.countStudies,
                    });
                  }}
                  onMouseLeave={() => onHoverLink(null)}
                  onClick={() => onSelectNode(target || null)}
                >
                  <p className="truncate font-medium text-ink">{source?.label || link.source} ? {target?.label || link.target}</p>
                  <p>{(getMetricValue(link, metricMode) * 100).toFixed(1)}%</p>
                </button>
              );
            })}
            {!topLinks.length && <p className="text-[11px] text-slate">No link values available for this slice.</p>}
          </div>
        </div>
      </div>
    </div>
  );
}
