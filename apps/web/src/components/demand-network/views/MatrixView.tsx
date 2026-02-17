"use client";

import ReactECharts from "echarts-for-react";
import { useMemo } from "react";

import type { HoveredLink } from "../../NetworkCanvas";
import { buildAggregatedLinks, getLinkId, getMetricValue } from "./helpers";
import type { DNViewCommonProps } from "./types";

type MatrixViewProps = DNViewCommonProps;

export default function MatrixView({
  nodes,
  links,
  metricMode,
  selectedNodeId,
  activeNodeId,
  activeLinkId,
  height,
  onHoverLink,
  onHoverNode,
  onSelectNode,
}: MatrixViewProps) {
  const nodeById = useMemo(() => new Map(nodes.map((node) => [node.id, node])), [nodes]);
  const aggregated = useMemo(() => buildAggregatedLinks(links), [links]);

  const brands = useMemo(
    () =>
      nodes
        .filter((node) => node.type === "brand")
        .slice()
        .sort((a, b) => a.label.localeCompare(b.label)),
    [nodes]
  );

  const touchpoints = useMemo(() => {
    const totalByTp = new Map<string, number>();
    for (const link of aggregated) {
      if (link.type !== "primary_tp_brand") continue;
      totalByTp.set(link.source, (totalByTp.get(link.source) || 0) + getMetricValue(link, metricMode));
    }
    return nodes
      .filter((node) => node.type === "touchpoint")
      .slice()
      .sort((a, b) => (totalByTp.get(b.id) || 0) - (totalByTp.get(a.id) || 0));
  }, [aggregated, metricMode, nodes]);

  const brandIndex = useMemo(() => new Map(brands.map((node, idx) => [node.id, idx])), [brands]);
  const tpIndex = useMemo(() => new Map(touchpoints.map((node, idx) => [node.id, idx])), [touchpoints]);

  const pointPayloadByKey = useMemo(() => {
    const map = new Map<string, HoveredLink>();
    for (const link of aggregated) {
      if (link.type !== "primary_tp_brand") continue;
      const y = brandIndex.get(link.target);
      const x = tpIndex.get(link.source);
      if (typeof x !== "number" || typeof y !== "number") continue;
      const id = getLinkId(link);
      map.set(`${x}:${y}`, {
        id,
        source: link.source,
        target: link.target,
        type: link.type,
        w_recall_raw: link.w_recall_raw,
        w_consideration_raw: link.w_consideration_raw,
        w_purchase_raw: link.w_purchase_raw,
        n_base: link.n_base,
        countStudies: link.countStudies,
      });
    }
    return map;
  }, [aggregated, brandIndex, tpIndex]);

  const option = useMemo(() => {
    const points: Array<[number, number, number]> = [];
    for (const link of aggregated) {
      if (link.type !== "primary_tp_brand") continue;
      const y = brandIndex.get(link.target);
      const x = tpIndex.get(link.source);
      if (typeof x !== "number" || typeof y !== "number") continue;
      points.push([x, y, getMetricValue(link, metricMode)]);
    }

    const max = Math.max(0.0001, ...points.map((item) => item[2]));

    return {
      animation: false,
      tooltip: {
        trigger: "item",
        formatter: (params: any) => {
          const [x, y, value] = params.data as [number, number, number];
          const tp = touchpoints[x]?.label || "-";
          const brand = brands[y]?.label || "-";
          return `${brand} ? ${tp}<br/>Value: ${(value * 100).toFixed(1)}%`;
        },
      },
      grid: {
        left: 160,
        right: 18,
        top: 16,
        bottom: 90,
      },
      xAxis: {
        type: "category",
        data: touchpoints.map((node) => node.label),
        axisLabel: { rotate: 40, fontSize: 10, color: "#475569" },
        axisTick: { show: false },
        axisLine: { lineStyle: { color: "rgba(15,23,42,0.14)" } },
      },
      yAxis: {
        type: "category",
        data: brands.map((node) => node.label),
        axisLabel: { fontSize: 11, color: "#334155" },
        axisTick: { show: false },
        axisLine: { lineStyle: { color: "rgba(15,23,42,0.14)" } },
      },
      visualMap: {
        show: false,
        min: 0,
        max,
        orient: "horizontal",
        left: "center",
        bottom: 12,
        itemWidth: 180,
        itemHeight: 10,
        text: ["High", "Low"],
        inRange: {
          color: ["#ecfeff", "#67e8f9", "#06b6d4", "#155e75"],
        },
      },
      series: [
        {
          type: "heatmap",
          data: points,
          progressive: 0,
          emphasis: {
            itemStyle: {
              borderColor: "#0f172a",
              borderWidth: 1,
            },
          },
        },
      ],
    };
  }, [aggregated, brandIndex, brands, metricMode, touchpoints, tpIndex]);

  return (
    <div className="rounded-[2rem] border border-ink/10 bg-slate-50/60 p-3 sm:p-4" style={{ height }}>
      <ReactECharts
        option={option}
        style={{ height: "100%", width: "100%" }}
        opts={{ renderer: "canvas", useDirtyRect: false } as any}
        onEvents={{
          mouseover: (params: any) => {
            if (params.dataType !== "series") return;
            const [x, y] = params.data as [number, number, number];
            const link = pointPayloadByKey.get(`${x}:${y}`) || null;
            onHoverLink(link);
            const brandId = link?.target || null;
            onHoverNode(brandId ? nodeById.get(brandId) || null : null);
          },
          mouseout: () => {
            onHoverLink(null);
            onHoverNode(null);
          },
          click: (params: any) => {
            if (params.dataType !== "series") {
              onSelectNode(null);
              return;
            }
            const [x, y] = params.data as [number, number, number];
            const link = pointPayloadByKey.get(`${x}:${y}`) || null;
            const brandId = link?.target || null;
            const node = brandId ? nodeById.get(brandId) || null : null;
            onSelectNode(node);
          },
          globalout: () => {
            onHoverLink(null);
            onHoverNode(null);
          },
        }}
      />
      <div className="mt-2 flex items-center justify-between text-[11px] text-slate">
        <span>Matrix view: brands x touchpoints (current metric)</span>
        <span>
          {selectedNodeId || activeNodeId || activeLinkId ? "Selection synced" : "Click any cell to focus a brand"}
        </span>
      </div>
    </div>
  );
}
