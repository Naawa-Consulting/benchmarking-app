"use client";

import ReactECharts from "echarts-for-react";
import { useMemo } from "react";

import { buildAggregatedLinks, getLinkId, getMetricValue } from "./helpers";
import type { DNViewCommonProps } from "./types";

export default function SankeyView({
  nodes,
  links,
  metricMode,
  height,
  onHoverNode,
  onHoverLink,
  onSelectNode,
}: DNViewCommonProps) {
  const nodeById = useMemo(() => new Map(nodes.map((node) => [node.id, node])), [nodes]);
  const aggregated = useMemo(() => buildAggregatedLinks(links), [links]);

  const option = useMemo(() => {
    const brandNodes = nodes
      .filter((node) => node.type === "brand")
      .slice()
      .sort((a, b) => a.label.localeCompare(b.label));
    const tpNodes = nodes
      .filter((node) => node.type === "touchpoint")
      .slice()
      .sort((a, b) => a.label.localeCompare(b.label));

    const sankeyNodes = [
      ...brandNodes.map((node) => ({ name: node.id, label: node.label, depth: 0, raw: node })),
      ...tpNodes.map((node) => ({ name: node.id, label: node.label, depth: 1, raw: node })),
    ];

    const sankeyLinks = aggregated
      .filter((link) => link.type === "primary_tp_brand")
      .map((link) => ({
        source: link.target,
        target: link.source,
        value: Math.max(0.0001, getMetricValue(link, metricMode)),
        raw: link,
      }));

    return {
      animation: false,
      tooltip: {
        trigger: "item",
        formatter: (params: any) => {
          if (params.dataType === "edge") {
            const raw = params.data.raw;
            return `${nodeById.get(raw.target)?.label || raw.target} ? ${nodeById.get(raw.source)?.label || raw.source}<br/>` +
              `Recall: ${typeof raw.w_recall_raw === "number" ? `${(raw.w_recall_raw * 100).toFixed(1)}%` : "--"}<br/>` +
              `Consideration: ${typeof raw.w_consideration_raw === "number" ? `${(raw.w_consideration_raw * 100).toFixed(1)}%` : "--"}<br/>` +
              `Purchase: ${typeof raw.w_purchase_raw === "number" ? `${(raw.w_purchase_raw * 100).toFixed(1)}%` : "--"}`;
          }
          const raw = params.data.raw;
          return `${raw.label}<br/>${raw.type === "brand" ? "Brand" : "Touchpoint"}`;
        },
      },
      series: [
        {
          type: "sankey",
          left: 10,
          top: 12,
          right: 12,
          bottom: 20,
          nodeWidth: 12,
          nodeGap: 10,
          draggable: false,
          emphasis: {
            focus: "adjacency",
          },
          label: {
            color: "#334155",
            fontSize: 11,
            formatter: (params: any) => params.data.raw?.label || params.name,
          },
          lineStyle: {
            color: "source",
            curveness: 0.45,
            opacity: 0.42,
          },
          data: sankeyNodes,
          links: sankeyLinks,
        },
      ],
    };
  }, [aggregated, metricMode, nodeById, nodes]);

  return (
    <div className="rounded-[2rem] border border-ink/10 bg-slate-50/60 p-3 sm:p-4" style={{ height }}>
      <ReactECharts
        option={option}
        style={{ height: "100%", width: "100%" }}
        opts={{ renderer: "canvas", useDirtyRect: false } as any}
        onEvents={{
          mouseover: (params: any) => {
            if (params.dataType === "node") {
              const id = params.data?.name as string;
              onHoverNode(nodeById.get(id) || null);
              onHoverLink(null);
              return;
            }
            if (params.dataType === "edge") {
              const raw = params.data?.raw;
              if (!raw) return;
              onHoverNode(null);
              onHoverLink({
                id: getLinkId(raw),
                source: raw.source,
                target: raw.target,
                type: raw.type,
                w_recall_raw: raw.w_recall_raw,
                w_consideration_raw: raw.w_consideration_raw,
                w_purchase_raw: raw.w_purchase_raw,
                n_base: raw.n_base,
                countStudies: raw.countStudies,
              });
            }
          },
          mouseout: () => {
            onHoverNode(null);
            onHoverLink(null);
          },
          click: (params: any) => {
            if (params.dataType === "node") {
              const id = params.data?.name as string;
              onSelectNode(nodeById.get(id) || null);
              return;
            }
            onSelectNode(null);
          },
        }}
      />
      <p className="mt-2 text-[11px] text-slate">Sankey flow uses current filtered links and metric weighting.</p>
    </div>
  );
}
