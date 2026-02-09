"use client";

import ReactECharts from "echarts-for-react";
import { useMemo } from "react";

type NetworkNode = {
  id: string;
  label: string;
  type: string;
  size: number;
  group: string;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  context_key?: string | null;
  halo_key?: string | null;
  context_sources?: Array<{
    study_id?: string;
    sector?: string | null;
    subsector?: string | null;
    category?: string | null;
  }> | null;
  colorMeta?: Record<string, unknown> | null;
};

type NetworkLink = {
  source: string;
  target: string;
  weight: number;
  type: string;
  w_recall_raw?: number | null;
  w_recall_norm?: number | null;
  w_consideration_raw?: number | null;
  w_consideration_norm?: number | null;
  w_purchase_raw?: number | null;
  w_purchase_norm?: number | null;
  n_base?: number | null;
  colorMeta?: Record<string, unknown> | null;
};

type NetworkCanvasProps = {
  nodes: NetworkNode[];
  links: NetworkLink[];
  metricMode: "recall" | "consideration" | "purchase" | "both";
  clusterMode?: "off" | "category";
  onHoverNode?: (node: NetworkNode | null) => void;
  labelMode?: "auto" | "off";
  spotlight?: boolean;
  layoutMode?: "auto" | "spacious";
  pulseNodeId?: string | null;
};

const formatPct01 = (value?: number | null) =>
  typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "--";

const formatPct100 = (value?: number | null) =>
  typeof value === "number" ? `${value.toFixed(1)}%` : "--";

const getMetricLabel = (metric: string) => {
  switch (metric) {
    case "consideration":
      return "Consideration";
    case "purchase":
      return "Purchase";
    default:
      return "Recall";
  }
};

const PALETTE = ["#0ea5a4", "#14b8a6", "#06b6d4", "#0f766e", "#22c55e", "#22d3ee", "#38bdf8", "#34d399"];
const HALO_PALETTE = ["#0f766e", "#0e7490", "#0f172a", "#1f2937", "#0f766e", "#155e75", "#1e293b", "#166534"];

const hashString = (value: string) => {
  let hash = 0;
  for (let i = 0; i < value.length; i += 1) {
    hash = (hash << 5) - hash + value.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
};

const getPaletteColor = (key: string | null | undefined, fallback: string) => {
  if (!key) return fallback;
  const index = hashString(key) % PALETTE.length;
  return PALETTE[index];
};

const getHaloColor = (key: string | null | undefined, fallback: string) => {
  if (!key) return "rgba(15, 23, 42, 0.25)";
  const index = hashString(key) % HALO_PALETTE.length;
  return HALO_PALETTE[index];
};

const hexToRgba = (value: string, alpha: number) => {
  if (value.startsWith("rgba")) return value;
  const hex = value.replace("#", "");
  if (hex.length !== 6) return `rgba(15, 23, 42, ${alpha})`;
  const r = parseInt(hex.slice(0, 2), 16);
  const g = parseInt(hex.slice(2, 4), 16);
  const b = parseInt(hex.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
};

export function NetworkCanvas({
  nodes,
  links,
  metricMode,
  clusterMode = "off",
  onHoverNode,
  labelMode = "auto",
  spotlight = true,
  layoutMode = "auto",
  pulseNodeId,
}: NetworkCanvasProps) {
  const option = useMemo(() => {
    const categories = Array.from(new Set(nodes.map((node) => node.group))).map((name) => ({
      name,
    }));
    const labelById = new Map(nodes.map((node) => [node.id, node.label]));
    const nodeById = new Map(nodes.map((node) => [node.id, node]));
    const shortLabel = (value: string) => value.replace(/^[^:]+:/, "");
    const brandCategories = Array.from(
      new Set(nodes.filter((node) => node.type === "brand").map((node) => node.category || "Unknown"))
    ).sort();
    const categoryIndex = new Map(brandCategories.map((value, idx) => [value, idx]));
    const topBrands = nodes
      .filter((node) => node.type === "brand")
      .slice()
      .sort((a, b) => b.size - a.size)
      .slice(0, 12)
      .map((node) => node.id);
    const topTouchpoints = nodes
      .filter((node) => node.type === "touchpoint")
      .slice()
      .sort((a, b) => b.size - a.size)
      .slice(0, 8)
      .map((node) => node.id);

    const degreeMap = new Map<string, number>();
    for (const link of links) {
      degreeMap.set(link.source, (degreeMap.get(link.source) || 0) + 1);
      degreeMap.set(link.target, (degreeMap.get(link.target) || 0) + 1);
    }
    const degrees = Array.from(degreeMap.values()).sort((a, b) => a - b);
    const degreeThreshold =
      degrees.length > 1 ? degrees[Math.floor((degrees.length - 1) * 0.7)] : 0;

    const connectionMap = new Map<string, { incoming: NetworkLink[]; outgoing: NetworkLink[] }>();
    for (const link of links) {
      const source = link.source;
      const target = link.target;
      if (!connectionMap.has(source)) {
        connectionMap.set(source, { incoming: [], outgoing: [] });
      }
      if (!connectionMap.has(target)) {
        connectionMap.set(target, { incoming: [], outgoing: [] });
      }
      connectionMap.get(source)?.outgoing.push(link);
      connectionMap.get(target)?.incoming.push(link);
    }

    const isSecondaryLink = (link: NetworkLink) => link.type.startsWith("secondary_");
    const secondaryMode = (link: NetworkLink): "consideration" | "purchase" | "recall" => {
      if (link.type.includes("purchase")) return "purchase";
      if (link.type.includes("consideration")) return "consideration";
      return "recall";
    };

    const buildLinkStyle = (
      link: NetworkLink,
      mode: "consideration" | "purchase" | "recall",
      secondary: boolean
    ) => {
      const weight =
        mode === "purchase"
          ? link.w_purchase_norm
          : mode === "consideration"
            ? link.w_consideration_norm
            : link.w_recall_norm;
      const width = (secondary ? 0.4 : 1.2) + (weight || 0) * (secondary ? 1.2 : 4.2);
      const opacity = (secondary ? 0.05 : 0.28) + (weight || 0) * (secondary ? 0.18 : 0.55);
      return {
        width,
        opacity,
        color: secondary ? "rgba(15, 23, 42, 0.6)" : "#0f172a",
        type: mode === "purchase" ? "dashed" : "solid",
        dashOffset: mode === "purchase" ? 4 : 0,
      };
    };

    const renderLinks: Array<NetworkLink & { lineStyle: Record<string, unknown>; emphasis: Record<string, unknown> }> =
      [];

    const pushLink = (link: NetworkLink, mode: "consideration" | "purchase" | "recall") => {
      const secondary = isSecondaryLink(link);
      renderLinks.push({
        ...link,
        lineStyle: buildLinkStyle(link, mode, secondary),
        emphasis: {
          lineStyle: {
            opacity: secondary ? 0.6 : 0.9,
            width: secondary ? 2.5 : 4.5,
          },
        },
      });
    };

    const primaryLinks = links.filter((link) => !isSecondaryLink(link));
    const secondaryLinks = links.filter((link) => isSecondaryLink(link));

    if (metricMode === "both") {
      for (const link of primaryLinks) {
        if (link.w_consideration_norm != null) {
          pushLink(link, "consideration");
        }
        if (link.w_purchase_norm != null) {
          pushLink(link, "purchase");
        }
      }
    } else if (metricMode === "purchase") {
      for (const link of primaryLinks) {
        if (link.w_purchase_norm != null) {
          pushLink(link, "purchase");
        }
      }
    } else if (metricMode === "consideration") {
      for (const link of primaryLinks) {
        if (link.w_consideration_norm != null) {
          pushLink(link, "consideration");
        }
      }
    } else {
      for (const link of primaryLinks) {
        if (link.w_recall_norm != null) {
          pushLink(link, "recall");
        }
      }
    }

    for (const link of secondaryLinks) {
      const mode = secondaryMode(link);
      if (mode === "purchase" && link.w_purchase_norm == null) continue;
      if (mode === "consideration" && link.w_consideration_norm == null) continue;
      if (mode === "recall" && link.w_recall_norm == null) continue;
      pushLink(link, mode);
    }

    return {
      tooltip: {
        trigger: "item",
        backgroundColor: "rgba(15, 23, 42, 0.95)",
        borderColor: "rgba(148, 163, 184, 0.4)",
        textStyle: { color: "#f8fafc", fontSize: 12 },
        formatter: (params: any) => {
          if (params.dataType === "edge") {
            const source = labelById.get(params.data.source) ?? shortLabel(params.data.source);
            const target = labelById.get(params.data.target) ?? shortLabel(params.data.target);
            const recall = formatPct01(params.data.w_recall_raw);
            const consideration = formatPct01(params.data.w_consideration_raw);
            const purchase = formatPct01(params.data.w_purchase_raw);
            const base = params.data.n_base ?? "--";
            if (params.data.type?.startsWith("secondary_")) {
              const metricLabel = params.data.type.includes("purchase")
                ? "Purchase"
                : params.data.type.includes("consideration")
                  ? "Consideration"
                  : "Recall";
              const value =
                metricLabel === "Purchase"
                  ? purchase
                  : metricLabel === "Consideration"
                    ? consideration
                    : recall;
              const meta = params.data.colorMeta || {};
              const coCount = meta.co_count ?? "--";
              const baseA = meta.base_a ?? "--";
              const baseB = meta.base_b ?? "--";
              return `${source} ↔ ${target}<br/>Co-${metricLabel}: ${value}<br/>Co-count: ${coCount}<br/>Base A: ${baseA} · Base B: ${baseB}`;
            }
            return `${source} -> ${target}<br/>Recall: ${recall}<br/>Consideration: ${consideration}<br/>Purchase: ${purchase}<br/>Base N: ${base}`;
          }
          const node: NetworkNode | undefined = nodeById.get(params.data.id);
          if (!node) return "";
          if (node.type === "brand") {
            const awareness = node.colorMeta?.kpi_awareness as number | undefined;
            const base = node.colorMeta?.base_n_awareness as number | undefined;
            const contextMixed = node.colorMeta?.context_mixed as boolean | undefined;
            const sources = node.context_sources || [];
            const incoming = (connectionMap.get(node.id)?.incoming || []).filter(
              (link) => link.type === "primary_tp_brand"
            );
            const top = incoming
              .slice()
              .sort((a, b) => (b.w_consideration_raw || 0) - (a.w_consideration_raw || 0))
              .slice(0, 3)
              .map((link) => {
                const label = labelById.get(link.source) ?? shortLabel(link.source);
                return `${label} (${formatPct01(link.w_consideration_raw)})`;
              })
              .join(", ");
            return `${node.label}<br/>Awareness: ${formatPct100(awareness)}<br/>Base N: ${base ?? "--"}${
              top ? `<br/><span style="opacity:.7">Top touchpoints: ${top}</span>` : ""
            }<br/>Sector: ${node.sector ?? "--"}<br/>Subsector: ${node.subsector ?? "--"}<br/>Category: ${
              node.category ?? "--"
            }${contextMixed ? "<br/><span style='opacity:.7'>Context: mixed</span>" : ""}${
              sources.length
                ? `<br/><span style='opacity:.7'>Sources: ${sources
                    .slice(0, 3)
                    .map((item) => item.study_id)
                    .join(", ")}</span>`
                : ""
            }`;
          }
          const recall = node.colorMeta?.kpi_recall as number | undefined;
          const base = node.colorMeta?.base_n_recall as number | undefined;
          const outgoing = (connectionMap.get(node.id)?.outgoing || []).filter(
            (link) => link.type === "primary_tp_brand"
          );
          const top = outgoing
            .slice()
            .sort((a, b) => (b.w_consideration_raw || 0) - (a.w_consideration_raw || 0))
            .slice(0, 3)
            .map((link) => {
              const label = labelById.get(link.target) ?? shortLabel(link.target);
              return `${label} (${formatPct01(link.w_consideration_raw)})`;
            })
            .join(", ");
          return `${node.label}<br/>Recall: ${formatPct100(recall)}<br/>Base N: ${base ?? "--"}${
            top ? `<br/><span style="opacity:.7">Top brands: ${top}</span>` : ""
          }`;
        },
      },
      series: [
        {
          type: "graph",
          layout: "force",
          roam: true,
          draggable: true,
          center: ["50%", "50%"],
          zoom: layoutMode === "spacious" ? 1.08 : 1.02,
          data: nodes.map((node) => {
            const paletteKey = (node.colorMeta?.paletteKey as string | undefined) || node.context_key || null;
            const haloKey = (node.colorMeta?.haloKey as string | undefined) || node.halo_key || null;
            const fillColor =
              node.group === "brand"
                ? getPaletteColor(paletteKey, "#0ea5a4")
                : node.group === "touchpoint"
                  ? "#1f2937"
                  : "#10b981";
            const haloColor = node.group === "brand" ? getHaloColor(haloKey, fillColor) : "transparent";
            const categoryName = node.category || "Unknown";
            const idx = categoryIndex.get(categoryName) ?? 0;
            const angle =
              node.type === "touchpoint"
                ? ((hashString(node.id) % 360) * Math.PI) / 180
                : (Math.PI * 2 * idx) / Math.max(1, brandCategories.length);
            const radius = layoutMode === "spacious" ? 240 : 180;
            const jitter = (hashString(node.id) % 40) - 20;
            let x = Math.cos(angle) * radius + jitter;
            let y = Math.sin(angle) * radius + jitter;
            if (clusterMode === "category" && node.type === "brand") {
              x = Math.cos(angle) * radius + jitter;
              y = Math.sin(angle) * radius + jitter;
            }
            if (node.type === "brand") {
              y -= layoutMode === "spacious" ? 120 : 80;
            }
            if (node.type === "touchpoint") {
              y += layoutMode === "spacious" ? 120 : 80;
            }

            const displaySize = node.group === "brand" ? node.size + 6 : node.size;
            const isTopLabel =
              labelMode === "auto" &&
              (topBrands.includes(node.id) ||
                topTouchpoints.includes(node.id) ||
                (degreeMap.get(node.id) || 0) >= degreeThreshold);
            const isBrand = node.group === "brand";

            const nodeValue = displaySize * (isBrand ? 1.2 : 1);
            return {
              id: node.id,
              name: node.label,
              label: {
                show: isTopLabel,
                backgroundColor: "rgba(255,255,255,0.88)",
                borderRadius: 12,
                padding: [4, 8],
                color: isBrand ? "#0f172a" : "#334155",
                fontSize: isBrand ? 12 : 11,
                fontWeight: isBrand ? 600 : 500,
                position: isBrand ? "top" : "bottom",
                distance: 6,
              },
              labelLine: { show: isTopLabel, length: 10, length2: 8, smooth: true },
              emphasis: {
                label: {
                  show: true,
                  color: "#0f172a",
                  backgroundColor: "rgba(255,255,255,0.95)",
                  borderRadius: 12,
                  padding: [4, 8],
                },
                labelLine: { show: true },
                itemStyle: {
                  borderWidth: node.group === "brand" ? 6 : 0,
                  borderColor: node.group === "brand" ? hexToRgba(haloColor, 0.55) : haloColor,
                  shadowBlur: 22,
                },
              },
              symbol: node.group === "touchpoint" ? "roundRect" : "circle",
              symbolSize: displaySize,
              value: nodeValue,
              category: categories.findIndex((category) => category.name === node.group),
              x,
              y,
              itemStyle: {
                color: fillColor,
                borderColor: node.group === "brand" ? hexToRgba(haloColor, 0.25) : haloColor,
                borderWidth: node.group === "brand" ? 5 : 0,
                borderType: node.group === "brand" ? "dashed" : "solid",
                shadowBlur: node.id === pulseNodeId ? 26 : 12,
                shadowColor: "rgba(15, 23, 42, 0.2)",
              },
            };
          }),
          links: renderLinks.map((link, idx) => ({
            source: link.source,
            target: link.target,
            w_recall_raw: link.w_recall_raw,
            w_consideration_raw: link.w_consideration_raw,
            w_purchase_raw: link.w_purchase_raw,
            n_base: link.n_base,
            type: link.type,
            colorMeta: link.colorMeta,
            lineStyle: {
              ...link.lineStyle,
              curveness:
                ((link.source.length + link.target.length + idx) % 20 - 10) / 70 +
                ((link.w_consideration_norm ?? link.w_purchase_norm ?? link.w_recall_norm ?? 0) * 0.15),
            },
            emphasis: link.emphasis,
          })),
          force: {
            repulsion: layoutMode === "spacious" ? 260 : 190,
            edgeLength: layoutMode === "spacious" ? [120, 260] : [90, 190],
            gravity: layoutMode === "spacious" ? 0.03 : 0.08,
          },
          emphasis: spotlight ? { focus: "adjacency", blurScope: "global" } : { focus: "none" },
          blur: spotlight
            ? {
                itemStyle: { opacity: 0.15 },
                lineStyle: { opacity: 0.04 },
              }
            : undefined,
          labelLayout: { hideOverlap: true, moveOverlap: "shiftX" },
        },
      ],
    };
  }, [nodes, links, metricMode, clusterMode, labelMode, spotlight, layoutMode, pulseNodeId]);

  return (
    <ReactECharts
      option={option}
      style={{ height: "78vh", width: "100%" }}
      onEvents={{
        mouseover: (params: any) => {
          if (!onHoverNode) return;
          if (params.dataType === "node") {
            const node = nodes.find((item) => item.id === params.data.id) || null;
            onHoverNode(node);
          }
        },
        mouseout: () => {
          onHoverNode?.(null);
        },
      }}
    />
  );
}
