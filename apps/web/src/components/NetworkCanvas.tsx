"use client";

import ReactECharts from "echarts-for-react";
import { forwardRef, useEffect, useImperativeHandle, useLayoutEffect, useMemo, useRef, useState } from "react";
import { aggregateLinks, buildThicknessScale, type AggregatedLink } from "./demand-network/graphUtils";

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

export type HoveredLink = {
  id: string;
  source: string;
  target: string;
  type: string;
  w_recall_raw?: number | null;
  w_consideration_raw?: number | null;
  w_purchase_raw?: number | null;
  n_base?: number | null;
  countStudies?: number;
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
  metricMode: "recall" | "consideration" | "purchase";
  clusterMode?: "off" | "category";
  onHoverNode?: (node: NetworkNode | null) => void;
  onHoverLink?: (link: HoveredLink | null) => void;
  onSelectNode?: (node: NetworkNode | null) => void;
  labelMode?: "auto" | "off";
  spotlight?: boolean;
  activeNodeId?: string | null;
  activeLinkId?: string | null;
  selectedNodeId?: string | null;
  selectedBrandsCount?: number;
  lockedBrandIds?: string[];
  showSecondaryAlways?: boolean;
  layoutMode?: "auto" | "spacious" | "radial" | "bipartite" | "cluster";
  pulseNodeId?: string | null;
  height?: string;
};

export type NetworkCanvasHandle = {
  fitToView: () => void;
  exportSnapshot: () => string | null;
};

type LayoutStrategy = "radial" | "bipartite" | "cluster";

type ViewportSize = {
  width: number;
  height: number;
};

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

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

const getPrimaryMetricLabel = (metric: "recall" | "consideration" | "purchase") => {
  if (metric === "consideration") return "Consideration (given recall of touchpoint)";
  if (metric === "purchase") return "Purchase";
  return "Recall";
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

const sortByLabel = (a: NetworkNode, b: NetworkNode) => a.label.localeCompare(b.label);
const getLinkId = (link: { source: string; target: string; type: string }) =>
  `${link.source}::${link.target}::${link.type}`;

const weightedMetricFromLinks = (
  links: AggregatedLink[],
  metric: "recall" | "consideration" | "purchase"
) => {
  let weightedSum = 0;
  let totalWeight = 0;
  for (const link of links) {
    if (link.type !== "primary_tp_brand") continue;
    const value =
      metric === "purchase"
        ? link.w_purchase_raw
        : metric === "consideration"
          ? link.w_consideration_raw
          : link.w_recall_raw;
    if (typeof value !== "number") continue;
    const w = typeof link.n_base === "number" && link.n_base > 0 ? link.n_base : 1;
    weightedSum += value * w;
    totalWeight += w;
  }
  return totalWeight > 0 ? weightedSum / totalWeight : null;
};

const buildTargetPositions = (
  nodes: NetworkNode[],
  links: AggregatedLink[],
  layout: LayoutStrategy,
  density: "auto" | "spacious",
  clusterMode: "off" | "category"
) => {
  const positions = new Map<string, [number, number]>();
  const brandNodes = nodes.filter((node) => node.type === "brand").slice().sort(sortByLabel);
  const touchpointNodes = nodes.filter((node) => node.type === "touchpoint").slice().sort(sortByLabel);
  const otherNodes = nodes.filter((node) => node.type !== "brand" && node.type !== "touchpoint");
  const primaryLinks = links.filter((link) => link.type === "primary_tp_brand");
  const spacing = density === "spacious" ? 1.18 : 1;

  const brandTouchpointWeight = new Map<string, number>();
  const touchpointBrandWeight = new Map<string, number>();
  const brandNeighbors = new Map<string, Set<string>>();
  for (const link of primaryLinks) {
    const keyBT = `${link.target}||${link.source}`;
    brandTouchpointWeight.set(
      keyBT,
      (brandTouchpointWeight.get(keyBT) || 0) + (link.w_recall_raw || link.w_consideration_raw || link.weight || 0)
    );
    const keyTB = `${link.source}||${link.target}`;
    touchpointBrandWeight.set(
      keyTB,
      (touchpointBrandWeight.get(keyTB) || 0) + (link.w_recall_raw || link.w_consideration_raw || link.weight || 0)
    );
    if (!brandNeighbors.has(link.target)) brandNeighbors.set(link.target, new Set());
    brandNeighbors.get(link.target)?.add(link.source);
  }

  if (layout === "radial") {
    const centerBrand =
      brandNodes.slice().sort((a, b) => (brandNeighbors.get(b.id)?.size || 0) - (brandNeighbors.get(a.id)?.size || 0))[0] ||
      brandNodes[0];
    if (centerBrand) positions.set(centerBrand.id, [0, 0]);

    const connectedTps = touchpointNodes
      .slice()
      .sort(
        (a, b) =>
          (brandTouchpointWeight.get(`${centerBrand?.id || ""}||${b.id}`) || 0) -
          (brandTouchpointWeight.get(`${centerBrand?.id || ""}||${a.id}`) || 0)
      );
    const ringRadius = Math.max(180, 240 * spacing + connectedTps.length * 2.2);
    connectedTps.forEach((tp, index) => {
      const angle = (Math.PI * 2 * index) / Math.max(1, connectedTps.length);
      positions.set(tp.id, [Math.cos(angle) * ringRadius, Math.sin(angle) * ringRadius]);
    });

    const outerBrands = brandNodes.filter((node) => node.id !== centerBrand?.id);
    outerBrands.forEach((brand, idx) => {
      const angle = (Math.PI * 2 * idx) / Math.max(1, outerBrands.length);
      positions.set(brand.id, [Math.cos(angle) * ringRadius * 1.55, Math.sin(angle) * ringRadius * 1.55]);
    });
  } else if (layout === "bipartite") {
    const brandOrder = brandNodes
      .slice()
      .sort((a, b) => (brandNeighbors.get(b.id)?.size || 0) - (brandNeighbors.get(a.id)?.size || 0));
    const brandY = new Map<string, number>();
    const brandSpacing = Math.max(96, (480 * spacing) / Math.max(1, brandOrder.length));
    const brandStart = -((brandOrder.length - 1) * brandSpacing) / 2;
    brandOrder.forEach((brand, idx) => {
      const y = brandStart + idx * brandSpacing;
      brandY.set(brand.id, y);
      positions.set(brand.id, [-320 * spacing, y]);
    });

    const touchpointOrder = touchpointNodes
      .slice()
      .sort((a, b) => {
        const barycenter = (node: NetworkNode) => {
          const linkedBrands = primaryLinks
            .filter((link) => link.source === node.id)
            .map((link) => link.target)
            .filter((id) => brandY.has(id));
          if (!linkedBrands.length) return Number.POSITIVE_INFINITY;
          const avg = linkedBrands.reduce((acc, id) => acc + (brandY.get(id) || 0), 0) / linkedBrands.length;
          return avg;
        };
        const byA = barycenter(a);
        const byB = barycenter(b);
        if (byA === byB) return a.label.localeCompare(b.label);
        return byA - byB;
      });
    const tpSpacing = Math.max(78, (520 * spacing) / Math.max(1, touchpointOrder.length));
    const tpStart = -((touchpointOrder.length - 1) * tpSpacing) / 2;
    touchpointOrder.forEach((tp, idx) => {
      positions.set(tp.id, [320 * spacing, tpStart + idx * tpSpacing]);
    });
  } else {
    const brandCenterBase: [number, number] = [-170 * spacing, -36 * spacing];
    const touchpointCenter: [number, number] = [190 * spacing, 58 * spacing];
    const categoryKeys = Array.from(new Set(brandNodes.map((node) => node.category || "Unknown"))).sort();
    const categoryCenter = new Map<string, [number, number]>();
    categoryKeys.forEach((key, idx) => {
      const angle = (Math.PI * 2 * idx) / Math.max(1, categoryKeys.length);
      const radius = clusterMode === "category" ? 160 * spacing : 90 * spacing;
      categoryCenter.set(key, [brandCenterBase[0] + Math.cos(angle) * radius, brandCenterBase[1] + Math.sin(angle) * radius]);
    });

    brandNodes.forEach((brand, idx) => {
      const key = brand.category || "Unknown";
      const center = categoryCenter.get(key) || brandCenterBase;
      const localAngle = ((hashString(brand.id) % 360) * Math.PI) / 180;
      const localRadius = 36 + (idx % 4) * 18;
      positions.set(brand.id, [
        center[0] + Math.cos(localAngle) * localRadius * spacing,
        center[1] + Math.sin(localAngle) * localRadius * spacing,
      ]);
    });

    touchpointNodes.forEach((tp, idx) => {
      const angle = ((hashString(tp.id) % 360) * Math.PI) / 180;
      const radius = 150 * spacing + (idx % 7) * 9;
      positions.set(tp.id, [touchpointCenter[0] + Math.cos(angle) * radius, touchpointCenter[1] + Math.sin(angle) * radius]);
    });
  }

  otherNodes.forEach((node, idx) => {
    const angle = ((hashString(node.id) % 360) * Math.PI) / 180;
    const radius = 340 + idx * 16;
    positions.set(node.id, [Math.cos(angle) * radius, Math.sin(angle) * radius]);
  });

  // Guard against invalid coordinates.
  positions.forEach((value, key) => {
    if (!Number.isFinite(value[0]) || !Number.isFinite(value[1])) {
      positions.set(key, [0, 0]);
    }
  });

  return positions;
};

export const NetworkCanvas = forwardRef<NetworkCanvasHandle, NetworkCanvasProps>(function NetworkCanvas(
  {
    nodes,
    links,
    metricMode,
    clusterMode = "off",
    onHoverNode,
    onHoverLink,
    onSelectNode,
    labelMode = "auto",
    spotlight = true,
    activeNodeId,
    activeLinkId,
    selectedNodeId,
    selectedBrandsCount = 0,
    lockedBrandIds = [],
    showSecondaryAlways = false,
    layoutMode = "auto",
    pulseNodeId,
    height = "78vh",
  }: NetworkCanvasProps,
  ref
) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<ReactECharts>(null);
  const [frozenPositions, setFrozenPositions] = useState<Map<string, [number, number]> | null>(null);
  const [viewportSize, setViewportSize] = useState<ViewportSize>({ width: 0, height: 0 });
  const lockedBrandAnchorsRef = useRef<Map<string, { x: number; y: number }>>(new Map());

  const debugLayoutEnabled =
    process.env.NODE_ENV !== "production" &&
    typeof window !== "undefined" &&
    Boolean((window as any).__BBS_DEBUG_LAYOUT__);

  const fitGraphToBounds = (reason: "manual" | "auto" = "manual") => {
    const instance = chartRef.current?.getEchartsInstance();
    if (!instance) return;
    instance.resize();
    const width = instance.getWidth();
    const height = instance.getHeight();
    if (width > 0 && height > 0) {
      setViewportSize((prev) => (prev.width === width && prev.height === height ? prev : { width, height }));
    }
    instance.getZr().refreshImmediately();
    if (debugLayoutEnabled) {
      console.debug("[BBS] fitGraphToBounds", { reason, width, height });
    }
  };

  const aggregatedLinks = useMemo(() => aggregateLinks(links), [links]);
  const brandCountInView = useMemo(() => nodes.filter((node) => node.type === "brand").length, [nodes]);
  const resolvedLayoutMode = useMemo<LayoutStrategy>(() => {
    if (layoutMode === "radial" || layoutMode === "bipartite" || layoutMode === "cluster") return layoutMode;
    if (layoutMode === "spacious") return "cluster";
    const count = selectedBrandsCount > 0 ? selectedBrandsCount : brandCountInView;
    if (count <= 1) return "radial";
    if (count <= 5) return "bipartite";
    return "cluster";
  }, [layoutMode, selectedBrandsCount, brandCountInView]);
  const targetPositions = useMemo(
    () =>
      buildTargetPositions(
        nodes,
        aggregatedLinks,
        resolvedLayoutMode,
        layoutMode === "spacious" ? "spacious" : "auto",
        clusterMode
      ),
    [aggregatedLinks, clusterMode, layoutMode, nodes, resolvedLayoutMode]
  );

  const datasetKey = useMemo(
    () =>
      `${nodes.length}:${links.length}:${nodes
        .map((node) => node.id)
        .slice(0, 12)
        .join("|")}:${links
        .map((link) => `${link.source}->${link.target}:${link.type}`)
        .slice(0, 18)
        .join("|")}:${resolvedLayoutMode}:${layoutMode}:${selectedBrandsCount}`,
    [layoutMode, links, nodes, resolvedLayoutMode, selectedBrandsCount]
  );

  useEffect(() => {
    // Keep node positions deterministic and frozen per layout/data change.
    setFrozenPositions(new Map(targetPositions));
  }, [targetPositions]);

  useEffect(() => {
    if (process.env.NODE_ENV === "production") return;
    if (!debugLayoutEnabled) return;
    console.debug("[BBS] demand-network layout", {
      selectedBrandsCount,
      resolvedLayoutMode,
      nodes: nodes.length,
      links: links.length,
      datasetKey,
    });
  }, [datasetKey, debugLayoutEnabled, links.length, nodes.length, resolvedLayoutMode, selectedBrandsCount]);

  useLayoutEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    const measure = () => {
      const rect = container.getBoundingClientRect();
      const width = Math.max(0, Math.floor(rect.width));
      const height = Math.max(0, Math.floor(rect.height));
      if (width <= 0 || height <= 0) return;
      setViewportSize((prev) => (prev.width === width && prev.height === height ? prev : { width, height }));
    };
    measure();
    const observer = new ResizeObserver(() => {
      measure();
    });
    observer.observe(container);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (viewportSize.width <= 0 || viewportSize.height <= 0) return;
    fitGraphToBounds("auto");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [datasetKey, viewportSize.width, viewportSize.height]);

  useImperativeHandle(
    ref,
    () => ({
      fitToView: () => {
        fitGraphToBounds();
      },
      exportSnapshot: () => {
        const instance = chartRef.current?.getEchartsInstance();
        if (!instance) return null;
        return instance.getDataURL({
          pixelRatio: 2,
          backgroundColor: "#f8fafc",
        });
      },
    }),
    [layoutMode]
  );

  const fittedPositions = useMemo(() => {
    const source = frozenPositions && frozenPositions.size ? frozenPositions : targetPositions;
    if (!source || !source.size) return source;
    if (viewportSize.width <= 0 || viewportSize.height <= 0) return source;

    const padding = layoutMode === "spacious" ? 52 : 38;
    let minX = Number.POSITIVE_INFINITY;
    let minY = Number.POSITIVE_INFINITY;
    let maxX = Number.NEGATIVE_INFINITY;
    let maxY = Number.NEGATIVE_INFINITY;
    source.forEach(([x, y]) => {
      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x);
      maxY = Math.max(maxY, y);
    });

    if (!Number.isFinite(minX) || !Number.isFinite(minY) || !Number.isFinite(maxX) || !Number.isFinite(maxY)) {
      return source;
    }

    const spanX = Math.max(1, maxX - minX);
    const spanY = Math.max(1, maxY - minY);
    const usableW = Math.max(1, viewportSize.width - padding * 2);
    const usableH = Math.max(1, viewportSize.height - padding * 2);
    const scale = clamp(Math.min(usableW / spanX, usableH / spanY), 0.45, 2.8);
    const centerX = (minX + maxX) / 2;
    const centerY = (minY + maxY) / 2;
    const targetX = viewportSize.width / 2;
    const targetY = viewportSize.height / 2;

    const map = new Map<string, [number, number]>();
    source.forEach(([x, y], id) => {
      map.set(id, [(x - centerX) * scale + targetX, (y - centerY) * scale + targetY]);
    });

    if (lockedBrandIds.length) {
      for (const id of lockedBrandIds) {
        const anchor = lockedBrandAnchorsRef.current.get(id);
        if (!anchor || !map.has(id)) continue;
        map.set(id, [anchor.x * viewportSize.width, anchor.y * viewportSize.height]);
      }
    }

    if (debugLayoutEnabled) {
      console.debug("[BBS] fittedPositions", {
        viewport: viewportSize,
        mode: resolvedLayoutMode,
        bounds: { minX, minY, maxX, maxY },
        scale,
      });
    }
    return map;
  }, [
    debugLayoutEnabled,
    frozenPositions,
    layoutMode,
    lockedBrandIds,
    resolvedLayoutMode,
    targetPositions,
    viewportSize,
  ]);

  const isViewportReady = viewportSize.width > 0 && viewportSize.height > 0;

  useEffect(() => {
    const anchorMap = lockedBrandAnchorsRef.current;
    const availableBrandIds = new Set(nodes.filter((node) => node.type === "brand").map((node) => node.id));
    const lockedSet = new Set(lockedBrandIds.filter((id) => availableBrandIds.has(id)));
    for (const id of Array.from(anchorMap.keys())) {
      if (!lockedSet.has(id)) {
        anchorMap.delete(id);
      }
    }
    if (!isViewportReady) return;
    for (const id of lockedSet) {
      if (anchorMap.has(id)) continue;
      const pos = fittedPositions.get(id);
      if (!pos) continue;
      anchorMap.set(id, {
        x: pos[0] / viewportSize.width,
        y: pos[1] / viewportSize.height,
      });
    }
  }, [fittedPositions, isViewportReady, lockedBrandIds, nodes, viewportSize.height, viewportSize.width]);

  const option = useMemo(() => {
    const categories = Array.from(new Set(nodes.map((node) => node.group))).map((name) => ({
      name,
    }));
    const labelById = new Map(nodes.map((node) => [node.id, node.label]));
    const nodeById = new Map(nodes.map((node) => [node.id, node]));
    const shortLabel = (value: string) => value.replace(/^[^:]+:/, "");
    const topBrands = nodes
      .filter((node) => node.type === "brand")
      .slice()
      .sort((a, b) => b.size - a.size)
      .slice(0, 10)
      .map((node) => node.id);
    const topTouchpoints = nodes
      .filter((node) => node.type === "touchpoint")
      .slice()
      .sort((a, b) => b.size - a.size)
      .slice(0, 6)
      .map((node) => node.id);

    const degreeMap = new Map<string, number>();
    const neighborMap = new Map<string, Set<string>>();
    const connectedSecondaryLinksByNode = new Map<string, Set<string>>();

    const addNeighbor = (a: string, b: string) => {
      if (!neighborMap.has(a)) neighborMap.set(a, new Set());
      neighborMap.get(a)?.add(b);
    };

    const addSecondaryRef = (nodeId: string, ref: string) => {
      if (!connectedSecondaryLinksByNode.has(nodeId)) {
        connectedSecondaryLinksByNode.set(nodeId, new Set());
      }
      connectedSecondaryLinksByNode.get(nodeId)?.add(ref);
    };

    for (const link of aggregatedLinks) {
      degreeMap.set(link.source, (degreeMap.get(link.source) || 0) + 1);
      degreeMap.set(link.target, (degreeMap.get(link.target) || 0) + 1);
      addNeighbor(link.source, link.target);
      addNeighbor(link.target, link.source);
      if (link.type.startsWith("secondary_")) {
        const ref = `${link.source}::${link.target}::${link.type}`;
        addSecondaryRef(link.source, ref);
        addSecondaryRef(link.target, ref);
      }
    }
    const degrees = Array.from(degreeMap.values()).sort((a, b) => a - b);
    const degreeThreshold =
      degrees.length > 1 ? degrees[Math.floor((degrees.length - 1) * 0.7)] : 0;
    const topBrandsSet = new Set(topBrands);
    const topTouchpointsSet = new Set(topTouchpoints);
    const lockedBrandSet = new Set(lockedBrandIds);
    const activeFocusNodeId = selectedNodeId || activeNodeId || null;
    const focusNeighbors = activeFocusNodeId ? neighborMap.get(activeFocusNodeId) || new Set<string>() : null;
    const activeLinkNodeIds = new Set<string>();
    if (activeLinkId) {
      for (const link of aggregatedLinks) {
        if (getLinkId(link) === activeLinkId) {
          activeLinkNodeIds.add(link.source);
          activeLinkNodeIds.add(link.target);
          break;
        }
      }
    }
    const shouldDimWithSpotlight = Boolean(spotlight && (activeFocusNodeId || activeLinkId));

    const connectionMap = new Map<string, { incoming: AggregatedLink[]; outgoing: AggregatedLink[] }>();
    for (const link of aggregatedLinks) {
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

    const isSecondaryLink = (link: AggregatedLink) => link.type.startsWith("secondary_");
    const secondaryMode = (link: AggregatedLink): "consideration" | "purchase" | "recall" => {
      if (link.type.includes("purchase")) return "purchase";
      if (link.type.includes("consideration")) return "consideration";
      return "recall";
    };

    const thicknessScale = buildThicknessScale(
      aggregatedLinks.map((link) =>
        metricMode === "purchase"
          ? link.w_purchase_raw
          : metricMode === "consideration"
            ? link.w_consideration_raw
            : link.w_recall_raw
      )
    );

    const buildLinkStyle = (
      link: AggregatedLink,
      mode: "consideration" | "purchase" | "recall",
      secondary: boolean
    ) => {
      const weight =
        mode === "purchase"
          ? link.w_purchase_norm
          : mode === "consideration"
            ? link.w_consideration_norm
            : link.w_recall_norm;
      const linkRef = getLinkId(link);
      const isFocusRelated = activeFocusNodeId
        ? link.source === activeFocusNodeId ||
          link.target === activeFocusNodeId ||
          (focusNeighbors?.has(link.source) && focusNeighbors?.has(link.target))
        : activeLinkId
          ? linkRef === activeLinkId
          : false;
      const secondaryConnectedToFocus = activeFocusNodeId
        ? connectedSecondaryLinksByNode.get(activeFocusNodeId)?.has(linkRef)
        : false;
      const metricRaw =
        mode === "purchase"
          ? link.w_purchase_raw
          : mode === "consideration"
            ? link.w_consideration_raw
            : link.w_recall_raw;
      const scaledPrimaryWidth = thicknessScale(metricRaw);
      const baseWidth = secondary ? clamp(Math.max(0.28, scaledPrimaryWidth * 0.34), 0.7, 1.6) : scaledPrimaryWidth;
      const baseOpacity = (secondary ? 0.2 : 0.28) + (weight || 0) * (secondary ? 0.2 : 0.52);
      let width = baseWidth;
      let opacity = baseOpacity;

      if (secondary) {
        if (!showSecondaryAlways && !secondaryConnectedToFocus) {
          opacity = shouldDimWithSpotlight ? 0.06 : 0.14;
          width = 0.7;
        } else if (secondaryConnectedToFocus) {
          opacity = Math.min(0.62, baseOpacity + 0.18);
          width = Math.min(2, baseWidth + 0.35);
        }
      }

      if (shouldDimWithSpotlight && !isFocusRelated && !(secondary && secondaryConnectedToFocus)) {
        opacity = Math.min(opacity, secondary ? 0.06 : 0.1);
      } else if (shouldDimWithSpotlight && isFocusRelated) {
        opacity = Math.max(opacity, secondary ? 0.36 : 0.82);
        width += secondary ? 0.4 : 0.65;
      }

      return {
        width,
        opacity,
        color: secondary ? "rgba(15, 23, 42, 0.6)" : "#0f172a",
        type: secondary ? "dashed" : "solid",
        dashOffset: 0,
      };
    };

    const renderLinks: Array<AggregatedLink & { lineStyle: Record<string, unknown>; emphasis: Record<string, unknown> }> =
      [];

    const pushLink = (link: AggregatedLink, mode: "consideration" | "purchase" | "recall") => {
      const secondary = isSecondaryLink(link);
      renderLinks.push({
        ...link,
        lineStyle: buildLinkStyle(link, mode, secondary),
        emphasis: {
          lineStyle: {
            opacity: secondary ? 0.6 : 0.9,
            width: secondary ? 2.5 : 4.5,
            type: secondary ? "dashed" : "solid",
          },
        },
      });
    };

    const primaryLinks = aggregatedLinks.filter((link) => !isSecondaryLink(link));
    const secondaryLinks = aggregatedLinks.filter((link) => isSecondaryLink(link));

    if (metricMode === "purchase") {
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
      animation: false,
      stateAnimation: { duration: 0 },
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
            const primaryMetricLabel = getPrimaryMetricLabel(metricMode);
            const primaryMetricValue =
              metricMode === "purchase"
                ? purchase
                : metricMode === "consideration"
                  ? consideration
                  : recall;
            const base = params.data.n_base ?? "--";
            const studies = params.data.countStudies ?? "--";
            if (params.data.type?.startsWith("secondary_")) {
              const metricLabel = params.data.type.includes("purchase")
                ? "Purchase layer: P(B | A)"
                : params.data.type.includes("consideration")
                  ? "Consideration layer: P(B | A)"
                  : "Recall layer: P(Y | X)";
              const value =
                params.data.type.includes("purchase")
                  ? purchase
                  : params.data.type.includes("consideration")
                    ? consideration
                    : recall;
              const meta = params.data.colorMeta || {};
              const coCount = meta.co_count ?? "--";
              const baseA = meta.base_a ?? "--";
              const baseB = meta.base_b ?? "--";
              return `${source} ↔ ${target}<br/>${metricLabel}: ${value}<br/>Co-count: ${coCount}<br/>Base A: ${baseA} · Base B: ${baseB}<br/>Studies: ${studies}`;
            }
            const extraLines = [];
            if (metricMode !== "recall") extraLines.push(`Recall: ${recall}`);
            if (metricMode !== "consideration") extraLines.push(`Consideration: ${consideration}`);
            if (metricMode !== "purchase") extraLines.push(`Purchase: ${purchase}`);
            return `${source} -> ${target}<br/>${primaryMetricLabel}: ${primaryMetricValue}${
              extraLines.length ? `<br/>${extraLines.join("<br/>")}` : ""
            }<br/>Base N: ${base}<br/>Studies: ${studies}`;
          }
          const node: NetworkNode | undefined = nodeById.get(params.data.id);
          if (!node) return "";
          if (node.type === "brand") {
            const awareness = node.colorMeta?.kpi_awareness as number | undefined;
            const weightedConsideration = weightedMetricFromLinks(
              (connectionMap.get(node.id)?.incoming || []).filter((link) => link.type === "primary_tp_brand"),
              "consideration"
            );
            const weightedPurchase = weightedMetricFromLinks(
              (connectionMap.get(node.id)?.incoming || []).filter((link) => link.type === "primary_tp_brand"),
              "purchase"
            );
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
            const primaryBrandMetricLabel =
              metricMode === "consideration"
                ? "Consideration (weighted)"
                : metricMode === "purchase"
                  ? "Purchase (weighted)"
                  : "Awareness";
            const primaryBrandMetricValue =
              metricMode === "consideration"
                ? formatPct01(weightedConsideration)
                : metricMode === "purchase"
                  ? formatPct01(weightedPurchase)
                  : formatPct100(awareness);
            return `${node.label}<br/>${primaryBrandMetricLabel}: ${primaryBrandMetricValue}<br/>Base N: ${base ?? "--"}${
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
          progressive: 0,
          progressiveThreshold: 0,
          hoverLayerThreshold: Infinity,
          layout: "none",
          roam: false,
          draggable: true,
          data: isViewportReady
            ? nodes.map((node) => {
            const paletteKey = (node.colorMeta?.paletteKey as string | undefined) || node.context_key || null;
            const haloKey = (node.colorMeta?.haloKey as string | undefined) || node.halo_key || null;
            const fillColor =
              node.group === "brand"
                ? getPaletteColor(paletteKey, "#0ea5a4")
                : node.group === "touchpoint"
                  ? "#1f2937"
                  : "#10b981";
            const haloColor = node.group === "brand" ? getHaloColor(haloKey, fillColor) : "transparent";
            const displaySize = node.group === "brand" ? node.size + 6 : node.size;
            const isTopLabel =
              labelMode === "auto" &&
              (topBrandsSet.has(node.id) ||
                topTouchpointsSet.has(node.id) ||
                (degreeMap.get(node.id) || 0) >= degreeThreshold);
            const isBrand = node.group === "brand";
            const isLockedBrand = isBrand && lockedBrandSet.has(node.id);
            const isFocusNode = activeFocusNodeId === node.id;
            const isFocusNeighbor = Boolean(activeFocusNodeId && focusNeighbors?.has(node.id));
            const isActiveLinkEndpoint = Boolean(activeLinkId && activeLinkNodeIds.has(node.id));
            const showSpotlightLabel = Boolean(
              labelMode === "auto" && (isFocusNode || isFocusNeighbor || isActiveLinkEndpoint)
            );
            const showLabel = isTopLabel || showSpotlightLabel;
            let nodeOpacity = 1;
            if (shouldDimWithSpotlight && !isFocusNode && !isFocusNeighbor && !isActiveLinkEndpoint) {
              nodeOpacity = 0.14;
            } else if (shouldDimWithSpotlight && (isFocusNeighbor || isActiveLinkEndpoint)) {
              nodeOpacity = 0.94;
            }
            const ringOpacity = isLockedBrand ? 0.82 : isFocusNode ? 0.62 : isFocusNeighbor ? 0.44 : 0.28;
            const ringWidth = isLockedBrand ? 8 : isFocusNode ? 7 : 5;
            const labelBackgroundAlpha = isBrand ? 0.92 : 0.86;

            const nodeValue = displaySize * (isBrand ? 1.2 : 1);
            const display = fittedPositions.get(node.id);
            return {
              id: node.id,
              name: node.label,
              label: {
                show: showLabel,
                backgroundColor: `rgba(255,255,255,${labelBackgroundAlpha})`,
                borderRadius: 12,
                padding: [4, 8],
                color: isBrand ? "#0f172a" : "#334155",
                fontSize: isBrand ? 12 : 11,
                fontWeight: isBrand ? 600 : 500,
                position: isBrand ? "top" : "right",
                distance: isBrand ? 7 : 9,
                opacity: showLabel || isLockedBrand ? 1 : 0,
              },
              labelLine: { show: false },
              emphasis: {
                label: {
                  show: true,
                  color: "#0f172a",
                  backgroundColor: "rgba(255,255,255,0.95)",
                  borderRadius: 12,
                  padding: [4, 8],
                },
                labelLine: { show: false },
                itemStyle: {
                  borderWidth: node.group === "brand" ? (isLockedBrand ? 8 : 6) : 0,
                  borderColor:
                    node.group === "brand"
                      ? isLockedBrand
                        ? "rgba(245, 158, 11, 0.85)"
                        : hexToRgba(haloColor, 0.55)
                      : haloColor,
                  shadowBlur: isLockedBrand ? 28 : 22,
                },
              },
              symbol: node.group === "touchpoint" ? "roundRect" : "circle",
              symbolSize: isLockedBrand ? displaySize + 2 : displaySize,
              value: nodeValue,
              category: categories.findIndex((category) => category.name === node.group),
              ...(display
                ? {
                    x: display[0],
                    y: display[1],
                  }
                : {}),
              fixed: true,
              itemStyle: {
                color: fillColor,
                opacity: nodeOpacity,
                borderColor:
                  node.group === "brand"
                    ? isLockedBrand
                      ? "rgba(245, 158, 11, 0.85)"
                      : hexToRgba(haloColor, ringOpacity)
                    : haloColor,
                borderWidth: node.group === "brand" ? ringWidth : 0,
                borderType: node.group === "brand" ? (isLockedBrand ? "solid" : "dashed") : "solid",
                shadowBlur: isLockedBrand ? 24 : node.id === pulseNodeId ? 26 : 12,
                shadowColor: "rgba(15, 23, 42, 0.2)",
              },
            };
            })
            : [],
          links: isViewportReady
            ? renderLinks.map((link, idx) => ({
            id: getLinkId(link),
            source: link.source,
            target: link.target,
            w_recall_raw: link.w_recall_raw,
            w_consideration_raw: link.w_consideration_raw,
            w_purchase_raw: link.w_purchase_raw,
            n_base: link.n_base,
            countStudies: link.countStudies,
            type: link.type,
            colorMeta: link.colorMeta,
            lineStyle: {
              ...link.lineStyle,
              curveness:
                ((link.source.length + link.target.length + idx) % 20 - 10) / 70 +
                ((link.w_consideration_norm ?? link.w_purchase_norm ?? link.w_recall_norm ?? 0) * 0.15),
            },
            emphasis: link.emphasis,
            }))
            : [],
          force: undefined,
          emphasis: shouldDimWithSpotlight ? { focus: "adjacency", blurScope: "global" } : { focus: "none" },
          blur: shouldDimWithSpotlight
            ? {
                itemStyle: { opacity: 0.15 },
                lineStyle: { opacity: 0.04 },
              }
            : undefined,
          labelLayout: { hideOverlap: true, moveOverlap: "shiftX" },
        },
      ],
    };
  }, [
    nodes,
    aggregatedLinks,
    metricMode,
    clusterMode,
    labelMode,
    spotlight,
    activeNodeId,
    activeLinkId,
    selectedNodeId,
    showSecondaryAlways,
    layoutMode,
    pulseNodeId,
    fittedPositions,
    isViewportReady,
  ]);

  return (
    <div ref={containerRef} style={{ height, width: "100%" }}>
      <ReactECharts
        ref={chartRef}
        option={option}
        lazyUpdate={false}
        opts={{ renderer: "canvas", useDirtyRect: false } as any}
        style={{ height: "100%", width: "100%" }}
        onEvents={{
          globalout: () => {
            onHoverNode?.(null);
            onHoverLink?.(null);
          },
          mouseover: (params: any) => {
            if (params.dataType === "node") {
              const node = nodes.find((item) => item.id === params.data.id) || null;
              onHoverNode?.(node);
              onHoverLink?.(null);
            } else if (params.dataType === "edge") {
              onHoverNode?.(null);
              onHoverLink?.({
                id: params.data.id || `${params.data.source}::${params.data.target}::${params.data.type}`,
                source: params.data.source,
                target: params.data.target,
                type: params.data.type,
                w_recall_raw: params.data.w_recall_raw,
                w_consideration_raw: params.data.w_consideration_raw,
                w_purchase_raw: params.data.w_purchase_raw,
                n_base: params.data.n_base,
                countStudies: params.data.countStudies,
              });
            }
          },
          mouseout: () => {
            onHoverNode?.(null);
            onHoverLink?.(null);
          },
          click: (params: any) => {
            if (!onSelectNode) return;
            if (params?.dataType === "node") {
              const node = nodes.find((item) => item.id === params.data.id) || null;
              onSelectNode(node);
            } else {
              onSelectNode(null);
            }
          },
        }}
      />
    </div>
  );
});
