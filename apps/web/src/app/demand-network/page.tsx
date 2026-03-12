"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { createPortal } from "react-dom";

import { getApiBaseUrl } from "../../lib/api";
import { type HoveredLink, type NetworkCanvasHandle } from "../../components/NetworkCanvas";
import DemandNetworkView from "../../components/demand-network/views/DemandNetworkView";
import type { DNDistanceMode, DNViewMode } from "../../components/demand-network/views/types";
import {
  ChipToggle,
  ChipToggleGroup,
  Toolbar,
  ToolbarGroup,
} from "../../components/demand-network/ControlsToolbar";
import { aggregateLinks } from "../../components/demand-network/graphUtils";
import { useScope } from "../../components/layout/ScopeProvider";

type NetworkNode = {
  id: string;
  type: string;
  label: string;
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
  w_consideration_raw?: number | null;
  w_purchase_raw?: number | null;
  w_recall_norm?: number | null;
  w_consideration_norm?: number | null;
  w_purchase_norm?: number | null;
  n_base?: number | null;
  colorMeta?: Record<string, unknown> | null;
};

type NetworkResponse = {
  ok: boolean;
  metric: string;
  filters: Record<string, unknown>;
  nodes: NetworkNode[];
  links: NetworkLink[];
  meta: {
    cache_hit: boolean;
    generated_at: string;
    note?: string | null;
    warning?: string | null;
    synthetic?: boolean;
    empty_reason?: string | null;
    node_counts?: { brand?: number; touchpoint?: number };
    link_count?: number;
    link_metric_counts?: { recall?: number; consideration?: number; purchase?: number };
  };
};

const SECONDARY_OPTIONS = ["off", "brands", "touchpoints"] as const;
const METRICS = ["recall", "consideration", "purchase"] as const;
const VIEW_OPTIONS: Array<{ label: string; value: DNViewMode }> = [
  { label: "◎ Network", value: "network" },
  { label: "▦ Matrix", value: "matrix" },
  { label: "⇄ Sankey", value: "sankey" },
  { label: "◫ Multiples", value: "multiples" },
];

type LoadState = "idle" | "loading" | "error";

type MetricKey = (typeof METRICS)[number];
type SecondaryMode = (typeof SECONDARY_OPTIONS)[number];
type InteractionState = {
  hoveredNodeId: string | null;
  hoveredLinkId: string | null;
  focusedNodeId: string | null;
  lockedBrandIds: string[];
};

const metricLabel = (metric: MetricKey) => {
  switch (metric) {
    case "consideration":
      return "Consideration";
    case "purchase":
      return "Purchase";
    default:
      return "Recall";
  }
};

const primaryMetricLabel = (metric: MetricKey) =>
  metric === "consideration" ? "Consideration (given recall of touchpoint)" : metricLabel(metric);

const TOOLTIP_COPY = {
  view: "Switch between advanced analytical views using the same filtered dataset.",
  metric: "Choose the primary metric used to render links.",
  layout: "Adjust network spacing density without changing data.",
  distance: "Set link distance by metric: higher value = shorter touchpoint-to-brand distance.",
  layers: "Show or hide secondary relationship layers.",
} as const;

export default function DemandNetworkPage() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { scope, studies } = useScope();
  const apiBase = getApiBaseUrl();
  const isPresentation = searchParams.get("presentation") === "1";
  const advancedOpen = searchParams.get("scope_advanced") === "1";
  const [metric, setMetric] = useState<MetricKey>("recall");
  const [viewMode, setViewMode] = useState<DNViewMode>("network");
  const [secondaryLinks, setSecondaryLinks] = useState<SecondaryMode>("off");
  const [showSecondaryAlways, setShowSecondaryAlways] = useState(false);
  const [secondaryWarning, setSecondaryWarning] = useState<string | null>(null);
  const [interaction, setInteraction] = useState<InteractionState>({
    hoveredNodeId: null,
    hoveredLinkId: null,
    focusedNodeId: null,
    lockedBrandIds: [],
  });
  const [hoveredLink, setHoveredLink] = useState<HoveredLink | null>(null);
  const [filtersCollapsed, setFiltersCollapsed] = useState(true);
  const [layoutMode, setLayoutMode] = useState<"auto" | "spacious">("spacious");
  const [distanceMode, setDistanceMode] = useState<DNDistanceMode>("off");
  const [graphVisible, setGraphVisible] = useState(false);
  const [pulseNodeId, setPulseNodeId] = useState<string | null>(null);
  const [reloadTick, setReloadTick] = useState(0);
  const [legendOpen, setLegendOpen] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);
  const [includeLegendInExport, setIncludeLegendInExport] = useState(true);
  const [exportMessage, setExportMessage] = useState<string | null>(null);
  const [data, setData] = useState<NetworkResponse | null>(null);
  const [state, setState] = useState<LoadState>("idle");
  const [error, setError] = useState<string | null>(null);
  const [advancedSlot, setAdvancedSlot] = useState<HTMLElement | null>(null);
  const canvasRef = useRef<NetworkCanvasHandle>(null);
  const secondaryInitRef = useRef(false);
  const requestSeqRef = useRef(0);
  const requestAbortRef = useRef<AbortController | null>(null);
  const activeQueryRef = useRef("");

  useEffect(() => {
    if (secondaryInitRef.current) return;
    secondaryInitRef.current = true;
    const incoming = searchParams.get("secondary_links");
    if (!incoming) return;
    if (incoming === "brands" || incoming === "touchpoints" || incoming === "off") {
      setSecondaryLinks(incoming);
      return;
    }
    if (incoming === "both") {
      setSecondaryLinks("off");
      const params = new URLSearchParams(searchParams.toString());
      params.delete("secondary_links");
      router.replace(`${pathname}${params.toString() ? `?${params.toString()}` : ""}`);
    }
  }, [pathname, router, searchParams]);

  useEffect(() => {
    if (typeof document === "undefined") return;
    if (!advancedOpen || isPresentation) {
      setAdvancedSlot(null);
      return;
    }
    const syncSlot = () => {
      setAdvancedSlot(document.getElementById("dn-advanced-controls-slot"));
    };
    syncSlot();
    const frame = window.requestAnimationFrame(syncSlot);
    return () => window.cancelAnimationFrame(frame);
  }, [advancedOpen, isPresentation]);

  const breadcrumb = useMemo(() => {
    if (!scope.category) return "All categories";
    const matched = studies.filter((study) => study.category === scope.category);
    const sectors = new Set(matched.map((study) => study.sector || "Unassigned"));
    const subsectors = new Set(matched.map((study) => study.subsector || "Unassigned"));
    const sectorValue = sectors.size === 1 ? Array.from(sectors)[0] : "Mixed";
    const subsectorValue = subsectors.size === 1 ? Array.from(subsectors)[0] : "Mixed";
    return `${sectorValue} → ${subsectorValue} → ${scope.category}`;
  }, [scope.category, studies]);

  const query = useMemo(() => {
    const params = new URLSearchParams({ metric_mode: metric });
    if (scope.studyIds.length) {
      params.set("study_ids", scope.studyIds.join(","));
    }
    if (scope.sector) params.set("sector", scope.sector);
    if (scope.subsector) params.set("subsector", scope.subsector);
    if (scope.category) {
      params.set("category", scope.category);
    }
    if (scope.brands.length) {
      params.set("brands", scope.brands.join(","));
    }
    if (scope.gender.length) params.set("gender", scope.gender[0]);
    if (scope.nse.length) params.set("nse", scope.nse[0]);
    if (scope.state.length) params.set("state", scope.state[0]);
    if (scope.ageMin !== null) params.set("age_min", String(scope.ageMin));
    if (scope.ageMax !== null) params.set("age_max", String(scope.ageMax));
    if (scope.quarterFrom) params.set("quarter_from", scope.quarterFrom);
    if (scope.quarterTo) params.set("quarter_to", scope.quarterTo);
    if (secondaryLinks !== "off") {
      params.set("secondary_links", secondaryLinks);
    }
    return params.toString();
  }, [metric, scope, secondaryLinks]);

  useEffect(() => {
    const seq = requestSeqRef.current + 1;
    requestSeqRef.current = seq;
    activeQueryRef.current = query;
    requestAbortRef.current?.abort();
    const abortController = new AbortController();
    requestAbortRef.current = abortController;

    const handle = setTimeout(() => {
      const load = async () => {
        setState("loading");
        setError(null);
        try {
          const response = await fetch(`${apiBase}/network?${query}`, { signal: abortController.signal });
          if (
            abortController.signal.aborted ||
            seq !== requestSeqRef.current ||
            activeQueryRef.current !== query
          ) {
            return;
          }
          const payload = (await response.json()) as NetworkResponse;
          if (!response.ok || !payload.ok) {
            throw new Error("Unable to load network data.");
          }
          setData(payload);
          setState("idle");
        } catch (err) {
          if (
            abortController.signal.aborted ||
            seq !== requestSeqRef.current ||
            activeQueryRef.current !== query
          ) {
            return;
          }
          setState("error");
          setError(err instanceof Error ? err.message : "Unable to load network.");
        }
      };

      load();
    }, 250);

    return () => {
      clearTimeout(handle);
      abortController.abort();
    };
  }, [apiBase, query, reloadTick]);

  useEffect(() => {
    if (!data) return;
    const nodeIds = new Set(data.nodes.map((node) => node.id));
    setInteraction((prev) => {
      const next: InteractionState = {
        hoveredNodeId: prev.hoveredNodeId && nodeIds.has(prev.hoveredNodeId) ? prev.hoveredNodeId : null,
        hoveredLinkId: prev.hoveredLinkId,
        focusedNodeId: prev.focusedNodeId && nodeIds.has(prev.focusedNodeId) ? prev.focusedNodeId : null,
        lockedBrandIds: prev.lockedBrandIds,
      };
      if (
        next.hoveredNodeId === prev.hoveredNodeId &&
        next.focusedNodeId === prev.focusedNodeId
      ) {
        return prev;
      }
      return next;
    });
  }, [data]);

  useEffect(() => {
    if (!data) return;
    const brandIds = new Set(data.nodes.filter((node) => node.type === "brand").map((node) => node.id));
    setInteraction((prev) => {
      const nextLocks = prev.lockedBrandIds.filter((id) => brandIds.has(id));
      if (nextLocks.length === prev.lockedBrandIds.length) return prev;
      return { ...prev, lockedBrandIds: nextLocks };
    });
  }, [data]);

  useEffect(() => {
    if (!data) return;
    if (data.nodes.length > 300 && secondaryLinks !== "off") {
      setSecondaryLinks("off");
      setSecondaryWarning("Secondary links were disabled to keep the graph responsive.");
    }
  }, [data, secondaryLinks]);

  const allowsBrandLayer = metric === "consideration" || metric === "purchase";
  const allowsTouchpointLayer = metric === "recall";
  useEffect(() => {
    if (
      (secondaryLinks === "brands" && !allowsBrandLayer) ||
      (secondaryLinks === "touchpoints" && !allowsTouchpointLayer)
    ) {
      setSecondaryLinks("off");
    }
  }, [allowsBrandLayer, allowsTouchpointLayer, secondaryLinks]);

  const layersOptions = useMemo(
    () => [
      { label: "off", value: "off" as const },
      {
        label: "Brand",
        value: "brands" as const,
        disabled: !allowsBrandLayer,
        tooltip: allowsBrandLayer
          ? "Brand layer is available for Consideration and Purchase."
          : "Brand layer is only available for Consideration and Purchase.",
      },
      {
        label: "Touchpoint",
        value: "touchpoints" as const,
        disabled: !allowsTouchpointLayer,
        tooltip: allowsTouchpointLayer
          ? "Touchpoint layer is available for Recall."
          : "Touchpoint layer is only available for Recall.",
      },
    ],
    [allowsBrandLayer, allowsTouchpointLayer]
  );

  useEffect(() => {
    if (!data) return;
    setGraphVisible(false);
    const handle = setTimeout(() => setGraphVisible(true), 500);
    const biggestBrand = data.nodes
      .filter((node) => node.type === "brand")
      .sort((a, b) => b.size - a.size)[0];
    if (biggestBrand) {
      setPulseNodeId(biggestBrand.id);
      const clear = setTimeout(() => setPulseNodeId(null), 1200);
      return () => {
        clearTimeout(handle);
        clearTimeout(clear);
      };
    }
    return () => clearTimeout(handle);
  }, [data]);

  const aggregatedLinks = useMemo(() => (data ? aggregateLinks(data.links) : []), [data]);
  const linkCounts = useMemo(() => {
    if (!data) return { primary: 0, secondary: 0, total: 0 };
    const primary = aggregatedLinks.filter((link) => link.type === "primary_tp_brand").length;
    const secondary = aggregatedLinks.filter((link) => link.type.startsWith("secondary_")).length;
    return { primary, secondary, total: aggregatedLinks.length };
  }, [aggregatedLinks, data]);
  const nodeById = useMemo(() => new Map((data?.nodes || []).map((node) => [node.id, node])), [data]);
  const activeNodeId = interaction.focusedNodeId || interaction.hoveredNodeId || null;
  const activeLinkId = interaction.focusedNodeId ? null : interaction.hoveredLinkId;

  useEffect(() => {
    if (process.env.NODE_ENV === "production" || !data) return;
    const sampleConsideration = aggregateLinks(data.links)
      .filter((link) => typeof link.w_consideration_raw === "number")
      .slice(0, 3)
      .map((link) => ({
        source: link.source,
        target: link.target,
        recall: link.w_recall_raw,
        consideration: link.w_consideration_raw,
      }));
    const nonNull = {
      recall: data.links.filter((link) => typeof link.w_recall_raw === "number").length,
      consideration: data.links.filter((link) => typeof link.w_consideration_raw === "number").length,
      purchase: data.links.filter((link) => typeof link.w_purchase_raw === "number").length,
    };
    // Dev-only visibility to confirm selected metric availability.
    console.debug("[BBS] demand-network metric availability", {
      metric,
      links: data.links.length,
      nonNull,
      sampleConsideration,
    });
  }, [data, metric]);
  const selectedBrandsWithoutLinks = useMemo(() => {
    if (!scope.brands.length || !data) return 0;
    const renderedBrands = new Set(
      data.nodes
        .filter((node) => node.type === "brand")
        .map((node) => node.label.toLowerCase().trim())
    );
    return scope.brands.filter((brand) => !renderedBrands.has(brand.toLowerCase().trim())).length;
  }, [data, scope.brands]);
  const canvasHeight = isPresentation
    ? "clamp(700px, calc(100vh - 120px), 1200px)"
    : "clamp(560px, calc(100vh - 270px), 860px)";

  const neighborIdsByNode = useMemo(() => {
    const map = new Map<string, Set<string>>();
    for (const link of aggregatedLinks) {
      if (!map.has(link.source)) map.set(link.source, new Set<string>());
      if (!map.has(link.target)) map.set(link.target, new Set<string>());
      map.get(link.source)?.add(link.target);
      map.get(link.target)?.add(link.source);
    }
    return map;
  }, [aggregatedLinks]);

  const handleHoverNode = useCallback(
    (node: NetworkNode | null) => {
      setInteraction((prev) => {
        if (prev.focusedNodeId) {
          const allowedNeighbors = neighborIdsByNode.get(prev.focusedNodeId) || new Set<string>();
          const allowed = node && (node.id === prev.focusedNodeId || allowedNeighbors.has(node.id));
          const nextHovered = allowed ? node.id : null;
          if (nextHovered === prev.hoveredNodeId && prev.hoveredLinkId === null) return prev;
          return { ...prev, hoveredNodeId: nextHovered, hoveredLinkId: null };
        }
        const nextHovered = node?.id || null;
        if (nextHovered === prev.hoveredNodeId && prev.hoveredLinkId === null) return prev;
        return { ...prev, hoveredNodeId: nextHovered, hoveredLinkId: null };
      });
      if (node) setHoveredLink(null);
    },
    [neighborIdsByNode]
  );

  const handleHoverLink = useCallback(
    (link: HoveredLink | null) => {
      setHoveredLink(link);
      setInteraction((prev) => {
        if (prev.focusedNodeId) {
          const neighbors = neighborIdsByNode.get(prev.focusedNodeId) || new Set<string>();
          const withinFocus =
            !!link &&
            (link.source === prev.focusedNodeId ||
              link.target === prev.focusedNodeId ||
              (neighbors.has(link.source) && neighbors.has(link.target)));
          const nextLink = withinFocus && link ? link.id : null;
          if (nextLink === prev.hoveredLinkId) return prev;
          return { ...prev, hoveredLinkId: nextLink, hoveredNodeId: null };
        }
        const nextLink = link?.id || null;
        if (nextLink === prev.hoveredLinkId) return prev;
        return { ...prev, hoveredLinkId: nextLink, hoveredNodeId: null };
      });
    },
    [neighborIdsByNode]
  );

  const handleSelectNode = useCallback((node: NetworkNode | null) => {
    setHoveredLink(null);
    setInteraction((prev) => {
      if (!node) {
        if (!prev.focusedNodeId && !prev.hoveredNodeId && !prev.hoveredLinkId) return prev;
        return { ...prev, focusedNodeId: null, hoveredNodeId: null, hoveredLinkId: null };
      }
      const nextFocus = prev.focusedNodeId === node.id ? null : node.id;
      return {
        ...prev,
        focusedNodeId: nextFocus,
        hoveredNodeId: nextFocus ? node.id : null,
        hoveredLinkId: null,
      };
    });
  }, []);

  const toggleBrandLock = useCallback((brandId: string) => {
    setInteraction((prev) => {
      const set = new Set(prev.lockedBrandIds);
      if (set.has(brandId)) {
        set.delete(brandId);
      } else {
        set.add(brandId);
      }
      return { ...prev, lockedBrandIds: Array.from(set) };
    });
  }, []);

  const clearBrandLocks = useCallback(() => {
    setInteraction((prev) => (prev.lockedBrandIds.length ? { ...prev, lockedBrandIds: [] } : prev));
  }, []);

  const setPresentation = useCallback(
    (enabled: boolean) => {
      const params = new URLSearchParams(searchParams.toString());
      if (enabled) {
        params.set("presentation", "1");
      } else {
        params.delete("presentation");
      }
      router.replace(`${pathname}${params.toString() ? `?${params.toString()}` : ""}`);
    },
    [pathname, router, searchParams]
  );

  const handleExportSnapshot = useCallback(() => {
    const dataUrl = canvasRef.current?.exportSnapshot();
    if (!dataUrl) {
      setExportMessage("Snapshot export is not available yet.");
      return;
    }
    const link = document.createElement("a");
    const stamp = new Date().toISOString().replace(/[:.]/g, "-");
    link.href = dataUrl;
    link.download = `bbs-demand-network-${stamp}.png`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    setExportMessage(includeLegendInExport ? "PNG snapshot exported." : "PNG snapshot exported.");
    setExportOpen(false);
  }, [includeLegendInExport]);

  useEffect(() => {
    let resizeTimer: ReturnType<typeof setTimeout> | null = setTimeout(() => {
      canvasRef.current?.fitToView();
    }, 220);
    const onResize = () => {
      if (resizeTimer) {
        clearTimeout(resizeTimer);
      }
      resizeTimer = setTimeout(() => {
        canvasRef.current?.fitToView();
      }, 220);
    };
    window.addEventListener("resize", onResize);
    return () => {
      if (resizeTimer) {
        clearTimeout(resizeTimer);
      }
      window.removeEventListener("resize", onResize);
    };
  }, []);

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape" && isPresentation) {
        setPresentation(false);
        return;
      }
      if (event.key === "Escape") {
        setInteraction((prev) => {
          if (!prev.focusedNodeId) return prev;
          return { ...prev, focusedNodeId: null, hoveredNodeId: null, hoveredLinkId: null };
        });
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [isPresentation, setPresentation]);

  useEffect(() => {
    if (!isPresentation) return;
    const timer = setTimeout(() => {
      canvasRef.current?.fitToView();
    }, 180);
    return () => clearTimeout(timer);
  }, [isPresentation]);

  const focusedNode = interaction.focusedNodeId ? nodeById.get(interaction.focusedNodeId) || null : null;
  const hoveredNode = interaction.hoveredNodeId ? nodeById.get(interaction.hoveredNodeId) || null : null;
  const panelNode = focusedNode || hoveredNode;
  const panelLink = !panelNode ? hoveredLink : null;
  const panelMode = focusedNode ? "focused" : hoveredNode || hoveredLink ? "hover" : "idle";

  const metricValueForLink = useCallback(
    (link: { w_recall_raw?: number | null; w_consideration_raw?: number | null; w_purchase_raw?: number | null }) => {
      if (metric === "purchase") return link.w_purchase_raw ?? 0;
      if (metric === "consideration") return link.w_consideration_raw ?? 0;
      return link.w_recall_raw ?? 0;
    },
    [metric]
  );

  const topConnections = useMemo(() => {
    if (!panelNode) return [];
    return aggregatedLinks
      .filter((link) => link.source === panelNode.id || link.target === panelNode.id)
      .slice()
      .sort((a, b) => metricValueForLink(b) - metricValueForLink(a))
      .slice(0, 5)
      .map((link) => {
        const connectedId = link.source === panelNode.id ? link.target : link.source;
        return {
          id: `${link.source}::${link.target}::${link.type}`,
          label: nodeById.get(connectedId)?.label || connectedId,
          recall: link.w_recall_raw,
          consideration: link.w_consideration_raw,
          purchase: link.w_purchase_raw,
        };
      });
  }, [aggregatedLinks, metricValueForLink, nodeById, panelNode]);

  const advancedControlsContent =
    !isPresentation && advancedOpen ? (
      <Toolbar className="w-full max-w-full">
        <ChipToggleGroup
          label="View"
          tooltip={TOOLTIP_COPY.view}
          value={viewMode}
          options={VIEW_OPTIONS}
          onChange={setViewMode}
        />
        <ChipToggleGroup
          label="Metric"
          tooltip={TOOLTIP_COPY.metric}
          value={metric}
          options={METRICS.map((item) => ({ label: metricLabel(item), value: item }))}
          onChange={setMetric}
        />
        <ChipToggleGroup
          label="Layout"
          tooltip={TOOLTIP_COPY.layout}
          value={layoutMode}
          options={[
            { label: "Auto", value: "auto" as const },
            { label: "Spacious", value: "spacious" as const },
          ]}
          onChange={setLayoutMode}
        />
        <ChipToggleGroup
          label="Distance"
          tooltip={TOOLTIP_COPY.distance}
          value={distanceMode}
          options={[
            { label: "Off", value: "off" as const },
            { label: "Consideration", value: "consideration" as const },
            { label: "Purchase", value: "purchase" as const },
          ]}
          onChange={setDistanceMode}
        />
        <ChipToggleGroup
          label="Layers"
          tooltip={TOOLTIP_COPY.layers}
          value={secondaryLinks}
          options={layersOptions}
          onChange={setSecondaryLinks}
        />
        {secondaryLinks !== "off" && (
          <ChipToggleGroup
            label="Secondary"
            tooltip="Latent keeps secondary links hidden until hover; Always keeps them softly visible."
            value={showSecondaryAlways ? "always" : "latent"}
            options={[
              { label: "Latent", value: "latent" as const },
              { label: "Always", value: "always" as const },
            ]}
            onChange={(value) => setShowSecondaryAlways(value === "always")}
          />
        )}
        <ToolbarGroup label="View" tooltip="Toggle clean demo mode">
          <ChipToggle
            label="Presentation"
            tooltip="Toggle clean demo mode"
            active={isPresentation}
            onClick={() => setPresentation(true)}
          />
        </ToolbarGroup>
      </Toolbar>
    ) : null;

  return (
    <main className={`space-y-6 ${isPresentation ? "pt-1" : ""}`}>
      {advancedControlsContent && advancedSlot ? createPortal(advancedControlsContent, advancedSlot) : null}

      <section className="space-y-4">
        <div className="hidden">
          <div className="flex items-center justify-between">
            <h3 className="text-base font-semibold">Filters</h3>
            <button
              className="rounded-full border border-ink/10 px-3 py-1 text-xs text-slate"
              type="button"
              onClick={() => setFiltersCollapsed((prev) => !prev)}
            >
              {filtersCollapsed ? "Expand" : "Collapse"}
            </button>
          </div>

          {filtersCollapsed ? (
            <div className="mt-4 space-y-2 text-xs text-slate">
              <p>{scope.studyIds.length || "All"} studies selected</p>
              <p>Category: {scope.category || "All"}</p>
            </div>
          ) : (
            <div className="mt-4 space-y-4">
              <div>
                <p className="text-sm font-semibold text-ink">Global Scope</p>
                <p className="text-xs text-slate">
                  Study, taxonomy, demographics and time are managed in the Scope Bar.
                </p>
                <div className="mt-3 rounded-2xl border border-ink/10 bg-white p-3 text-xs text-slate">
                  <p>
                    Studies:{" "}
                    {scope.studyIds.length ? `${scope.studyIds.length} selected` : "All available"}
                  </p>
                  <p className="mt-1">Category: {scope.category || "All categories"}</p>
                  <p className="mt-1">Path: {breadcrumb}</p>
                </div>
              </div>
            </div>
          )}
        </div>

        <div className={`main-surface relative rounded-3xl ${isPresentation ? "p-2 sm:p-3 lg:p-4" : "p-4 sm:p-5 lg:p-6"}`}>
          {!isPresentation && (
          <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
            <div>
              <h3 className="text-xl font-semibold">Demand Network</h3>
              <p className="text-xs text-slate">
                How touchpoints translate into brand demand across categories.
              </p>
              <div className="mt-2 flex flex-wrap items-center gap-3 text-[11px] text-slate">
                {viewMode === "network" && metric === "consideration" && (
                  <span className="flex items-center gap-2">
                    <span className="h-[2px] w-6 bg-ink/70" /> Solid = Consideration
                  </span>
                )}
                {viewMode === "network" && metric === "purchase" && (
                  <span className="flex items-center gap-2">
                    <span className="h-[2px] w-6 bg-ink/70" /> Solid = Purchase
                  </span>
                )}
                {viewMode === "network" && metric === "recall" && (
                  <span className="flex items-center gap-2">
                    <span className="h-[2px] w-6 bg-ink/70" /> Solid = Recall
                  </span>
                )}
                {viewMode === "network" && secondaryLinks !== "off" && (
                  <span className="flex items-center gap-2 text-slate/70">
                    Secondary links are lighter; hover a node to highlight.
                  </span>
                )}
                {viewMode !== "network" && (
                  <span className="text-slate/80">Advanced view: {VIEW_OPTIONS.find((item) => item.value === viewMode)?.label}</span>
                )}
              </div>
            </div>
            {data?.meta && (
              <div className="flex flex-wrap items-center gap-2 text-xs text-slate">
                <span className="rounded-full border border-emerald-500/20 bg-emerald-500/10 px-3 py-1 text-emerald-700">
                  {data.meta.cache_hit ? "Cached" : "Live"}
                </span>
                <span className="rounded-full border border-ink/10 px-3 py-1">Mode: {metricLabel(metric)}</span>
                {data.meta.node_counts && (
                  <span className="rounded-full border border-ink/10 px-3 py-1">
                    Nodes: {data.meta.node_counts.brand || 0} brands / {data.meta.node_counts.touchpoint || 0} tps
                  </span>
                )}
                {interaction.lockedBrandIds.length > 0 && (
                  <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-3 py-1 text-amber-700">
                    Locked brands: {interaction.lockedBrandIds.length}
                  </span>
                )}
                <span className="rounded-full border border-ink/10 px-3 py-1">Links: {linkCounts.total}</span>
              </div>
            )}
          </div>
          )}

          {isPresentation && (
            <div className="pointer-events-none absolute inset-x-4 top-4 z-20 flex items-start justify-between gap-3">
              <div className="pointer-events-auto rounded-2xl border border-white/60 bg-white/75 px-3 py-2 shadow-sm backdrop-blur">
                <p className="text-sm font-semibold text-ink">Demand Network</p>
                <p className="text-[11px] text-slate">{metricLabel(metric)} view</p>
              </div>
              <div className="pointer-events-auto flex items-center gap-2 rounded-2xl border border-white/60 bg-white/75 px-2 py-2 shadow-sm backdrop-blur">
                <button
                  type="button"
                  className="rounded-full border border-ink/10 px-3 py-1 text-xs text-slate hover:border-ink/20"
                  onClick={() => setLegendOpen((prev) => !prev)}
                >
                  Legend
                </button>
                <button
                  type="button"
                  className="rounded-full border border-ink/10 px-3 py-1 text-xs text-slate hover:border-ink/20"
                  onClick={() => setExportOpen(true)}
                >
                  Export Snapshot
                </button>
                <button
                  type="button"
                  className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-3 py-1 text-xs text-emerald-700"
                  onClick={() => setPresentation(false)}
                >
                  Exit Presentation
                </button>
              </div>
            </div>
          )}

          {data?.meta?.warning && (
            <div className="mb-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-xs text-amber-700">
              {data.meta.warning}
            </div>
          )}
          {selectedBrandsWithoutLinks > 0 && (
            <div className="mb-4 rounded-2xl border border-ink/10 bg-white px-4 py-3 text-xs text-slate">
              {selectedBrandsWithoutLinks} selected brand
              {selectedBrandsWithoutLinks === 1 ? "" : "s"} have no links in the current scope.
            </div>
          )}

          {secondaryWarning && (
            <div className="mb-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-xs text-amber-700">
              {secondaryWarning}
            </div>
          )}
          {data?.meta?.empty_reason && (
            <div className="mb-4 rounded-2xl border border-ink/10 bg-white px-4 py-3 text-xs text-slate">
              {data.meta.empty_reason}
            </div>
          )}

          {state === "loading" && (
            <div className="space-y-4">
              <div className="h-6 w-48 animate-pulse rounded-full bg-slate-200" />
              <div
                className="animate-pulse rounded-3xl border border-ink/10 bg-slate-100"
                style={{ height: canvasHeight }}
              />
            </div>
          )}

          {state === "error" && (
            <div className="rounded-3xl border border-red-200 bg-red-50 p-6 text-sm text-red-700">
              <p>{error || "Unable to load demand network."}</p>
              <button
                className="mt-4 rounded-full border border-red-200 px-4 py-2 text-xs"
                type="button"
                onClick={() => setReloadTick((prev) => prev + 1)}
              >
                Retry
              </button>
            </div>
          )}

          {state === "idle" && data && (
            <div
              className={`rounded-[2rem] border border-ink/10 bg-slate-50/60 p-3 transition-opacity duration-700 sm:p-4 ${
                graphVisible ? "opacity-100" : "opacity-0"
              }`}
            >
              {data.meta.synthetic && data.meta.note && (
                <p className="mb-3 text-xs text-slate">{data.meta.note}</p>
              )}
              <DemandNetworkView
                viewMode={viewMode}
                canvasRef={canvasRef}
                nodes={data.nodes}
                links={data.links}
                metricMode={metric}
                clusterMode="off"
                selectedBrandsCount={scope.brands.length}
                showSecondaryAlways={showSecondaryAlways}
                labelMode="auto"
                spotlight={false}
                layoutMode={layoutMode}
                distanceMode={distanceMode}
                pulseNodeId={pulseNodeId}
                height={canvasHeight}
                onHoverNode={handleHoverNode}
                onHoverLink={handleHoverLink}
                onSelectNode={handleSelectNode}
                activeNodeId={activeNodeId}
                activeLinkId={activeLinkId}
                selectedNodeId={interaction.focusedNodeId}
                lockedBrandIds={interaction.lockedBrandIds}
              />
              <aside className="pointer-events-none absolute bottom-5 right-5 z-20 w-[320px] max-w-[calc(100%-2.5rem)]">
                <div className="pointer-events-auto rounded-2xl border border-white/60 bg-white/85 p-4 shadow-lg backdrop-blur">
                  <div className="mb-2 flex items-center justify-between gap-2">
                    <p className="text-xs font-semibold uppercase tracking-wide text-slate">
                      {panelMode === "focused" ? "Focused details" : panelMode === "hover" ? "Hover details" : "Details"}
                    </p>
                    {interaction.focusedNodeId && (
                      <button
                        type="button"
                        className="rounded-full border border-ink/10 px-2 py-1 text-[11px] text-slate hover:border-ink/20"
                        onClick={() =>
                          setInteraction((prev) => ({ ...prev, focusedNodeId: null, hoveredNodeId: null, hoveredLinkId: null }))
                        }
                      >
                        Clear focus
                      </button>
                    )}
                  </div>

                  {panelNode ? (
                    <div className="space-y-2 text-xs text-slate">
                      <p className="text-base font-semibold text-ink">{panelNode.label}</p>
                      {panelNode.type === "brand" && interaction.lockedBrandIds.includes(panelNode.id) && (
                        <p className="text-[11px] font-semibold uppercase tracking-wide text-amber-700">Locked brand</p>
                      )}
                      <p>Type: {panelNode.type === "brand" ? "Brand" : "Touchpoint"}</p>
                      <p>Connections: {aggregatedLinks.filter((link) => link.source === panelNode.id || link.target === panelNode.id).length}</p>
                      {panelNode.type === "brand" && (
                        <div className="flex items-center gap-2">
                          <button
                            type="button"
                            className={`rounded-full border px-2 py-1 text-[11px] ${
                              interaction.lockedBrandIds.includes(panelNode.id)
                                ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-700"
                                : "border-ink/10 text-slate hover:border-ink/20"
                            }`}
                            onClick={() => toggleBrandLock(panelNode.id)}
                          >
                            {interaction.lockedBrandIds.includes(panelNode.id) ? "Unlock brand" : "Lock brand"}
                          </button>
                          {interaction.lockedBrandIds.length > 0 && (
                            <button
                              type="button"
                              className="rounded-full border border-ink/10 px-2 py-1 text-[11px] text-slate hover:border-ink/20"
                              onClick={clearBrandLocks}
                            >
                              Clear locks ({interaction.lockedBrandIds.length})
                            </button>
                          )}
                        </div>
                      )}
                      <div className="pt-1">
                        <p className="mb-1 text-[11px] font-semibold uppercase tracking-wide text-slate">Top connections</p>
                        {topConnections.length ? (
                          <ul className="space-y-1">
                            {topConnections.map((item) => (
                              <li key={item.id} className="rounded-xl border border-ink/10 bg-white/80 px-2 py-1">
                                <p className="truncate text-[12px] font-medium text-ink">{item.label}</p>
                                <p className="text-[11px] text-slate">
                                  R {typeof item.recall === "number" ? `${(item.recall * 100).toFixed(1)}%` : "--"} · C{" "}
                                  {typeof item.consideration === "number"
                                    ? `${(item.consideration * 100).toFixed(1)}%`
                                    : "--"}{" "}
                                  · P {typeof item.purchase === "number" ? `${(item.purchase * 100).toFixed(1)}%` : "--"}
                                </p>
                              </li>
                            ))}
                          </ul>
                        ) : (
                          <p className="text-[11px] text-slate">No connected links in current scope.</p>
                        )}
                      </div>
                    </div>
                  ) : panelLink ? (
                    <div className="space-y-2 text-xs text-slate">
                      <p className="text-base font-semibold text-ink">
                        {nodeById.get(panelLink.source)?.label || panelLink.source} →{" "}
                        {nodeById.get(panelLink.target)?.label || panelLink.target}
                      </p>
                      <p>Type: Link ({panelLink.type})</p>
                      <p className="font-semibold text-ink">
                        {primaryMetricLabel(metric)}:{" "}
                        {(() => {
                          const selectedValue =
                            metric === "purchase"
                              ? panelLink.w_purchase_raw
                              : metric === "consideration"
                                ? panelLink.w_consideration_raw
                                : panelLink.w_recall_raw;
                          return typeof selectedValue === "number" ? `${(selectedValue * 100).toFixed(1)}%` : "--";
                        })()}
                      </p>
                      <p>Recall: {typeof panelLink.w_recall_raw === "number" ? `${(panelLink.w_recall_raw * 100).toFixed(1)}%` : "--"}</p>
                      <p>
                        Consideration:{" "}
                        {typeof panelLink.w_consideration_raw === "number"
                          ? `${(panelLink.w_consideration_raw * 100).toFixed(1)}%`
                          : "--"}
                      </p>
                      <p>Purchase: {typeof panelLink.w_purchase_raw === "number" ? `${(panelLink.w_purchase_raw * 100).toFixed(1)}%` : "--"}</p>
                      <p>Base N: {panelLink.n_base ?? "--"} · Studies: {panelLink.countStudies ?? "--"}</p>
                    </div>
                  ) : (
                    <div className="space-y-2 text-xs text-slate">
                      <p className="text-sm text-ink">Hover a node or link to inspect details.</p>
                      <p>Click a node to keep focus. Press Esc to clear.</p>
                      {interaction.lockedBrandIds.length > 0 && (
                        <button
                          type="button"
                          className="rounded-full border border-ink/10 px-2 py-1 text-[11px] text-slate hover:border-ink/20"
                          onClick={clearBrandLocks}
                        >
                          Clear locks ({interaction.lockedBrandIds.length})
                        </button>
                      )}
                    </div>
                  )}
                </div>
              </aside>
              {isPresentation && legendOpen && (
                <div className="absolute bottom-5 left-5 z-20 rounded-xl border border-white/60 bg-white/80 px-3 py-2 text-[11px] text-slate shadow-sm backdrop-blur">
                  <p className="font-semibold text-ink">Legend</p>
                  <p>Node: Touchpoint / Brand</p>
                  <p>Solid links: Recall / Consideration / Purchase</p>
                </div>
              )}
              {isPresentation && (
                <div className="absolute bottom-5 right-5 z-20 rounded-xl border border-white/60 bg-white/80 px-3 py-2 text-[11px] text-slate shadow-sm backdrop-blur">
                  <p>
                    Nodes: {data.meta.node_counts?.brand || 0}/{data.meta.node_counts?.touchpoint || 0}
                  </p>
                  <p>Links: {linkCounts.total}</p>
                </div>
              )}
              {process.env.NODE_ENV !== "production" && (
                <p className="mt-3 text-[11px] text-slate">
                  Nodes: {data.meta.node_counts?.brand || 0} brands / {data.meta.node_counts?.touchpoint || 0} tps ·
                  Links: primary {linkCounts.primary} / secondary {linkCounts.secondary}
                </p>
              )}
            </div>
          )}

          {exportMessage && (
            <div className="mt-3 rounded-xl border border-emerald-500/20 bg-emerald-500/10 px-3 py-2 text-xs text-emerald-700">
              {exportMessage}
            </div>
          )}
        </div>

      </section>

      {exportOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-ink/30 px-4">
          <div className="w-full max-w-md rounded-2xl border border-ink/10 bg-white p-5 shadow-xl">
            <h3 className="text-lg font-semibold">Export snapshot</h3>
            <p className="mt-1 text-sm text-slate">Save the current network canvas for demos.</p>
            <div className="mt-4 space-y-3 text-sm">
              <label className="flex items-center justify-between rounded-xl border border-ink/10 px-3 py-2">
                <span>PNG</span>
                <span className="text-emerald-700">Enabled</span>
              </label>
              <label className="flex items-center justify-between rounded-xl border border-ink/10 px-3 py-2">
                <span>SVG</span>
                <span className="text-slate">Coming soon</span>
              </label>
              <label className="flex items-center gap-2 rounded-xl border border-ink/10 px-3 py-2">
                <input
                  type="checkbox"
                  checked={includeLegendInExport}
                  onChange={(event) => setIncludeLegendInExport(event.target.checked)}
                />
                <span>Include legend</span>
              </label>
            </div>
            <div className="mt-5 flex items-center justify-end gap-2">
              <button
                type="button"
                className="rounded-full border border-ink/10 px-4 py-2 text-xs text-slate"
                onClick={() => setExportOpen(false)}
              >
                Cancel
              </button>
              <button
                type="button"
                className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-4 py-2 text-xs text-emerald-700"
                onClick={handleExportSnapshot}
              >
                Export
              </button>
            </div>
          </div>
        </div>
      )}
    </main>
  );
}
