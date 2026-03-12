"use client";

import dynamic from "next/dynamic";
import { Suspense } from "react";
import type { MutableRefObject } from "react";

import { NetworkCanvas, type HoveredLink, type NetworkCanvasHandle } from "../../NetworkCanvas";
import type {
  DNLink,
  DNNode,
  DNMetric,
  DNSecondaryCluster,
  DNLayout,
  DNViewMode,
  DNDistanceMode,
} from "./types";

const MatrixView = dynamic(() => import("./MatrixView"), {
  ssr: false,
  loading: () => <div className="rounded-[2rem] border border-ink/10 bg-slate-100" style={{ height: "100%" }} />,
});
const SankeyView = dynamic(() => import("./SankeyView"), {
  ssr: false,
  loading: () => <div className="rounded-[2rem] border border-ink/10 bg-slate-100" style={{ height: "100%" }} />,
});
const SmallMultiplesView = dynamic(() => import("./SmallMultiplesView"), {
  ssr: false,
  loading: () => <div className="rounded-[2rem] border border-ink/10 bg-slate-100" style={{ height: "100%" }} />,
});

type DemandNetworkViewProps = {
  viewMode: DNViewMode;
  canvasRef: MutableRefObject<NetworkCanvasHandle | null>;
  nodes: DNNode[];
  links: DNLink[];
  metricMode: DNMetric;
  clusterMode: DNSecondaryCluster;
  selectedBrandsCount: number;
  showSecondaryAlways: boolean;
  labelMode: "auto" | "off";
  spotlight: boolean;
  layoutMode: DNLayout;
  distanceMode: DNDistanceMode;
  pulseNodeId: string | null;
  height: string;
  activeNodeId: string | null;
  activeLinkId: string | null;
  selectedNodeId: string | null;
  lockedBrandIds: string[];
  onHoverNode: (node: DNNode | null) => void;
  onHoverLink: (link: HoveredLink | null) => void;
  onSelectNode: (node: DNNode | null) => void;
};

export default function DemandNetworkView({
  viewMode,
  canvasRef,
  nodes,
  links,
  metricMode,
  clusterMode,
  selectedBrandsCount,
  showSecondaryAlways,
  labelMode,
  spotlight,
  layoutMode,
  distanceMode,
  pulseNodeId,
  height,
  activeNodeId,
  activeLinkId,
  selectedNodeId,
  lockedBrandIds,
  onHoverNode,
  onHoverLink,
  onSelectNode,
}: DemandNetworkViewProps) {
  if (viewMode === "network") {
    return (
      <NetworkCanvas
        ref={canvasRef as any}
        nodes={nodes}
        links={links}
        metricMode={metricMode}
        clusterMode={clusterMode}
        onHoverNode={onHoverNode}
        onHoverLink={onHoverLink}
        onSelectNode={onSelectNode}
        activeNodeId={activeNodeId}
        activeLinkId={activeLinkId}
        selectedNodeId={selectedNodeId}
        selectedBrandsCount={selectedBrandsCount}
        lockedBrandIds={lockedBrandIds}
        showSecondaryAlways={showSecondaryAlways}
        labelMode={labelMode}
        spotlight={spotlight}
        layoutMode={layoutMode}
        distanceMode={distanceMode}
        pulseNodeId={pulseNodeId}
        height={height}
      />
    );
  }

  return (
    <Suspense fallback={<div className="rounded-[2rem] border border-ink/10 bg-slate-100" style={{ height }} />}>
      {viewMode === "matrix" && (
        <MatrixView
          nodes={nodes}
          links={links}
          metricMode={metricMode}
          selectedNodeId={selectedNodeId}
          activeNodeId={activeNodeId}
          activeLinkId={activeLinkId}
          lockedBrandIds={lockedBrandIds}
          height={height}
          onHoverNode={onHoverNode}
          onHoverLink={onHoverLink}
          onSelectNode={onSelectNode}
        />
      )}
      {viewMode === "sankey" && (
        <SankeyView
          nodes={nodes}
          links={links}
          metricMode={metricMode}
          selectedNodeId={selectedNodeId}
          activeNodeId={activeNodeId}
          activeLinkId={activeLinkId}
          lockedBrandIds={lockedBrandIds}
          height={height}
          onHoverNode={onHoverNode}
          onHoverLink={onHoverLink}
          onSelectNode={onSelectNode}
        />
      )}
      {viewMode === "multiples" && (
        <SmallMultiplesView
          nodes={nodes}
          links={links}
          metricMode={metricMode}
          selectedNodeId={selectedNodeId}
          activeNodeId={activeNodeId}
          activeLinkId={activeLinkId}
          lockedBrandIds={lockedBrandIds}
          height={height}
          onHoverNode={onHoverNode}
          onHoverLink={onHoverLink}
          onSelectNode={onSelectNode}
        />
      )}
    </Suspense>
  );
}
