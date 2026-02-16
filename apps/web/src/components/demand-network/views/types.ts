import type { HoveredLink } from "../../NetworkCanvas";

export type DNNode = {
  id: string;
  type: string;
  label: string;
  size: number;
  group: string;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  colorMeta?: Record<string, unknown> | null;
};

export type DNLink = {
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

export type DNMetric = "recall" | "consideration" | "purchase" | "both";
export type DNLayout = "auto" | "spacious";
export type DNSecondaryCluster = "off" | "category";
export type DNViewMode = "network" | "matrix" | "sankey" | "multiples";

export type DNViewCommonProps = {
  nodes: DNNode[];
  links: DNLink[];
  metricMode: DNMetric;
  selectedNodeId: string | null;
  activeNodeId: string | null;
  activeLinkId: string | null;
  lockedBrandIds: string[];
  height: string;
  onHoverNode: (node: DNNode | null) => void;
  onHoverLink: (link: HoveredLink | null) => void;
  onSelectNode: (node: DNNode | null) => void;
};
