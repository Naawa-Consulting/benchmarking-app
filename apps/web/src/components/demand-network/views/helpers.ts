import { aggregateLinks, type AggregatedLink } from "../graphUtils";
import type { DNLink, DNMetric } from "./types";

export const getMetricValue = (
  link: Pick<AggregatedLink, "w_recall_raw" | "w_consideration_raw" | "w_purchase_raw">,
  metric: DNMetric
) => {
  if (metric === "purchase") return link.w_purchase_raw ?? 0;
  if (metric === "consideration") return link.w_consideration_raw ?? 0;
  return link.w_recall_raw ?? 0;
};

export const getLinkId = (link: { source: string; target: string; type: string }) =>
  `${link.source}::${link.target}::${link.type}`;

export const buildAggregatedLinks = (links: DNLink[]) => aggregateLinks(links);

const TIME_KEYS = ["time_bucket", "quarter", "period", "wave", "time"] as const;

export const extractTimeBuckets = (links: DNLink[]): string[] => {
  const buckets = new Set<string>();
  for (const link of links) {
    const meta = link.colorMeta || {};
    for (const key of TIME_KEYS) {
      const value = meta[key];
      if (typeof value === "string" && value.trim()) {
        buckets.add(value);
      }
    }
  }
  return Array.from(buckets).sort();
};

export const linkInBucket = (link: DNLink, bucket: string) => {
  const meta = link.colorMeta || {};
  for (const key of TIME_KEYS) {
    const value = meta[key];
    if (typeof value === "string" && value === bucket) {
      return true;
    }
  }
  return false;
};
