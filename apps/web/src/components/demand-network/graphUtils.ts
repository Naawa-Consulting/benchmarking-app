type MetricValue = number | null | undefined;

export type AggregatableLink = {
  source: string;
  target: string;
  type: string;
  weight?: number | null;
  n_base?: number | null;
  w_recall_raw?: number | null;
  w_consideration_raw?: number | null;
  w_purchase_raw?: number | null;
  w_recall_norm?: number | null;
  w_consideration_norm?: number | null;
  w_purchase_norm?: number | null;
  colorMeta?: Record<string, unknown> | null;
};

export type AggregatedLink = AggregatableLink & {
  weight: number;
  n_base: number;
  countStudies: number;
  metrics: {
    recall: number | null;
    consideration: number | null;
    purchase: number | null;
  };
};

type MetricKey = "w_recall_raw" | "w_consideration_raw" | "w_purchase_raw";

const METRIC_KEYS: MetricKey[] = ["w_recall_raw", "w_consideration_raw", "w_purchase_raw"];

const asFinite = (value: MetricValue): number | null => {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return value;
};

const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

const normalizePercentiles = (values: number[]) => {
  if (!values.length) return [];
  const sorted = values.slice().sort((a, b) => a - b);
  if (sorted.length === 1) return [0.5];
  const p05Idx = Math.max(0, Math.round((sorted.length - 1) * 0.05));
  const p95Idx = Math.max(0, Math.round((sorted.length - 1) * 0.95));
  const p05 = sorted[p05Idx];
  const p95 = sorted[p95Idx];
  if (p95 <= p05) return values.map(() => 0.5);
  return values.map((value) => clamp((value - p05) / (p95 - p05), 0, 1));
};

const deriveConsiderationGivenRecall = (link: AggregatableLink) => {
  const fromMeta = asFinite(link.colorMeta?.consideration_given_recall as number | null | undefined);
  if (fromMeta !== null) return clamp(fromMeta, 0, 1);
  if (link.type !== "primary_tp_brand") return asFinite(link.w_consideration_raw);
  const recall = asFinite(link.w_recall_raw);
  const consideration = asFinite(link.w_consideration_raw);
  if (recall === null || consideration === null || recall <= 0) return null;
  return clamp(consideration / recall, 0, 1);
};

type Accumulator = {
  link: AggregatableLink;
  baseNSum: number;
  studies: Set<string>;
  weighted: Record<MetricKey, { sum: number; weight: number }>;
};

export const aggregateLinks = (rawLinks: AggregatableLink[]): AggregatedLink[] => {
  const buckets = new Map<string, Accumulator>();

  for (const link of rawLinks) {
    const key = `${link.source}||${link.target}||${link.type}`;
    if (!buckets.has(key)) {
      buckets.set(key, {
        link: { ...link, colorMeta: link.colorMeta ? { ...link.colorMeta } : null },
        baseNSum: 0,
        studies: new Set<string>(),
        weighted: {
          w_recall_raw: { sum: 0, weight: 0 },
          w_consideration_raw: { sum: 0, weight: 0 },
          w_purchase_raw: { sum: 0, weight: 0 },
        },
      });
    }

    const bucket = buckets.get(key)!;
    const baseN = asFinite(link.n_base);
    const weight = baseN && baseN > 0 ? baseN : 1;
    bucket.baseNSum += weight;

    const studyId = link.colorMeta?.study_id;
    if (typeof studyId === "string" && studyId.length > 0) {
      bucket.studies.add(studyId);
    }

    const recall = asFinite(link.w_recall_raw);
    const considerationGivenRecall = deriveConsiderationGivenRecall(link);
    const purchase = asFinite(link.w_purchase_raw);

    if (recall !== null) {
      bucket.weighted.w_recall_raw.sum += recall * weight;
      bucket.weighted.w_recall_raw.weight += weight;
    }
    if (considerationGivenRecall !== null) {
      bucket.weighted.w_consideration_raw.sum += considerationGivenRecall * weight;
      bucket.weighted.w_consideration_raw.weight += weight;
    }
    if (purchase !== null) {
      bucket.weighted.w_purchase_raw.sum += purchase * weight;
      bucket.weighted.w_purchase_raw.weight += weight;
    }
  }

  const output: AggregatedLink[] = [];
  for (const bucket of buckets.values()) {
    const metricValue = (metric: MetricKey) => {
      const { sum, weight } = bucket.weighted[metric];
      return weight > 0 ? sum / weight : null;
    };

    const recall = metricValue("w_recall_raw");
    const consideration = metricValue("w_consideration_raw");
    const purchase = metricValue("w_purchase_raw");

    output.push({
      ...bucket.link,
      weight: asFinite(bucket.link.weight) ?? metricValue("w_recall_raw") ?? 0,
      n_base: bucket.baseNSum,
      countStudies: Math.max(1, bucket.studies.size),
      metrics: {
        recall,
        consideration,
        purchase,
      },
      w_recall_raw: recall,
      w_consideration_raw: consideration,
      w_purchase_raw: purchase,
    });
  }

  // Keep norm fields in sync with aggregated raw metrics so selected metric really changes thickness/rendering.
  const metricToNorm: Array<[MetricKey, "w_recall_norm" | "w_consideration_norm" | "w_purchase_norm"]> = [
    ["w_recall_raw", "w_recall_norm"],
    ["w_consideration_raw", "w_consideration_norm"],
    ["w_purchase_raw", "w_purchase_norm"],
  ];

  for (const [rawKey, normKey] of metricToNorm) {
    const indexes: number[] = [];
    const values: number[] = [];
    output.forEach((link, index) => {
      const value = asFinite(link[rawKey]);
      if (value === null) return;
      indexes.push(index);
      values.push(value);
    });
    const normalized = normalizePercentiles(values);
    indexes.forEach((linkIndex, idx) => {
      output[linkIndex][normKey] = normalized[idx];
    });
  }

  return output;
};

export const buildThicknessScale = (values: Array<number | null | undefined>) => {
  const finite = values
    .map((value) => asFinite(value))
    .filter((value): value is number => value !== null && value > 0)
    .sort((a, b) => a - b);

  if (!finite.length) {
    return () => 0.6;
  }

  const min = finite[0];
  const max = finite[finite.length - 1];
  const span = Math.max(max - min, 1e-9);

  return (value: number | null | undefined) => {
    const v = asFinite(value);
    if (v === null || v <= 0) return 0.6;
    const normalized = clamp((v - min) / span, 0, 1);
    const shaped = Math.sqrt(normalized);
    return 0.6 + shaped * (6 - 0.6);
  };
};
