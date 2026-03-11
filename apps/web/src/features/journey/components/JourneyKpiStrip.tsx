"use client";

import type {
  JourneyBenchmarkAggregate,
  JourneyIndexEntry,
  JourneyMetricValue,
  JourneyStage,
  JourneyStageAggregate,
} from "../data/journeySchema";

const KPI_STAGE_MAP: Array<{ key: "awareness" | "consideration" | "purchase"; label: string; stage: JourneyStage }> = [
  { key: "awareness", label: "Awareness", stage: "Brand Awareness" },
  { key: "consideration", label: "Consideration", stage: "Brand Consideration" },
  { key: "purchase", label: "Purchase", stage: "Brand Purchase" },
];

const formatPct = (value: number | null) => (typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "--");

const formatDelta = (value: number | null, benchmark: number | null, isPercent: boolean, benchmarkLabel: string) => {
  if (typeof value !== "number" || typeof benchmark !== "number") {
    return { text: `n/a vs ${benchmarkLabel.toLowerCase()}`, tone: "text-slate-500" };
  }
  const delta = value - benchmark;
  const signed = `${delta >= 0 ? "+" : ""}${(isPercent ? delta * 100 : delta).toFixed(1)}`;
  const unit = " pts";
  return {
    text: `${signed}${unit} vs ${benchmarkLabel.toLowerCase()}`,
    tone: delta > 0 ? "text-emerald-700" : delta < 0 ? "text-rose-700" : "text-slate-500",
  };
};

type JourneyKpiStripProps = {
  brand: {
    stageAggregates: JourneyStageAggregate[];
    csat: JourneyMetricValue;
    nps: JourneyMetricValue;
  };
  benchmark: JourneyBenchmarkAggregate;
  journeyIndex?: JourneyIndexEntry | null;
  benchmarkLabel?: string;
};

export default function JourneyKpiStrip({
  brand,
  benchmark,
  journeyIndex = null,
  benchmarkLabel = "Benchmark",
}: JourneyKpiStripProps) {
  const stageByName = new Map(brand.stageAggregates.map((item) => [item.stage, item.value]));
  const benchmarkByName = new Map(benchmark.stageAggregates.map((item) => [item.stage, item.value]));

  const stageCards = KPI_STAGE_MAP.map((entry) => {
    const value = stageByName.get(entry.stage) ?? null;
    const benchmarkValue = benchmarkByName.get(entry.stage) ?? null;
    const delta = formatDelta(value, benchmarkValue, true, benchmarkLabel);
    return {
      key: entry.key,
      label: entry.label,
      valueText: formatPct(value),
      deltaText: delta.text,
      deltaTone: delta.tone,
      unavailable: value == null,
    };
  });

  const csatDelta = formatDelta(brand.csat.value, benchmark.csat.value, true, benchmarkLabel);
  const npsDelta = formatDelta(brand.nps.value, benchmark.nps.value, true, benchmarkLabel);

  const cards = [
    ...stageCards,
    {
      key: "csat",
      label: "CSAT",
      valueText: formatPct(brand.csat.value),
      deltaText: csatDelta.text,
      deltaTone: csatDelta.tone,
      unavailable: brand.csat.value == null,
    },
    {
      key: "nps",
      label: "NPS",
      valueText: formatPct(brand.nps.value),
      deltaText: npsDelta.text,
      deltaTone: npsDelta.tone,
      unavailable: brand.nps.value == null,
    },
    {
      key: "journey-index",
      label: "Journey Index",
      valueText: typeof journeyIndex?.value === "number" ? `${Math.round(journeyIndex.value)}` : "--",
      deltaText:
        typeof journeyIndex?.deltaVsBenchmark === "number"
          ? `${journeyIndex.deltaVsBenchmark >= 0 ? "+" : ""}${journeyIndex.deltaVsBenchmark.toFixed(1)} pts vs ${benchmarkLabel.toLowerCase()}`
          : `n/a vs ${benchmarkLabel.toLowerCase()}`,
      deltaTone:
        typeof journeyIndex?.deltaVsBenchmark === "number"
          ? journeyIndex.deltaVsBenchmark > 0
            ? "text-emerald-700"
            : journeyIndex.deltaVsBenchmark < 0
              ? "text-rose-700"
              : "text-slate-500"
          : "text-slate-500",
      unavailable: journeyIndex?.value == null,
      meta: typeof journeyIndex?.rank === "number" ? `#${journeyIndex.rank} in category` : undefined,
    },
  ];

  return (
    <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-6">
      {cards.map((card) => (
        <div key={card.key} className="rounded-2xl border border-ink/10 bg-white/80 px-4 py-3">
          <p className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate">{card.label}</p>
          <p className="mt-1 text-2xl font-semibold text-ink">{card.valueText}</p>
          <p className={`mt-1 text-xs ${card.deltaTone}`} title={card.unavailable ? "Not available in selected range" : undefined}>
            {card.deltaText}
          </p>
          {typeof (card as { meta?: string }).meta === "string" && (
            <p className="mt-1 text-[11px] text-slate">{(card as { meta?: string }).meta}</p>
          )}
        </div>
      ))}
    </div>
  );
}

