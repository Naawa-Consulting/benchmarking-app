"use client";

import { useMemo, useState } from "react";
import type { JourneyInsight } from "../insights/generateJourneyInsights";

type JourneyInsightsProps = {
  insights: JourneyInsight[];
};

type InsightKind = "risk" | "strength" | "opportunity" | "experience";

type ExperienceMetric = {
  value: number | null;
  benchmark: number | null;
  coverage: JourneyInsight["coverage"] | null;
};

const levelWeight = (level: JourneyInsight["coverage"]["level"]) => {
  if (level === "high") return 1;
  if (level === "med") return 0.7;
  return 0.45;
};

const getNumericStat = (insight: JourneyInsight | null | undefined, key: string) => {
  if (!insight) return null;
  const value = insight.stats?.[key];
  return typeof value === "number" ? value : null;
};

const getStringStat = (insight: JourneyInsight | null | undefined, key: string) => {
  if (!insight) return null;
  const value = insight.stats?.[key];
  return typeof value === "string" && value.trim() ? value : null;
};

const getInsightBrandLabel = (insight: JourneyInsight | null) => {
  if (!insight) return null;
  const brand = getStringStat(insight, "brand");
  const compareBrand = getStringStat(insight, "compareBrand");
  const brandA = getStringStat(insight, "brandA");
  const brandB = getStringStat(insight, "brandB");
  if (brand && compareBrand) return `${brand} vs ${compareBrand}`;
  if (brandA && brandB) return `${brandA} vs ${brandB}`;
  if (brand) return brand;
  return null;
};

const resolveDelta = (insight: JourneyInsight) => {
  const delta = getNumericStat(insight, "delta");
  if (delta != null) return delta;
  const stageValue = getNumericStat(insight, "stageValue");
  const benchmarkValue = getNumericStat(insight, "benchmarkValue");
  if (stageValue != null && benchmarkValue != null) return stageValue - benchmarkValue;
  const conversion = getNumericStat(insight, "conversion");
  const benchmarkConversion = getNumericStat(insight, "benchmarkConversion");
  if (conversion != null && benchmarkConversion != null) return conversion - benchmarkConversion;
  return null;
};

const resolveDropMagnitude = (insight: JourneyInsight) => {
  const drop = getNumericStat(insight, "drop");
  if (drop != null) return Math.abs(drop);
  const benchmarkDrop = getNumericStat(insight, "benchmarkDrop");
  if (benchmarkDrop != null) return Math.abs(benchmarkDrop);
  return 0;
};

const classify = (insight: JourneyInsight): InsightKind => {
  const id = insight.id.toLowerCase();
  const title = insight.title.toLowerCase();
  if (id.startsWith("csat-") || id.startsWith("nps-") || title.includes("csat") || title.includes("nps")) {
    return "experience";
  }
  if (title.includes("fortaleza") || title.includes("destacada") || insight.severity === "positive") {
    return "strength";
  }
  if (title.includes("oportunidad") || title.includes("conversion por debajo")) {
    return "opportunity";
  }
  if (title.includes("caida") || title.includes("warning") || insight.severity === "warning") {
    return "risk";
  }
  return "opportunity";
};

const scoreRisk = (insight: JourneyInsight) => {
  const delta = resolveDelta(insight);
  const weight = levelWeight(insight.coverage.level);
  if (delta != null && delta < 0) return Math.abs(delta) * 2.4 * weight;
  return resolveDropMagnitude(insight) * 2.1 * weight + insight.score * 0.2;
};

const scoreStrength = (insight: JourneyInsight) => {
  const delta = resolveDelta(insight);
  const weight = levelWeight(insight.coverage.level);
  if (delta != null && delta > 0) return delta * 2.4 * weight;
  return insight.score * 0.6 * weight;
};

const scoreOpportunity = (insight: JourneyInsight) => {
  const delta = resolveDelta(insight);
  const weight = levelWeight(insight.coverage.level);
  return Math.abs(delta ?? 0) * 2 * weight + insight.score * 0.4;
};

const formatPts = (value: number | null) => {
  if (typeof value !== "number") return "n/a";
  const points = value * 100;
  return `${points >= 0 ? "+" : ""}${points.toFixed(1)} pts`;
};

function InsightHeroCard({
  title,
  insight,
  fallback,
  onSeeDetails,
}: {
  title: string;
  insight: JourneyInsight | null;
  fallback: string;
  onSeeDetails: () => void;
}) {
  return (
    <article className="rounded-2xl border border-ink/10 bg-slate-50/60 p-4">
      <div className="flex items-center justify-between gap-2">
        <p className="text-sm font-semibold text-ink">{title}</p>
      </div>
      {getInsightBrandLabel(insight) && (
        <p className="mt-1 text-[11px] font-medium text-slate">Brand: {getInsightBrandLabel(insight)}</p>
      )}
      <p className="mt-2 text-sm text-slate">{insight?.description || fallback}</p>
      <div className="mt-3 flex items-center justify-between gap-3">
        <span />
        <button
          type="button"
          className="rounded-full border border-ink/10 bg-white px-2.5 py-1 text-[11px] text-slate hover:bg-slate-50"
          onClick={onSeeDetails}
        >
          See details
        </button>
      </div>
    </article>
  );
}

function ExperienceSignalCard({
  brandLabel,
  csat,
  nps,
  onSeeDetails,
}: {
  brandLabel?: string | null;
  csat: ExperienceMetric;
  nps: ExperienceMetric;
  onSeeDetails: () => void;
}) {
  const deltaTone = (value: number | null, bench: number | null) => {
    if (typeof value !== "number" || typeof bench !== "number") return "text-slate-500";
    const d = value - bench;
    if (d > 0) return "text-emerald-700";
    if (d < 0) return "text-rose-700";
    return "text-slate-500";
  };

  const deltaText = (value: number | null, bench: number | null) => {
    if (typeof value !== "number" || typeof bench !== "number") return "n/a vs bench";
    return `${formatPts(value - bench)} vs bench`;
  };

  return (
    <article className="rounded-2xl border border-ink/10 bg-slate-50/60 p-4">
      <div className="flex items-center justify-between gap-2">
        <p className="text-sm font-semibold text-ink">Experience Signal</p>
        <span className="rounded-full border border-ink/10 bg-white px-2 py-0.5 text-[11px] text-slate">CSAT · NPS</span>
      </div>
      {brandLabel ? <p className="mt-1 text-[11px] font-medium text-slate">Brand: {brandLabel}</p> : null}

      <div className="mt-3 grid gap-3 sm:grid-cols-2">
        <div className="rounded-xl border border-ink/10 bg-white/60 p-3">
          <div className="flex items-center justify-between gap-2">
            <p className="text-slate">CSAT</p>
            <p className="font-semibold text-ink">{typeof csat.value === "number" ? `${csat.value.toFixed(1)} pts` : "--"}</p>
          </div>
          <p className={`mt-1 text-[11px] ${deltaTone(csat.value, csat.benchmark)}`}>{deltaText(csat.value, csat.benchmark)}</p>
        </div>
        <div className="rounded-xl border border-ink/10 bg-white/60 p-3">
          <div className="flex items-center justify-between gap-2">
            <p className="text-slate">NPS</p>
            <p className="font-semibold text-ink">{typeof nps.value === "number" ? `${nps.value.toFixed(1)} pts` : "--"}</p>
          </div>
          <p className={`mt-1 text-[11px] ${deltaTone(nps.value, nps.benchmark)}`}>{deltaText(nps.value, nps.benchmark)}</p>
        </div>
      </div>

      <div className="mt-3 flex items-center justify-between gap-3">
        <span />
        <button
          type="button"
          className="rounded-full border border-ink/10 bg-white px-2.5 py-1 text-[11px] text-slate hover:bg-slate-50"
          onClick={onSeeDetails}
        >
          See details
        </button>
      </div>
    </article>
  );
}

export default function JourneyInsights({ insights }: JourneyInsightsProps) {
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [drawerFilter, setDrawerFilter] = useState<"all" | InsightKind>("all");

  const sortedInsights = useMemo(
    () => [...insights].sort((a, b) => {
      const impactA = Math.max(Math.abs(resolveDelta(a) ?? 0), resolveDropMagnitude(a), a.score);
      const impactB = Math.max(Math.abs(resolveDelta(b) ?? 0), resolveDropMagnitude(b), b.score);
      if (impactB !== impactA) return impactB - impactA;
      return levelWeight(b.coverage.level) - levelWeight(a.coverage.level);
    }),
    [insights]
  );

  const grouped = useMemo(() => {
    const riskCandidates = sortedInsights.filter((item) => classify(item) === "risk");
    const strengthCandidates = sortedInsights.filter((item) => classify(item) === "strength");
    const opportunityCandidates = sortedInsights.filter((item) => classify(item) === "opportunity");
    const experienceCandidates = sortedInsights.filter((item) => classify(item) === "experience");

    const biggestRisk =
      riskCandidates
        .slice()
        .sort((a, b) => scoreRisk(b) - scoreRisk(a))[0] ||
      opportunityCandidates.slice().sort((a, b) => scoreRisk(b) - scoreRisk(a))[0] ||
      null;

    const strongestStage =
      strengthCandidates.slice().sort((a, b) => scoreStrength(b) - scoreStrength(a))[0] || null;

    const strategicOpportunity =
      opportunityCandidates
        .filter((item) => item.id !== biggestRisk?.id)
        .slice()
        .sort((a, b) => scoreOpportunity(b) - scoreOpportunity(a))[0] ||
      riskCandidates
        .filter((item) => item.id !== biggestRisk?.id)
        .slice()
        .sort((a, b) => scoreOpportunity(b) - scoreOpportunity(a))[0] ||
      null;

    const csatInsight = experienceCandidates.find((item) => item.id.startsWith("csat-")) || null;
    const npsInsight = experienceCandidates.find((item) => item.id.startsWith("nps-")) || null;

    const csat: ExperienceMetric = {
      value: getNumericStat(csatInsight, "csat"),
      benchmark: getNumericStat(csatInsight, "benchmark"),
      coverage: csatInsight?.coverage || null,
    };

    const nps: ExperienceMetric = {
      value: getNumericStat(npsInsight, "nps"),
      benchmark: getNumericStat(npsInsight, "benchmark"),
      coverage: npsInsight?.coverage || null,
    };
    const experienceBrandLabel = getInsightBrandLabel(csatInsight) || getInsightBrandLabel(npsInsight);

    return {
      biggestRisk,
      strongestStage,
      strategicOpportunity,
      csat,
      nps,
      experienceBrandLabel,
    };
  }, [sortedInsights]);

  const drawerInsights = useMemo(() => {
    if (drawerFilter === "all") return sortedInsights;
    return sortedInsights.filter((item) => classify(item) === drawerFilter);
  }, [drawerFilter, sortedInsights]);

  return (
    <section className="main-surface p-6">
      <div className="flex items-center justify-between gap-3">
        <h3 className="text-xl font-semibold">Insights</h3>
        <button
          type="button"
          className="rounded-full border border-ink/10 bg-white px-3 py-1.5 text-xs text-slate hover:bg-slate-50"
          onClick={() => setDrawerOpen(true)}
        >
          View all insights ({insights.length})
        </button>
      </div>

      <div className="mt-4 grid gap-3 md:grid-cols-2">
        <InsightHeroCard
          title="Biggest Risk"
          insight={grouped.biggestRisk}
          fallback="Not enough data in selected filters."
          onSeeDetails={() => {
            setDrawerFilter("risk");
            setDrawerOpen(true);
          }}
        />
        <InsightHeroCard
          title="Strongest Stage"
          insight={grouped.strongestStage}
          fallback="No clear strength signal yet."
          onSeeDetails={() => {
            setDrawerFilter("strength");
            setDrawerOpen(true);
          }}
        />
        <InsightHeroCard
          title="Strategic Opportunity"
          insight={grouped.strategicOpportunity}
          fallback="No strategic opportunity surfaced yet."
          onSeeDetails={() => {
            setDrawerFilter("opportunity");
            setDrawerOpen(true);
          }}
        />
        <ExperienceSignalCard
          brandLabel={grouped.experienceBrandLabel}
          csat={grouped.csat}
          nps={grouped.nps}
          onSeeDetails={() => {
            setDrawerFilter("experience");
            setDrawerOpen(true);
          }}
        />
      </div>

      {drawerOpen && (
        <div className="fixed inset-0 z-[90]">
          <button
            type="button"
            className="absolute inset-0 bg-black/35"
            aria-label="Close insights panel"
            onClick={() => setDrawerOpen(false)}
          />
          <aside className="absolute right-0 top-0 h-full w-full max-w-[560px] overflow-auto border-l border-ink/10 bg-white p-5 shadow-2xl">
            <div className="flex items-center justify-between gap-3">
              <h4 className="text-lg font-semibold text-ink">All insights</h4>
              <button
                type="button"
                className="rounded-full border border-ink/10 bg-white px-3 py-1 text-xs text-slate hover:bg-slate-50"
                onClick={() => setDrawerOpen(false)}
              >
                Close
              </button>
            </div>

            <div className="mt-4 flex flex-wrap gap-2 text-xs">
              {([
                ["all", "All"],
                ["risk", "Risks"],
                ["strength", "Strengths"],
                ["opportunity", "Opportunities"],
                ["experience", "Experience"],
              ] as const).map(([value, label]) => (
                <button
                  key={value}
                  type="button"
                  className={`rounded-full border px-3 py-1 ${
                    drawerFilter === value
                      ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
                      : "border-ink/10 bg-white text-slate"
                  }`}
                  onClick={() => setDrawerFilter(value)}
                >
                  {label}
                </button>
              ))}
            </div>

            <div className="mt-4 space-y-3">
              {drawerInsights.map((insight) => (
                <article key={insight.id} className="rounded-2xl border border-ink/10 bg-slate-50/60 p-4">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <p className="text-sm font-semibold text-ink">{insight.title}</p>
                  </div>
                  {getInsightBrandLabel(insight) && (
                    <p className="mt-1 text-[11px] font-medium text-slate">Brand: {getInsightBrandLabel(insight)}</p>
                  )}
                  <p className="mt-2 text-sm text-slate">{insight.description}</p>
                </article>
              ))}
              {!drawerInsights.length && <p className="text-sm text-slate">No insights for this filter.</p>}
            </div>
          </aside>
        </div>
      )}
    </section>
  );
}

