"use client";

import type { JourneyInsight } from "../insights/generateJourneyInsights";

type JourneyInsightsProps = {
  insights: JourneyInsight[];
};

const badgeClass = (severity: JourneyInsight["severity"]) => {
  if (severity === "positive") return "border-emerald-500/30 bg-emerald-500/10 text-emerald-700";
  if (severity === "warning") return "border-amber-500/30 bg-amber-500/10 text-amber-700";
  return "border-ink/10 bg-white text-slate";
};

export default function JourneyInsights({ insights }: JourneyInsightsProps) {
  return (
    <section className="main-surface p-6">
      <h3 className="text-xl font-semibold">Insights</h3>
      <div className="mt-4 grid gap-3 md:grid-cols-2">
        {insights.map((insight) => (
          <article key={insight.id} className="rounded-2xl border border-ink/10 bg-slate-50/60 p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <p className="text-sm font-semibold text-ink">{insight.title}</p>
              <span className={`rounded-full border px-2 py-0.5 text-[11px] ${badgeClass(insight.severity)}`}>
                {insight.coverage.level} coverage
              </span>
            </div>
            <p className="mt-2 text-sm text-slate">{insight.description}</p>
            <p className="mt-2 text-[11px] text-slate">
              n studies: {insight.coverage.studies}/{insight.coverage.totalStudies}
            </p>
          </article>
        ))}
      </div>
    </section>
  );
}

