import type { TrackingBrandMetricKey, TrackingSeriesModel } from "../types";

type TrackingKpiStripProps = {
  model: TrackingSeriesModel;
};

const KPI_KEYS: TrackingBrandMetricKey[] = [
  "brand_awareness",
  "brand_consideration",
  "brand_purchase",
  "csat",
  "nps",
];

function average(values: Array<number | null>) {
  const numeric = values.filter((value): value is number => typeof value === "number");
  if (!numeric.length) return null;
  return numeric.reduce((acc, value) => acc + value, 0) / numeric.length;
}

function fmt(value: number | null, unit: string) {
  if (value == null) return "-";
  return `${value.toFixed(1)}${unit}`;
}

export default function TrackingKpiStrip({ model }: TrackingKpiStripProps) {
  const periods = model.periods;
  const latest = periods[periods.length - 1]?.key;
  const previous = periods.length > 1 ? periods[periods.length - 2]?.key : null;

  const cards = KPI_KEYS.filter((key) => model.metric_meta_brand[key]).map((key) => {
    const meta = model.metric_meta_brand[key];
    const latestAvg = average(
      model.entity_rows.map((row) => {
        const value = row.metrics[key]?.values?.[latest || ""];
        return typeof value === "number" ? value : null;
      })
    );
    const prevAvg = previous
      ? average(
          model.entity_rows.map((row) => {
            const value = row.metrics[key]?.values?.[previous];
            return typeof value === "number" ? value : null;
          })
        )
      : null;
    const delta = latestAvg != null && prevAvg != null ? latestAvg - prevAvg : null;
    return { key, label: meta.label, unit: meta.unit, latestAvg, delta };
  });

  return (
    <section className="space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-slate">
        <p>
          Modo: {model.resolved_granularity === "year" ? "Comparando por Ano" : "Comparando por Trimestre"}
        </p>
        <p className="rounded-full border border-ink/10 bg-white px-2 py-1">KPI = promedio de entidades visibles</p>
      </div>
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
        {cards.map((card) => (
          <article key={card.key} className="rounded-2xl border border-ink/10 bg-white p-4 shadow-sm">
            <p className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate">{card.label}</p>
            <p className="mt-2 text-2xl font-semibold text-ink">{fmt(card.latestAvg, card.unit)}</p>
            <p className="mt-1 text-xs text-slate">
              Delta vs periodo anterior:{" "}
              <span className={card.delta == null ? "text-slate" : card.delta >= 0 ? "text-emerald-700" : "text-rose-700"}>
                {card.delta == null ? "n/a" : `${card.delta > 0 ? "+" : ""}${card.delta.toFixed(1)} pts`}
              </span>
            </p>
          </article>
        ))}
      </div>
    </section>
  );
}
