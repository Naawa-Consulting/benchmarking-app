import type { TrackingComparisonModel, TrackingMetricKey } from "../types";

type TrackingKpiStripProps = {
  model: TrackingComparisonModel;
};

const KPI_KEYS: TrackingMetricKey[] = [
  "brand_awareness",
  "brand_consideration",
  "brand_purchase",
  "brand_satisfaction",
  "brand_recommendation",
];

function average(values: Array<number | null>) {
  const numeric = values.filter((value): value is number => typeof value === "number");
  if (!numeric.length) return null;
  return numeric.reduce((acc, value) => acc + value, 0) / numeric.length;
}

function formatValue(value: number | null, unit: "%" | "pts") {
  if (value == null) return "-";
  return `${value.toFixed(1)}${unit}`;
}

function formatDelta(value: number | null) {
  if (value == null) return "n/a";
  const sign = value > 0 ? "+" : "";
  return `${sign}${value.toFixed(1)} pts`;
}

export default function TrackingKpiStrip({ model }: TrackingKpiStripProps) {
  const cards = KPI_KEYS.filter((key) => model.metricMeta[key].available).map((key) => {
    const avgEarlier = average(model.brands.map((brand) => brand.metrics[key].valueEarlier));
    const avgLater = average(model.brands.map((brand) => brand.metrics[key].valueLater));
    const avgDelta = average(model.brands.map((brand) => brand.metrics[key].deltaAbs));
    return {
      key,
      label: model.metricMeta[key].label,
      unit: model.metricMeta[key].unit,
      earlier: avgEarlier,
      later: avgLater,
      delta: avgDelta,
    };
  });

  return (
    <section className="space-y-2">
      <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-slate">
        <p>Promedio de marcas en seleccion actual.</p>
        <p
          className="rounded-full border border-ink/10 bg-white px-2 py-1"
          title="KPI = mean(valor_marca_post) y Delta = mean(post - pre) sobre marcas con dato valido."
        >
          Formula KPI
        </p>
      </div>
      {model.brands.length < 2 && (
        <p className="text-xs text-amber-700">Base de comparacion limitada: menos de 2 marcas validas.</p>
      )}
      <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
      {cards.map((card) => (
        <article key={card.key} className="rounded-2xl border border-ink/10 bg-white p-4 shadow-sm">
          <p className="text-[11px] font-semibold uppercase tracking-[0.08em] text-slate">{card.label}</p>
          <p className="mt-2 text-2xl font-semibold text-ink">{formatValue(card.later, card.unit)}</p>
          <p className="mt-1 text-xs text-slate">
            Pre: {formatValue(card.earlier, card.unit)} | Delta:{" "}
            <span className={card.delta == null ? "text-slate" : card.delta >= 0 ? "text-emerald-700" : "text-rose-700"}>
              {formatDelta(card.delta)}
            </span>
          </p>
        </article>
      ))}
      </div>
    </section>
  );
}
