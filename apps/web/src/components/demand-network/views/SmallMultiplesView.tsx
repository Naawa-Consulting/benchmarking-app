"use client";

import { useMemo, useState } from "react";

import { buildAggregatedLinks, getMetricValue } from "./helpers";
import type { DNViewCommonProps } from "./types";

type SmallMultiplesViewProps = DNViewCommonProps;

export default function SmallMultiplesView({
  nodes,
  links,
  metricMode,
  selectedNodeId,
  height,
  onHoverNode,
  onSelectNode,
}: SmallMultiplesViewProps) {
  const [topLinksPerCard, setTopLinksPerCard] = useState(10);
  const aggregated = useMemo(() => buildAggregatedLinks(links), [links]);
  const nodeById = useMemo(() => new Map(nodes.map((node) => [node.id, node])), [nodes]);

  const brands = useMemo(
    () => nodes.filter((node) => node.type === "brand").slice().sort((a, b) => b.size - a.size),
    [nodes]
  );

  const cards = useMemo(() => {
    return brands.map((brand) => {
      const related = aggregated
        .filter((link) => link.type === "primary_tp_brand" && link.target === brand.id)
        .slice()
        .sort((a, b) => getMetricValue(b, metricMode) - getMetricValue(a, metricMode))
        .slice(0, topLinksPerCard)
        .map((link) => nodeById.get(link.source))
        .filter((node): node is NonNullable<typeof node> => Boolean(node));
      return { brand, related };
    });
  }, [aggregated, brands, metricMode, nodeById, topLinksPerCard]);

  return (
    <div className="space-y-3 rounded-[2rem] border border-ink/10 bg-slate-50/60 p-3 sm:p-4" style={{ height }}>
      <div className="flex items-center justify-between gap-3 text-[11px] text-slate">
        <span>Small multiples (one card per brand)</span>
        <label className="flex items-center gap-2">
          Top links/card
          <input
            type="range"
            min={6}
            max={14}
            value={topLinksPerCard}
            onChange={(event) => setTopLinksPerCard(Number(event.target.value))}
          />
          <span>{topLinksPerCard}</span>
        </label>
      </div>
      <div className="grid h-[calc(100%-2.2rem)] grid-cols-1 gap-3 overflow-auto pr-1 md:grid-cols-2 xl:grid-cols-3">
        {cards.map(({ brand, related }) => {
          const isActive = selectedNodeId === brand.id;
          return (
            <button
              key={brand.id}
              type="button"
              className={`rounded-2xl border bg-white/90 p-3 text-left transition-colors ${
                isActive ? "border-emerald-500/40" : "border-ink/10 hover:border-ink/20"
              }`}
              onMouseEnter={() => onHoverNode(brand)}
              onMouseLeave={() => onHoverNode(null)}
              onClick={() => onSelectNode(brand)}
            >
              <p className="truncate text-sm font-semibold text-ink">{brand.label}</p>
              <p className="mt-1 text-[11px] text-slate">Top touchpoints ({related.length})</p>
              <div className="mt-2 space-y-1">
                {related.map((tp) => (
                  <div key={tp.id} className="truncate rounded-lg border border-ink/10 px-2 py-1 text-[11px] text-slate">
                    {tp.label}
                  </div>
                ))}
                {!related.length && <p className="text-[11px] text-slate">No links in current scope.</p>}
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
