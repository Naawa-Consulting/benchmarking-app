"use client";

import { memo, useEffect, useMemo, useRef, useState } from "react";

import type {
  JourneyBenchmarkAggregate,
  JourneyBrandAggregate,
  FunnelHealthEntry,
  JourneyIndexEntry,
  JourneyModel,
  JourneyStage,
  JourneyStageAggregate,
} from "../data/journeySchema";
import JourneyKpiStrip from "./JourneyKpiStrip";

type HeroSankeyProps = {
  model: JourneyModel;
  selectedBrandNames: string[];
  focusBrandName?: string | null;
  compareBrandName?: string | null;
  timeBucketLabel?: string | null;
};

type SankeyUnit = {
  key: string;
  title: string;
  stagesOrdered: JourneyStage[];
  stageAggregates: JourneyStageAggregate[];
  links: JourneyBrandAggregate["links"];
  csat: JourneyBrandAggregate["csat"];
  nps: JourneyBrandAggregate["nps"];
  journeyIndex: JourneyIndexEntry | null;
  funnelHealth: FunnelHealthEntry | null;
  isFocused?: boolean;
};

type ConversionBadge = {
  key: string;
  left: number;
  text: string;
  toneClass: string;
  hint: string;
};

type StageGeometry = {
  stage: JourneyStage;
  value: number | null;
  x: number;
  top: number;
  bottom: number;
  height: number;
  coverageStudies: number;
  rank: number | null;
};

const stageShortLabel = (stage: JourneyStage) => {
  if (stage === "Brand Awareness") return "Awareness";
  if (stage === "Ad Awareness") return "Ad Awareness";
  if (stage === "Brand Consideration") return "Consideration";
  if (stage === "Brand Purchase") return "Purchase";
  if (stage === "Brand Satisfaction") return "Satisfaction";
  return "Recommendation";
};

const pct = (value: number | null) => (typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "--");

const healthBadgeStyle = (status: FunnelHealthEntry["status"] | "unknown") => {
  if (status === "healthy") return "border-emerald-500/30 bg-emerald-500/10 text-emerald-700";
  if (status === "moderate") return "border-amber-500/30 bg-amber-500/10 text-amber-700";
  if (status === "critical") return "border-rose-500/30 bg-rose-500/10 text-rose-700";
  return "border-ink/10 bg-white text-slate";
};

const healthBadgeLabel = (status: FunnelHealthEntry["status"] | "unknown") => {
  if (status === "healthy") return "Healthy";
  if (status === "moderate") return "Moderate";
  if (status === "critical") return "Critical Drop";
  return "Unknown";
};

function buildConversionBadges(unit: SankeyUnit): ConversionBadge[] {
  if (unit.stagesOrdered.length < 2) return [];
  const linksByKey = new Map(unit.links.map((item) => [`${item.fromStage}->${item.toStage}`, item]));
  const transitions = unit.stagesOrdered.length - 1;
  const badges: ConversionBadge[] = [];

  for (let i = 0; i < transitions; i += 1) {
    const from = unit.stagesOrdered[i];
    const to = unit.stagesOrdered[i + 1];
    const link = linksByKey.get(`${from}->${to}`);
    if (!link || typeof link.conversion !== "number") continue;
    const conversion = link.conversion;
    const pctInt = Math.round(conversion * 100);
    const toneClass = conversion >= 0.95
      ? "border-emerald-300 bg-emerald-50 text-emerald-700"
      : conversion >= 0.75
        ? "border-amber-300 bg-amber-50 text-amber-700"
        : "border-rose-300 bg-rose-50 text-rose-700";
    const hint = conversion >= 0.95
      ? "High conversion segment."
      : conversion >= 0.75
        ? "Average conversion segment."
        : "Low conversion segment.";

    badges.push({
      key: `${from}-${to}`,
      text: `${pctInt}%`,
      left: ((i + 0.5) / transitions) * 100,
      toneClass,
      hint,
    });
  }

  return badges;
}

function useElementSize<T extends HTMLElement>() {
  const ref = useRef<T | null>(null);
  const [size, setSize] = useState({ width: 0, height: 0 });

  useEffect(() => {
    const node = ref.current;
    if (!node) return;
    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) return;
      setSize({
        width: Math.max(0, Math.floor(entry.contentRect.width)),
        height: Math.max(0, Math.floor(entry.contentRect.height)),
      });
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  return { ref, size };
}

function FunnelRibbonChart({
  unit,
  benchmark,
  totalStudies,
  ranksByStage,
  showBenchmarkOverlay,
}: {
  unit: SankeyUnit;
  benchmark: JourneyBenchmarkAggregate;
  totalStudies: number;
  ranksByStage: JourneyModel["ranksByStage"];
  showBenchmarkOverlay: boolean;
}) {
  const { ref, size } = useElementSize<HTMLDivElement>();
  const [hoveredStage, setHoveredStage] = useState<string | null>(null);
  const [hoveredLink, setHoveredLink] = useState<string | null>(null);

  const geometry = useMemo(() => {
    const width = size.width;
    const height = size.height;
    if (width <= 0 || height <= 0) return null;

    const byStage = new Map(unit.stageAggregates.map((item) => [item.stage, item]));
    const benchmarkByStage = new Map(benchmark.stageAggregates.map((item) => [item.stage, item]));
    const count = unit.stagesOrdered.length;
    if (!count) return null;

    const sidePad = 44;
    const barWidth = 28;
    const trackWidth = Math.max(1, width - sidePad * 2);
    const step = count > 1 ? trackWidth / (count - 1) : 0;
    const maxValue = Math.max(
      ...unit.stagesOrdered.map((stage) => byStage.get(stage)?.value ?? 0),
      ...unit.stagesOrdered.map((stage) => benchmarkByStage.get(stage)?.value ?? 0),
      0.0001
    );
    const maxBarHeight = Math.min(height - 120, 300);
    const minBarHeight = 28;
    const centerY = height * 0.5;

    const stageGeom: StageGeometry[] = unit.stagesOrdered.map((stage, index) => {
      const info = byStage.get(stage);
      const value = typeof info?.value === "number" ? info.value : null;
      const ratio = value == null ? 0 : Math.max(0, Math.min(1, value / maxValue));
      const h = value == null ? minBarHeight : minBarHeight + ratio * (maxBarHeight - minBarHeight);
      const top = centerY - h / 2;
      const bottom = centerY + h / 2;
      const rank = value == null ? null : ranksByStage[stage]?.find((entry) => entry.brandName === unit.title)?.rank ?? null;
      return {
        stage,
        value,
        x: sidePad + step * index,
        top,
        bottom,
        height: h,
        coverageStudies: info?.stageCoverageStudies ?? 0,
        rank,
      };
    });

    const benchGeom = unit.stagesOrdered.map((stage, index) => {
      const info = benchmarkByStage.get(stage);
      const value = typeof info?.value === "number" ? info.value : null;
      const ratio = value == null ? 0 : Math.max(0, Math.min(1, value / maxValue));
      const h = value == null ? minBarHeight : minBarHeight + ratio * (maxBarHeight - minBarHeight);
      return {
        stage,
        value,
        x: sidePad + step * index,
        top: centerY - h / 2,
        bottom: centerY + h / 2,
      };
    });

    return { stageGeom, benchGeom, barWidth, labelNameY: 40, labelValueY: 56 };
  }, [benchmark.stageAggregates, ranksByStage, size.height, size.width, unit.stageAggregates, unit.stagesOrdered, unit.title]);

  const linksByKey = useMemo(
    () => new Map(unit.links.map((item) => [`${item.fromStage}->${item.toStage}`, item])),
    [unit.links]
  );
  const benchmarkLinksByKey = useMemo(
    () => new Map(benchmark.links.map((item) => [`${item.fromStage}->${item.toStage}`, item])),
    [benchmark.links]
  );

  const buildRibbonPath = (
    left: StageGeometry | { x: number; top: number; bottom: number },
    right: StageGeometry | { x: number; top: number; bottom: number },
    barWidth: number
  ) => {
    const x1 = left.x + barWidth / 2;
    const x2 = right.x - barWidth / 2;
    const dx = Math.max(1, x2 - x1);
    const c = 0.42;
    return [
      `M ${x1} ${left.top}`,
      `C ${x1 + dx * c} ${left.top}, ${x2 - dx * c} ${right.top}, ${x2} ${right.top}`,
      `L ${x2} ${right.bottom}`,
      `C ${x2 - dx * c} ${right.bottom}, ${x1 + dx * c} ${left.bottom}, ${x1} ${left.bottom}`,
      "Z",
    ].join(" ");
  };

  return (
    <div ref={ref} className="h-full w-full">
      {geometry && (
        <svg className="h-full w-full" viewBox={`0 0 ${size.width} ${size.height}`} preserveAspectRatio="none">
          {showBenchmarkOverlay &&
            geometry.benchGeom.slice(0, -1).map((from, index) => {
              const to = geometry.benchGeom[index + 1];
              if (from.value == null || to.value == null) return null;
              const link = benchmarkLinksByKey.get(`${from.stage}->${to.stage}`);
              return (
                <path
                  key={`bench-${from.stage}-${to.stage}`}
                  d={buildRibbonPath(from, to, geometry.barWidth)}
                  fill="rgba(148,163,184,0.24)"
                  stroke="rgba(100,116,139,0.55)"
                  strokeDasharray="4 3"
                  style={{ transition: "opacity 180ms ease, stroke-width 180ms ease" }}
                >
                  <title>
                    {`Benchmark transition\nFrom: ${stageShortLabel(from.stage)}\nTo: ${stageShortLabel(to.stage)}\nConversion: ${pct(typeof link?.conversion === "number" ? link.conversion : null)}\nDrop-off: ${pct(typeof link?.dropAbs === "number" ? link.dropAbs : null)}\nCoverage: ${link?.linkCoverageStudies ?? 0}/${totalStudies} studies`}
                  </title>
                </path>
              );
            })}

          {geometry.stageGeom.slice(0, -1).map((from, index) => {
            const to = geometry.stageGeom[index + 1];
            if (from.value == null || to.value == null) return null;
            const key = `${from.stage}->${to.stage}`;
            const link = linksByKey.get(key);
            const isHighlighted = hoveredLink === key || hoveredStage === from.stage || hoveredStage === to.stage;
            const isDimmed = (hoveredLink || hoveredStage) && !isHighlighted;
            return (
              <path
                key={key}
                d={buildRibbonPath(from, to, geometry.barWidth)}
                fill="rgba(147,197,253,0.42)"
                stroke="rgba(96,165,250,0.8)"
                onMouseEnter={() => setHoveredLink(key)}
                onMouseLeave={() => setHoveredLink(null)}
                style={{
                  opacity: isDimmed ? 0.34 : 0.92,
                  strokeWidth: isHighlighted ? 1.6 : 1.05,
                  transition: "opacity 180ms ease, stroke-width 180ms ease",
                  cursor: "pointer",
                }}
              >
                <title>
                  {`Brand transition\nFrom: ${stageShortLabel(from.stage)}\nTo: ${stageShortLabel(to.stage)}\nConversion: ${pct(typeof link?.conversion === "number" ? link.conversion : null)}\nDrop-off: ${pct(typeof link?.dropAbs === "number" ? link.dropAbs : null)}\nCoverage: ${link?.linkCoverageStudies ?? 0}/${totalStudies} studies`}
                </title>
              </path>
            );
          })}

          {geometry.stageGeom.map((stage) => {
            const isHighlighted = hoveredStage === stage.stage;
            const isDimmed = Boolean(hoveredStage || hoveredLink) && !isHighlighted && !hoveredLink?.includes(stage.stage);
            return (
              <g key={stage.stage}>
                <rect
                  x={stage.x - geometry.barWidth / 2}
                  y={stage.top}
                  width={geometry.barWidth}
                  height={stage.height}
                  rx={9}
                  fill={stage.value == null ? "rgba(148,163,184,0.24)" : "#7dd3fc"}
                  stroke={stage.value == null ? "rgba(148,163,184,0.45)" : "#0284c7"}
                  strokeWidth={isHighlighted ? 1.7 : 1.15}
                  onMouseEnter={() => setHoveredStage(stage.stage)}
                  onMouseLeave={() => setHoveredStage(null)}
                  style={{
                    opacity: isDimmed ? 0.45 : 1,
                    filter: isHighlighted ? "drop-shadow(0 0 6px rgba(56,189,248,0.45))" : "none",
                    transition: "opacity 180ms ease, stroke-width 180ms ease, filter 180ms ease",
                    cursor: "pointer",
                  }}
                >
                  <title>
                    {stage.value == null
                      ? `${stage.stage}\nNot available in selected studies.`
                      : `Stage: ${stage.stage}\nValue: ${pct(stage.value)}\nCoverage: ${stage.coverageStudies}/${totalStudies} studies${stage.rank ? `\nRank: #${stage.rank}` : ""}`}
                  </title>
                </rect>

                <text x={stage.x} y={geometry.labelNameY} fill="#334155" fontSize="9" textAnchor="middle">
                  {stageShortLabel(stage.stage)}
                </text>
                <text x={stage.x} y={geometry.labelValueY} fill="#0f172a" fontSize="12" fontWeight={700} textAnchor="middle">
                  {pct(stage.value)}
                </text>
              </g>
            );
          })}
        </svg>
      )}
    </div>
  );
}

const SankeyBrandCard = memo(function SankeyBrandCard({
  unit,
  benchmark,
  totalStudies,
  ranksByStage,
  showBenchmarkOverlay,
}: {
  unit: SankeyUnit;
  benchmark: JourneyBenchmarkAggregate;
  totalStudies: number;
  ranksByStage: JourneyModel["ranksByStage"];
  showBenchmarkOverlay: boolean;
}) {
  const badges = useMemo(() => buildConversionBadges(unit), [unit]);

  return (
    <article
      className={`rounded-2xl border border-ink/10 bg-slate-50/60 p-4 ${
        unit.isFocused ? "ring-1 ring-emerald-300/70" : ""
      }`}
    >
      <div className="mb-3 flex items-center justify-between gap-3">
        <p className="text-sm font-semibold text-ink">{unit.title}</p>
        <div className="flex items-center gap-2">
          <span
            className={`rounded-full border px-2.5 py-1 text-[11px] ${healthBadgeStyle(unit.funnelHealth?.status || "unknown")}`}
            title={
              unit.funnelHealth
                ? [
                    `Biggest drop: ${unit.funnelHealth.link ? `${unit.funnelHealth.link.fromStage} -> ${unit.funnelHealth.link.toStage}` : "n/a"}`,
                    `Drop: ${unit.funnelHealth.maxDropPts == null ? "--" : `-${unit.funnelHealth.maxDropPts.toFixed(1)} pts`}`,
                    unit.funnelHealth.benchMaxDropPts == null
                      ? "Benchmark: --"
                      : `Benchmark: -${unit.funnelHealth.benchMaxDropPts.toFixed(1)} pts`,
                    `Studies: ${unit.funnelHealth.studiesCovered}`,
                  ].join("\n")
                : "No funnel health signal."
            }
          >
            {healthBadgeLabel(unit.funnelHealth?.status || "unknown")}
          </span>
        </div>
      </div>

      <JourneyKpiStrip brand={unit} benchmark={benchmark} journeyIndex={unit.journeyIndex} />

      <div className="relative mt-4 h-[420px] w-full rounded-2xl border border-ink/10 bg-white/90">
        <div className="pointer-events-none absolute left-0 right-0 top-6 z-10 h-8">
          {badges.map((badge) => (
            <div
              key={badge.key}
              className={`absolute -translate-x-1/2 rounded-full border px-2 py-0.5 text-[10px] font-semibold shadow-sm ${badge.toneClass}`}
              style={{ left: `${badge.left}%` }}
              title={badge.hint}
            >
              {badge.text}
            </div>
          ))}
        </div>
        <FunnelRibbonChart
          unit={unit}
          benchmark={benchmark}
          totalStudies={totalStudies}
          ranksByStage={ranksByStage}
          showBenchmarkOverlay={showBenchmarkOverlay}
        />
      </div>
    </article>
  );
});

export default function HeroSankey({
  model,
  selectedBrandNames,
  focusBrandName = null,
  compareBrandName = null,
  timeBucketLabel = null,
}: HeroSankeyProps) {
  const [showBenchmarkOverlay, setShowBenchmarkOverlay] = useState(true);

  const totalStudies = useMemo(() => new Set(model.rows.map((row) => row.studyId)).size, [model.rows]);

  const selectedBrands = useMemo(() => {
    const all = model.brandStageAggregates;
    if (!selectedBrandNames.length) {
      const firstStage = model.stagesOrdered[0];
      return all
        .slice()
        .sort(
          (a, b) =>
            (b.stageAggregates.find((item) => item.stage === firstStage)?.value ?? 0) -
            (a.stageAggregates.find((item) => item.stage === firstStage)?.value ?? 0)
        );
    }
    return all.filter((item) => selectedBrandNames.includes(item.brandName));
  }, [model.brandStageAggregates, model.stagesOrdered, selectedBrandNames]);

  const limitedBrands = useMemo(() => {
    const sorted = selectedBrands
      .slice()
      .sort((a, b) => (b.totalConversion || 0) - (a.totalConversion || 0));
    return sorted.slice(0, 5);
  }, [selectedBrands]);

  const hiddenCount = Math.max(0, selectedBrands.length - limitedBrands.length);

  const units = useMemo<SankeyUnit[]>(() => {
    const makeUnit = (brand: JourneyBrandAggregate): SankeyUnit => ({
      key: brand.key,
      title: brand.brandName,
      stagesOrdered: model.stagesOrdered,
      stageAggregates: brand.stageAggregates,
      links: brand.links,
      csat: brand.csat,
      nps: brand.nps,
      journeyIndex: model.journeyIndexByBrand[brand.key] ?? null,
      funnelHealth: model.funnelHealthByBrand[brand.key] ?? null,
      isFocused: focusBrandName ? brand.brandName === focusBrandName : false,
    });

    if (focusBrandName) {
      const focused = selectedBrands.find((brand) => brand.brandName === focusBrandName);
      const compare = compareBrandName
        ? selectedBrands.find((brand) => brand.brandName === compareBrandName)
        : null;
      const focusedUnits: SankeyUnit[] = [];
      if (focused) focusedUnits.push(makeUnit(focused));
      if (compare && compare.brandName !== focused?.brandName) focusedUnits.push(makeUnit(compare));
      return focusedUnits;
    }

    return limitedBrands.map(makeUnit);
  }, [compareBrandName, focusBrandName, limitedBrands, model.stagesOrdered, selectedBrands]);

  if (!model.rows.length) {
    return (
      <section className="main-surface p-6">
        <h3 className="text-xl font-semibold">Journey Hero</h3>
        <p className="mt-3 text-sm text-slate">No hay resultados para esta seleccion.</p>
      </section>
    );
  }

  return (
    <section className="main-surface p-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h3 className="text-xl font-semibold">Brand Funnel</h3>
          <p className="text-sm text-slate">
            {focusBrandName
              ? `Focus: ${focusBrandName}${compareBrandName ? ` vs ${compareBrandName}` : ""}`
              : "Evolución por etapa y comparativa vs benchmark de categoría."}
            {timeBucketLabel ? ` · ${timeBucketLabel}` : ""}
          </p>
        </div>

        <div className="flex flex-wrap items-center gap-3">
          <label className="flex items-center gap-2 rounded-full border border-ink/10 bg-white px-3 py-1.5 text-xs text-slate">
            <input
              type="checkbox"
              checked={showBenchmarkOverlay}
              onChange={(event) => setShowBenchmarkOverlay(event.target.checked)}
            />
            Show benchmark overlay
          </label>
          <div className="flex items-center gap-2 text-[11px] text-slate">
            <span className="inline-block h-0.5 w-4 rounded bg-sky-400" /> Brand
            {showBenchmarkOverlay && (
              <>
                <span className="ml-2 inline-block h-0.5 w-4 rounded border-t border-dashed border-slate-500" /> Benchmark
              </>
            )}
          </div>
        </div>
      </div>

      {!focusBrandName && hiddenCount > 0 && (
        <div className="mt-4">
          <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-3 py-1 text-xs text-amber-700">
            Mostrando 5 de {selectedBrands.length} marcas (ajusta filtros).
          </span>
        </div>
      )}

      <div className="mt-4 space-y-4">
        {units.map((unit) => (
          <SankeyBrandCard
            key={unit.key}
            unit={unit}
            benchmark={model.benchmarkStageAggregates}
            totalStudies={totalStudies}
            ranksByStage={model.ranksByStage}
            showBenchmarkOverlay={showBenchmarkOverlay}
          />
        ))}
      </div>
    </section>
  );
}
