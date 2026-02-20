"use client";

import ReactECharts from "echarts-for-react";
import { useMemo } from "react";
import type { EChartsOption } from "echarts";

import type {
  JourneyBenchmarkAggregate,
  JourneyBrandAggregate,
  JourneyModel,
  JourneyStage,
  JourneyStageAggregate,
} from "../data/journeySchema";

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
  isBenchmark: boolean;
  stagesOrdered: JourneyStage[];
  stageAggregates: JourneyStageAggregate[];
  links: JourneyBrandAggregate["links"] | JourneyBenchmarkAggregate["links"];
};

const pct = (value: number | null) => (typeof value === "number" ? `${(value * 100).toFixed(1)}%` : "--");

function buildSankeyOption(
  unit: SankeyUnit,
  totalStudies: number,
  ranksByStage: JourneyModel["ranksByStage"]
): EChartsOption {
  const byStage = new Map(unit.stageAggregates.map((item) => [item.stage, item]));
  const byLink = new Map(unit.links.map((item) => [`${item.fromStage}->${item.toStage}`, item]));

  const nodes = unit.stagesOrdered.map((stage) => {
    const info = byStage.get(stage);
    const value = info?.value ?? null;
    const missing = value == null;
    return {
      name: stage,
      value: value ?? 0,
      itemStyle: {
        color: missing ? "rgba(148,163,184,0.25)" : unit.isBenchmark ? "rgba(15,23,42,0.18)" : "#67e8f9",
        borderColor: missing ? "rgba(148,163,184,0.5)" : unit.isBenchmark ? "rgba(15,23,42,0.3)" : "#0ea5a4",
        borderWidth: 1,
      },
      // custom tooltip payload
      stage,
      stageValue: value,
      stageCoverageStudies: info?.stageCoverageStudies ?? 0,
      stageCoverageWeight: info?.stageCoverageWeight ?? 0,
      rank:
        unit.isBenchmark || missing
          ? null
          : ranksByStage[stage]?.find((entry) => entry.brandName === unit.title)?.rank ?? null,
      isMissing: missing,
    };
  });

  const links: Array<Record<string, unknown>> = [];
  for (let i = 0; i < unit.stagesOrdered.length - 1; i += 1) {
    const from = unit.stagesOrdered[i];
    const to = unit.stagesOrdered[i + 1];
    const link = byLink.get(`${from}->${to}`);
    if (!link) continue;
    const fromValue = byStage.get(from)?.value ?? null;
    const toValue = byStage.get(to)?.value ?? null;
    let value: number | null = null;
    if (typeof link.conversion === "number" && typeof fromValue === "number") {
      value = link.conversion * fromValue;
    } else if (typeof fromValue === "number" && typeof toValue === "number") {
      value = Math.min(fromValue, toValue);
    }
    if (value == null || value <= 0) continue;
    links.push({
      source: from,
      target: to,
      value,
      conversion: link.conversion,
      dropAbs: link.dropAbs,
      linkCoverageStudies: link.linkCoverageStudies,
      linkCoverageWeight: link.linkCoverageWeight,
    });
  }

  return {
    tooltip: {
      trigger: "item",
      backgroundColor: "rgba(15, 23, 42, 0.96)",
      borderColor: "rgba(148, 163, 184, 0.4)",
      textStyle: { color: "#f8fafc", fontSize: 12 },
      formatter: (params: any) => {
        if (params.dataType === "edge") {
          const conversion = pct(
            typeof params.data?.conversion === "number" ? (params.data.conversion as number) : null
          );
          const dropAbs = pct(typeof params.data?.dropAbs === "number" ? (params.data.dropAbs as number) : null);
          const coverageStudies = params.data?.linkCoverageStudies ?? 0;
          return [
            `${params.data.source} -> ${params.data.target}`,
            `Conversion: ${conversion}`,
            `Drop-off: ${dropAbs}`,
            `Coverage: ${coverageStudies}/${totalStudies} studies`,
          ].join("<br/>");
        }
        const node = params.data || {};
        if (node.isMissing) {
          return [`${node.stage}`, "No disponible en estudios seleccionados."].join("<br/>");
        }
        return [
          `${node.stage}`,
          `Value: ${pct(typeof node.stageValue === "number" ? node.stageValue : null)}`,
          `Coverage: ${node.stageCoverageStudies}/${totalStudies} studies`,
          node.rank ? `Rank: #${node.rank}` : "",
        ]
          .filter(Boolean)
          .join("<br/>");
      },
    },
    series: [
      {
        type: "sankey",
        layoutIterations: 0,
        left: 22,
        right: 22,
        top: 18,
        bottom: 16,
        nodeAlign: "justify",
        nodeGap: 20,
        nodeWidth: 16,
        draggable: false,
        emphasis: { focus: "adjacency" },
        lineStyle: {
          color: "source",
          opacity: 0.45,
          curveness: 0.5,
        },
        label: {
          color: "#0f172a",
          fontSize: 11,
          distance: 8,
        },
        data: nodes,
        links,
        animationDurationUpdate: 280,
        animationEasingUpdate: "cubicOut",
      } as any,
    ],
  };
}

export default function HeroSankey({
  model,
  selectedBrandNames,
  focusBrandName = null,
  compareBrandName = null,
  timeBucketLabel = null,
}: HeroSankeyProps) {
  const totalStudies = useMemo(() => new Set(model.rows.map((row) => row.studyId)).size, [model.rows]);

  const selectedBrands = useMemo(() => {
    const all = model.brandStageAggregates;
    if (!selectedBrandNames.length) {
      const firstStage = model.stagesOrdered[0];
      const sorted = all
        .slice()
        .sort(
          (a, b) =>
            (b.stageAggregates.find((item) => item.stage === firstStage)?.value ?? 0) -
            (a.stageAggregates.find((item) => item.stage === firstStage)?.value ?? 0)
        );
      return sorted.slice(0, 1);
    }
    const selected = all.filter((item) => selectedBrandNames.includes(item.brandName));
    const firstStage = model.stagesOrdered[0];
    return selected
      .slice()
      .sort(
        (a, b) =>
          (b.stageAggregates.find((item) => item.stage === firstStage)?.value ?? 0) -
          (a.stageAggregates.find((item) => item.stage === firstStage)?.value ?? 0)
      );
  }, [model.brandStageAggregates, model.stagesOrdered, selectedBrandNames]);

  const limitedBrands = selectedBrands.slice(0, 5);
  const hiddenCount = Math.max(0, selectedBrands.length - limitedBrands.length);

  const units = useMemo<SankeyUnit[]>(() => {
    const makeUnit = (brand: JourneyBrandAggregate): SankeyUnit => ({
      key: brand.key,
      title: brand.brandName,
      isBenchmark: false,
      stagesOrdered: model.stagesOrdered,
      stageAggregates: brand.stageAggregates,
      links: brand.links,
    });
    const brandUnits: SankeyUnit[] = focusBrandName
      ? (() => {
          const focused = selectedBrands.find((brand) => brand.brandName === focusBrandName);
          const compare = compareBrandName
            ? selectedBrands.find((brand) => brand.brandName === compareBrandName)
            : null;
          const unitsInternal: SankeyUnit[] = [];
          if (focused) unitsInternal.push(makeUnit(focused));
          if (compare && compare.brandName !== focused?.brandName) unitsInternal.push(makeUnit(compare));
          return unitsInternal;
        })()
      : limitedBrands.map(makeUnit);
    const benchmarkUnit: SankeyUnit = {
      key: "benchmark",
      title: `Benchmark (${model.benchmarkStageAggregates.scope})`,
      isBenchmark: true,
      stagesOrdered: model.stagesOrdered,
      stageAggregates: model.benchmarkStageAggregates.stageAggregates,
      links: model.benchmarkStageAggregates.links,
    };
    return [...brandUnits, benchmarkUnit];
  }, [
    compareBrandName,
    focusBrandName,
    limitedBrands,
    model.benchmarkStageAggregates,
    model.stagesOrdered,
    selectedBrands,
  ]);

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
          <h3 className="text-xl font-semibold">Journey Hero Sankey</h3>
          <p className="text-sm text-slate">
            {focusBrandName
              ? `Focus: ${focusBrandName}${compareBrandName ? ` vs ${compareBrandName}` : ""} + benchmark`
              : "Comparativo por marca + benchmark (categoria)."}
            {timeBucketLabel ? ` · ${timeBucketLabel}` : ""}
          </p>
        </div>
        {!focusBrandName && hiddenCount > 0 && (
          <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-3 py-1 text-xs text-amber-700">
            Mostrando 5 de {selectedBrands.length} marcas (ajusta filtros).
          </span>
        )}
      </div>

      <div className="mt-4 space-y-3">
        {units.map((unit) => {
          const lowCoverageStages = unit.stageAggregates.filter(
            (stage) => totalStudies > 0 && stage.stageCoverageStudies / totalStudies < 0.5
          ).length;
          return (
            <article
              key={unit.key}
              className={`rounded-2xl border border-ink/10 bg-slate-50/60 p-3 ${
                focusBrandName && !unit.isBenchmark && unit.title === focusBrandName ? "ring-1 ring-emerald-300/70" : ""
              } ${focusBrandName && unit.isBenchmark ? "opacity-80" : ""}`}
            >
              <div className="mb-2 flex items-center justify-between gap-3">
                <p className="text-sm font-semibold text-ink">{unit.title}</p>
                {lowCoverageStages > 0 && (
                  <span className="rounded-full border border-amber-500/30 bg-amber-500/10 px-2 py-0.5 text-[11px] text-amber-700">
                    Low coverage
                  </span>
                )}
              </div>
              <div className={`w-full ${focusBrandName && !unit.isBenchmark && unit.title === focusBrandName ? "h-[360px]" : "h-[260px]"}`}>
                <ReactECharts
                  style={{ height: "100%", width: "100%" }}
                  option={buildSankeyOption(unit, totalStudies, model.ranksByStage)}
                  notMerge
                  lazyUpdate
                />
              </div>
            </article>
          );
        })}
      </div>
    </section>
  );
}
