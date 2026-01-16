"use client";

import { useEffect, useMemo, useState } from "react";
import ReactECharts from "echarts-for-react";

import { fetchJourney } from "../lib/api";
import { JourneyPoint } from "../lib/types";

const STAGES = [
  { value: "awareness", label: "Brand Awareness" },
  { value: "ad_awareness", label: "Ad Awareness" },
  { value: "consideration", label: "Brand Consideration" },
  { value: "purchase", label: "Brand Purchase" },
  { value: "satisfaction", label: "Brand Satisfaction" },
  { value: "recommendation", label: "Brand Recommendation" },
  { value: "touchpoints", label: "Touchpoints" },
];

export default function JourneyChart({ studyId }: { studyId: string }) {
  const [points, setPoints] = useState<JourneyPoint[]>([]);
  const [status, setStatus] = useState<"idle" | "loading" | "error">("idle");
  const [source, setSource] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    setStatus("loading");

    fetchJourney(studyId)
      .then((data) => {
        if (!active) return;
        setPoints(data.points);
        setSource(data.source ?? null);
        setStatus("idle");
      })
      .catch(() => {
        if (!active) return;
        setStatus("error");
      });

    return () => {
      active = false;
    };
  }, [studyId]);

  const chartOption = useMemo(() => {
    const brands = Array.from(new Set(points.map((point) => point.brand))).sort();
    const series = brands.map((brand) => {
      const brandPoints = STAGES.map((stage) => {
        const match = points.find((point) => point.brand === brand && point.stage === stage.value);
        return match ? Number(match.percentage.toFixed(1)) : 0;
      });
      return {
        name: brand,
        type: "bar",
        data: brandPoints,
      };
    });

    return {
      tooltip: { trigger: "axis" },
      legend: { top: 0 },
      grid: { left: 40, right: 20, bottom: 30, top: 40 },
      xAxis: { type: "category", data: STAGES.map((stage) => stage.label) },
      yAxis: { type: "value", axisLabel: { formatter: "{value}%" } },
      series,
    };
  }, [points]);

  if (status === "loading") {
    return <p className="text-slate">Loading journey metrics...</p>;
  }

  if (status === "error") {
    return <p className="text-red-600">Unable to load journey data. Seed demo data first.</p>;
  }

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <p className="text-sm text-slate">Journey metrics by stage and brand.</p>
        {source && (
          <span className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-3 py-1 text-xs text-emerald-700">
            {source}
          </span>
        )}
      </div>
      <ReactECharts option={chartOption} style={{ height: 420 }} />
    </div>
  );
}
