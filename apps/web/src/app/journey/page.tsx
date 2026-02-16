"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { postJourneyTableMultiDetailed, postTouchpointsTableMultiDetailed } from "../../lib/api";
import { useScope } from "../../components/layout/ScopeProvider";

type TableRow = {
  study_id: string;
  sector: string | null;
  subsector: string | null;
  category: string | null;
  brand: string;
  brand_awareness: number | null;
  ad_awareness: number | null;
  brand_consideration: number | null;
  brand_purchase: number | null;
  brand_satisfaction: number | null;
  brand_recommendation: number | null;
};

type TouchpointRow = {
  study_id: string;
  study_name?: string | null;
  sector: string | null;
  subsector: string | null;
  category: string | null;
  brand: string;
  touchpoint: string;
  recall: number | null;
};

const firstOrNull = (values: string[]) => (values.length ? values[0] : null);

export default function JourneyPage() {
  const { scope } = useScope();
  const [limitMode, setLimitMode] = useState<"top10" | "top25" | "all">("top10");
  const [touchpointLimit, setTouchpointLimit] = useState<"top10" | "top25" | "all">("top25");
  const [tableRows, setTableRows] = useState<TableRow[]>([]);
  const [tableState, setTableState] = useState<"idle" | "loading" | "error">("idle");
  const [tableMessage, setTableMessage] = useState<string | null>(null);
  const [tableSource, setTableSource] = useState<string | null>(null);
  const [touchpointRows, setTouchpointRows] = useState<TouchpointRow[]>([]);
  const [touchpointState, setTouchpointState] = useState<"idle" | "loading" | "error">("idle");
  const [touchpointMessage, setTouchpointMessage] = useState<string | null>(null);

  useEffect(() => {
    setTableState("loading");
    setTableMessage(null);
    postJourneyTableMultiDetailed(
      {
        study_ids: scope.studyIds,
        sector: scope.sector,
        subsector: scope.subsector,
        category: scope.category,
        gender: firstOrNull(scope.gender),
        nse: firstOrNull(scope.nse),
        state: firstOrNull(scope.state),
        age_min: scope.ageMin,
        age_max: scope.ageMax,
        date_grain: scope.timeGranularity,
        quarter_from: scope.quarterFrom,
        quarter_to: scope.quarterTo,
      },
      limitMode
    )
      .then((result) => {
        if (!result.ok) {
          setTableState("error");
          const message =
            result.data && typeof result.data === "object" && "detail" in result.data
              ? String(result.data.detail)
              : result.error || "Unable to load table.";
          setTableMessage(message);
          setTableRows([]);
          return;
        }
        const data = result.data as { rows?: TableRow[] };
        setTableRows(Array.isArray(data.rows) ? data.rows : []);
        setTableSource("curated");
        setTableState("idle");
      })
      .catch((error) => {
        setTableState("error");
        setTableMessage(error instanceof Error ? error.message : "Unable to load table.");
        setTableRows([]);
      });
  }, [scope, limitMode]);

  useEffect(() => {
    setTouchpointState("loading");
    setTouchpointMessage(null);
    postTouchpointsTableMultiDetailed(
      {
        study_ids: scope.studyIds,
        sector: scope.sector,
        subsector: scope.subsector,
        category: scope.category,
        gender: firstOrNull(scope.gender),
        nse: firstOrNull(scope.nse),
        state: firstOrNull(scope.state),
        age_min: scope.ageMin,
        age_max: scope.ageMax,
        date_grain: scope.timeGranularity,
        quarter_from: scope.quarterFrom,
        quarter_to: scope.quarterTo,
      },
      touchpointLimit
    )
      .then((result) => {
        if (!result.ok) {
          setTouchpointState("error");
          const message =
            result.data && typeof result.data === "object" && "detail" in result.data
              ? String(result.data.detail)
              : result.error || "Unable to load touchpoints.";
          setTouchpointMessage(message);
          setTouchpointRows([]);
          return;
        }
        const data = result.data as { rows?: TouchpointRow[] };
        setTouchpointRows(Array.isArray(data.rows) ? data.rows : []);
        setTouchpointState("idle");
      })
      .catch((error) => {
        setTouchpointState("error");
        setTouchpointMessage(error instanceof Error ? error.message : "Unable to load touchpoints.");
        setTouchpointRows([]);
      });
  }, [scope, touchpointLimit]);

  return (
    <main className="space-y-6">
      <section className="main-surface p-6">
        <div className="flex items-center justify-between">
          <h3 className="text-xl font-semibold">Journey Results</h3>
          {tableSource && (
            <span className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-3 py-1 text-xs text-emerald-700">
              source: {tableSource}
            </span>
          )}
        </div>
        <div className="mt-4 flex flex-wrap items-center gap-2 text-xs">
          <span className="text-slate">View:</span>
          {(["top10", "top25", "all"] as const).map((mode) => (
            <button
              key={mode}
              type="button"
              onClick={() => setLimitMode(mode)}
              className={`rounded-full border px-3 py-1 ${
                limitMode === mode
                  ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
                  : "border-ink/10 text-slate"
              }`}
            >
              {mode === "top10" ? "Top 10" : mode === "top25" ? "Top 25" : "All"}
            </button>
          ))}
          <span className="text-[10px] text-slate">Sorted by Brand Awareness (desc)</span>
        </div>
        {tableState === "loading" && <p className="mt-4 text-sm text-slate">Loading curated results...</p>}
        {tableState === "error" && (
          <p className="mt-4 text-sm text-red-600">
            {tableMessage || "Results not published for this study yet. Go to Admin → Publish Journey Results."}{" "}
            <Link className="underline" href="/admin">
              Open Admin
            </Link>
          </p>
        )}
        {tableState !== "error" && tableRows.length > 0 && (
          <div className="mt-4 max-h-[520px] overflow-auto">
            <table className="w-full border-collapse text-sm">
              <thead>
                <tr className="border-b border-ink/10 text-left">
                  <th className="py-2 font-semibold">Study</th>
                  <th className="py-2 font-semibold">Sector</th>
                  <th className="py-2 font-semibold">Subsector</th>
                  <th className="py-2 font-semibold">Category</th>
                  <th className="py-2 font-semibold">Brand</th>
                  <th className="py-2 text-right font-semibold">Brand Awareness</th>
                  <th className="py-2 text-right font-semibold">Ad Awareness</th>
                  <th className="py-2 text-right font-semibold">Brand Consideration</th>
                  <th className="py-2 text-right font-semibold">Brand Purchase</th>
                  <th className="py-2 text-right font-semibold">Brand Satisfaction</th>
                  <th className="py-2 text-right font-semibold">Brand Recommendation</th>
                </tr>
              </thead>
              <tbody>
                {tableRows.map((row, index) => (
                  <tr key={`${row.study_id}-${row.brand}-${index}`} className="border-b border-ink/5">
                    <td className="py-2">{row.study_id}</td>
                    <td className="py-2">{row.sector || "--"}</td>
                    <td className="py-2">{row.subsector || "--"}</td>
                    <td className="py-2">{row.category || "--"}</td>
                    <td className="py-2">{row.brand}</td>
                    <td className="py-2 text-right">
                      {row.brand_awareness === null || row.brand_awareness === undefined
                        ? "--"
                        : `${row.brand_awareness}%`}
                    </td>
                    <td className="py-2 text-right">
                      {row.ad_awareness === null || row.ad_awareness === undefined ? "--" : `${row.ad_awareness}%`}
                    </td>
                    <td className="py-2 text-right">
                      {row.brand_consideration === null || row.brand_consideration === undefined
                        ? "--"
                        : `${row.brand_consideration}%`}
                    </td>
                    <td className="py-2 text-right">
                      {row.brand_purchase === null || row.brand_purchase === undefined
                        ? "--"
                        : `${row.brand_purchase}%`}
                    </td>
                    <td className="py-2 text-right">
                      {row.brand_satisfaction === null || row.brand_satisfaction === undefined
                        ? "--"
                        : `${row.brand_satisfaction}%`}
                    </td>
                    <td className="py-2 text-right">
                      {row.brand_recommendation === null || row.brand_recommendation === undefined
                        ? "--"
                        : `${row.brand_recommendation}%`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {tableState !== "error" && tableRows.length === 0 && (
          <p className="mt-4 text-sm text-slate">
            Results not published for this study yet. Go to Admin → Publish Journey Results.{" "}
            <Link className="underline" href="/admin">
              Open Admin
            </Link>
          </p>
        )}
      </section>

      <section className="main-surface p-6">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-xl font-semibold">Touchpoints (Recall)</h3>
            <p className="text-sm text-slate">Recall = % value == 1 within touchpoints module.</p>
          </div>
        </div>
        <div className="mt-4 flex flex-wrap items-center gap-2 text-xs">
          <span className="text-slate">View:</span>
          {(["top10", "top25", "all"] as const).map((mode) => (
            <button
              key={mode}
              type="button"
              onClick={() => setTouchpointLimit(mode)}
              className={`rounded-full border px-3 py-1 ${
                touchpointLimit === mode
                  ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
                  : "border-ink/10 text-slate"
              }`}
            >
              {mode === "top10" ? "Top 10" : mode === "top25" ? "Top 25" : "All"}
            </button>
          ))}
          <span className="text-[10px] text-slate">Sorted by Recall (desc)</span>
        </div>
        {touchpointState === "loading" && <p className="mt-4 text-sm text-slate">Loading touchpoints...</p>}
        {touchpointState === "error" && <p className="mt-4 text-sm text-red-600">{touchpointMessage}</p>}
        {touchpointState !== "error" && touchpointRows.length > 0 && (
          <div className="mt-4 max-h-[520px] overflow-auto">
            <table className="w-full border-collapse text-sm">
              <thead>
                <tr className="border-b border-ink/10 text-left">
                  <th className="py-2 font-semibold">Study</th>
                  <th className="py-2 font-semibold">Sector</th>
                  <th className="py-2 font-semibold">Subsector</th>
                  <th className="py-2 font-semibold">Category</th>
                  <th className="py-2 font-semibold">Brand</th>
                  <th className="py-2 font-semibold">Touchpoint</th>
                  <th className="py-2 text-right font-semibold">Recall %</th>
                </tr>
              </thead>
              <tbody>
                {touchpointRows.map((row, index) => (
                  <tr key={`${row.study_id}-${row.brand}-${row.touchpoint}-${index}`} className="border-b border-ink/5">
                    <td className="py-2">{row.study_id}</td>
                    <td className="py-2">{row.sector || "--"}</td>
                    <td className="py-2">{row.subsector || "--"}</td>
                    <td className="py-2">{row.category || "--"}</td>
                    <td className="py-2">{row.brand}</td>
                    <td className="py-2">{row.touchpoint}</td>
                    <td className="py-2 text-right">
                      {row.recall === null || row.recall === undefined ? "--" : `${row.recall}%`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {touchpointState !== "error" && touchpointRows.length === 0 && (
          <p className="mt-4 text-sm text-slate">
            No touchpoint data available yet. Map touchpoints rules and publish curated to see results.
          </p>
        )}
      </section>
    </main>
  );
}
