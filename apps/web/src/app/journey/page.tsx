"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";

import JourneyChart from "../../components/JourneyChart";
import SeedDemoButton from "../../components/SeedDemoButton";
import { fetchJourneyTableDetailed, getStudiesDetailed } from "../../lib/api";
import { Study } from "../../lib/types";

type TableRow = {
  brand: string;
  awareness: number | null;
  consideration: number | null;
  purchase: number | null;
};

export default function JourneyPage() {
  const searchParams = useSearchParams();
  const [studyId, setStudyId] = useState("demo_001");
  const [studies, setStudies] = useState<Study[]>([]);
  const [tableRows, setTableRows] = useState<TableRow[]>([]);
  const [tableState, setTableState] = useState<"idle" | "loading" | "error">("idle");
  const [tableMessage, setTableMessage] = useState<string | null>(null);
  const [tableSource, setTableSource] = useState<string | null>(null);

  const queryStudyId = useMemo(() => searchParams.get("study_id"), [searchParams]);

  useEffect(() => {
    const loadStudies = async () => {
      const result = await getStudiesDetailed(true);
      if (result.ok && result.data) {
        const payload = result.data as { studies?: Study[] } | Study[];
        const items = Array.isArray(payload) ? payload : payload.studies || [];
        setStudies(items);
      }
    };

    loadStudies();
  }, []);

  useEffect(() => {
    if (queryStudyId) {
      setStudyId(queryStudyId);
      return;
    }
    if (studies.length > 0) {
      const nonDemo = studies.find((item) => item.source !== "demo");
      setStudyId(nonDemo?.id || studies[0].id);
    }
  }, [queryStudyId, studies]);

  useEffect(() => {
    if (!studyId) return;
    setTableState("loading");
    setTableMessage(null);
    fetchJourneyTableDetailed(studyId)
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
        const data = result.data as { rows?: TableRow[]; source?: string };
        setTableRows(Array.isArray(data.rows) ? data.rows : []);
        setTableSource(data.source || "curated");
        setTableState("idle");
      })
      .catch((error) => {
        setTableState("error");
        setTableMessage(error instanceof Error ? error.message : "Unable to load table.");
        setTableRows([]);
      });
  }, [studyId]);

  return (
    <main className="space-y-6">
      <section className="main-surface rounded-3xl p-6">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <h2 className="text-2xl font-semibold">Journey Benchmark</h2>
            <p className="text-slate">Compare brand performance across funnel stages.</p>
          </div>
          <SeedDemoButton />
        </div>
        <div className="mt-6">
          <div className="flex flex-col gap-2">
            <label className="text-sm font-medium text-slate">Study</label>
            <select
              className="rounded-xl border border-ink/10 bg-white px-4 py-2"
              value={studyId}
              onChange={(event) => setStudyId(event.target.value)}
            >
              {studies.length === 0 ? (
                <option value={studyId}>{studyId}</option>
              ) : (
                studies.map((study) => (
                  <option key={study.id} value={study.id}>
                    {study.id}
                  </option>
                ))
              )}
            </select>
          </div>
        </div>
      </section>

      <section className="main-surface rounded-3xl p-6">
        <JourneyChart studyId={studyId} />
      </section>

      <section className="main-surface rounded-3xl p-6">
        <div className="flex items-center justify-between">
          <h3 className="text-xl font-semibold">Journey Results</h3>
          {tableSource && (
            <span className="rounded-full border border-emerald-500/30 bg-emerald-500/10 px-3 py-1 text-xs text-emerald-700">
              source: {tableSource}
            </span>
          )}
        </div>
        {tableState === "loading" && (
          <p className="mt-4 text-sm text-slate">Loading curated results...</p>
        )}
        {tableState === "error" && (
          <p className="mt-4 text-sm text-red-600">
            {tableMessage ||
              "Results not published for this study yet. Go to Admin -> Publish Journey Results."}{" "}
            <Link className="underline" href={`/admin?study_id=${encodeURIComponent(studyId)}`}>
              Open Admin
            </Link>
          </p>
        )}
        {tableState !== "error" && tableRows.length > 0 && (
          <div className="mt-4 overflow-auto">
            <table className="w-full border-collapse text-sm">
              <thead>
                <tr className="border-b border-ink/10 text-left">
                  <th className="py-2 font-semibold">Brand</th>
                  <th className="py-2 text-right font-semibold">Awareness</th>
                  <th className="py-2 text-right font-semibold">Consideration</th>
                  <th className="py-2 text-right font-semibold">Purchase</th>
                </tr>
              </thead>
              <tbody>
                {tableRows.map((row) => (
                  <tr key={row.brand} className="border-b border-ink/5">
                    <td className="py-2">{row.brand}</td>
                    <td className="py-2 text-right">
                      {row.awareness === null || row.awareness === undefined ? "--" : `${row.awareness}%`}
                    </td>
                    <td className="py-2 text-right">
                      {row.consideration === null || row.consideration === undefined
                        ? "--"
                        : `${row.consideration}%`}
                    </td>
                    <td className="py-2 text-right">
                      {row.purchase === null || row.purchase === undefined ? "--" : `${row.purchase}%`}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {tableState !== "error" && tableRows.length === 0 && (
          <p className="mt-4 text-sm text-slate">
            Results not published for this study yet. Go to Admin -> Publish Journey Results.{" "}
            <Link className="underline" href={`/admin?study_id=${encodeURIComponent(studyId)}`}>
              Open Admin
            </Link>
          </p>
        )}
      </section>
    </main>
  );
}
