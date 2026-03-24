"use client";

import { useEffect, useMemo, useState } from "react";

type UploadedFileRow = {
  id: string;
  filename: string;
  status: string;
  created_at: string;
  size_bytes: number;
};

type StudyRow = {
  id: string;
  name: string;
  status?: string;
  local_ready?: boolean;
  mapped?: boolean;
  curated_ready?: boolean;
  published_to_supabase?: boolean;
};

type PushHistoryRow = {
  id: string;
  status: string;
  created_at: string;
  finished_at: string | null;
  error_message: string | null;
  payload?: {
    study_ids?: string[];
    journey_rows?: number;
    touchpoint_rows?: number;
    study_catalog_rows?: number;
  };
};

type Role = "owner" | "admin" | "analyst" | "viewer";

type DataOpsPanelProps = {
  canMutate: boolean;
  role: Role;
  selectedStudyId: string;
};

function formatBytes(size: number | null | undefined) {
  if (!size || size < 1024) return `${size || 0} B`;
  if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
  return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}

function canPushOrDelete(role: Role) {
  return role === "owner" || role === "admin";
}

export default function DataOpsPanel({ canMutate, role, selectedStudyId }: DataOpsPanelProps) {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [files, setFiles] = useState<UploadedFileRow[]>([]);
  const [studies, setStudies] = useState<StudyRow[]>([]);
  const [publishHistory, setPublishHistory] = useState<PushHistoryRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [pushBusy, setPushBusy] = useState(false);
  const [deleteBusy, setDeleteBusy] = useState(false);
  const canPublishToSupabase = canPushOrDelete(role);
  const selectedStudy = useMemo(
    () => studies.find((study) => study.id === selectedStudyId) || null,
    [studies, selectedStudyId]
  );

  async function loadData() {
    setLoading(true);
    try {
      const [filesRes, studiesRes, historyRes] = await Promise.all([
        fetch("/api/data/files?limit=30", { cache: "no-store" }),
        fetch("/api/data/studies", { cache: "no-store" }),
        fetch("/api/data/publish/history?limit=20", { cache: "no-store" }),
      ]);
      const filesJson = await filesRes.json();
      const studiesJson = await studiesRes.json();
      const historyJson = await historyRes.json();
      setFiles(Array.isArray(filesJson?.items) ? filesJson.items : []);
      setStudies(Array.isArray(studiesJson?.studies) ? studiesJson.studies : []);
      setPublishHistory(Array.isArray(historyJson?.items) ? historyJson.items : []);
    } catch {
      setFiles([]);
      setStudies([]);
      setPublishHistory([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData();
  }, []);

  async function handleUpload() {
    setError(null);
    setMessage(null);
    if (!canMutate) {
      setError("You do not have permission to upload files.");
      return;
    }
    if (!selectedFile) {
      setError("Choose a .sav file first.");
      return;
    }
    if (!selectedFile.name.toLowerCase().endsWith(".sav")) {
      setError("Only .sav files are supported in this phase.");
      return;
    }

    setUploading(true);
    try {
      const form = new FormData();
      form.append("file", selectedFile);
      const response = await fetch("/api/data/upload", {
        method: "POST",
        body: form,
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        setError(data?.detail || "Upload failed.");
      } else {
        setMessage("File uploaded. Running pipeline job automatically...");
        setSelectedFile(null);
        await loadData();
        const jobId = typeof data?.job_id === "string" ? data.job_id : "";
        if (jobId) {
          await runJob(jobId);
        } else {
          setMessage("File uploaded to local pipeline. Ingestion job created as pending.");
        }
        window.dispatchEvent(new Event("bbs:data-studies-changed"));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Upload failed.");
    } finally {
      setUploading(false);
    }
  }

  async function runJob(jobId: string) {
    if (!canMutate) return;
    setError(null);
    setMessage(null);
    try {
      const response = await fetch(`/api/data/jobs/${jobId}/run`, { method: "POST" });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        setError(data?.detail || "Failed to run job.");
      } else {
        setMessage("Job executed locally and status persisted.");
        await loadData();
        window.dispatchEvent(new Event("bbs:data-studies-changed"));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to run job.");
    }
  }

  async function pushStudyToSupabase() {
    if (!selectedStudyId || !canPublishToSupabase) return;
    setPushBusy(true);
    setError(null);
    setMessage(null);
    try {
      const response = await fetch("/api/data/push", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ study_id: selectedStudyId }),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        setError(data?.detail || "Push to Supabase failed.");
      } else {
        setMessage("Snapshot pushed to Supabase.");
        await loadData();
        window.dispatchEvent(new Event("bbs:data-studies-changed"));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Push to Supabase failed.");
    } finally {
      setPushBusy(false);
    }
  }

  async function deleteStudy() {
    if (!selectedStudyId || !canPublishToSupabase) return;
    const confirmed = window.confirm(
      `Delete study '${selectedStudyId}' from local artifacts and Supabase snapshots?`
    );
    if (!confirmed) return;

    setDeleteBusy(true);
    setError(null);
    setMessage(null);
    try {
      const response = await fetch(`/api/data/studies/${encodeURIComponent(selectedStudyId)}/delete`, {
        method: "POST",
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        setError(data?.detail || "Delete study failed.");
      } else {
        setMessage("Study deleted from local artifacts and Supabase snapshot tables.");
        await loadData();
        window.dispatchEvent(new Event("bbs:data-studies-changed"));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Delete study failed.");
    } finally {
      setDeleteBusy(false);
    }
  }

  return (
    <section className="main-surface rounded-3xl p-6 space-y-5">
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-xl font-semibold">Data Operations (Local-First)</h3>
          <p className="text-xs text-slate">Upload SAV y empieza clasificación/mapeo. Push a Supabase es manual cuando esté listo.</p>
        </div>
        <button
          type="button"
          onClick={loadData}
          className="rounded-full border border-ink/10 px-3 py-1.5 text-xs font-medium"
          disabled={loading}
        >
          {loading ? "Refreshing..." : "Refresh"}
        </button>
      </div>

      <div className="rounded-2xl border border-ink/10 bg-white p-4 space-y-3">
        <p className="text-xs font-medium text-slate">Selected Study Status</p>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="rounded-full border border-ink/10 px-3 py-1">Study: {selectedStudyId || "-"}</span>
          <span className="rounded-full border border-ink/10 px-3 py-1">Local Ready: {selectedStudy?.local_ready ? "yes" : "no"}</span>
          <span className="rounded-full border border-ink/10 px-3 py-1">Mapped: {selectedStudy?.mapped ? "yes" : "no"}</span>
          <span className="rounded-full border border-ink/10 px-3 py-1">Curated: {selectedStudy?.curated_ready ? "yes" : "no"}</span>
          <span className="rounded-full border border-ink/10 px-3 py-1">Published to Supabase: {selectedStudy?.published_to_supabase ? "yes" : "no"}</span>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={pushStudyToSupabase}
            disabled={!selectedStudyId || !canPublishToSupabase || pushBusy}
            className="rounded-full bg-emerald-600 px-4 py-2 text-xs font-medium text-white disabled:opacity-60"
          >
            {pushBusy ? "Pushing..." : "Push to Supabase"}
          </button>
          <button
            type="button"
            onClick={deleteStudy}
            disabled={!selectedStudyId || !canPublishToSupabase || deleteBusy}
            className="rounded-full border border-rose-300 px-4 py-2 text-xs font-medium text-rose-700 disabled:opacity-60"
          >
            {deleteBusy ? "Deleting..." : "Delete Study"}
          </button>
          {!canPublishToSupabase ? (
            <p className="text-xs text-amber-700">Only owner/admin can push or delete.</p>
          ) : null}
        </div>
      </div>

      <div className="rounded-2xl border border-ink/10 bg-white p-4 space-y-3">
        <p className="text-xs font-medium text-slate">Upload SAV</p>
        <div className="flex flex-wrap items-center gap-2">
          <input
            type="file"
            accept=".sav"
            onChange={(event) => setSelectedFile(event.target.files?.[0] || null)}
            className="text-xs"
            disabled={!canMutate || uploading}
          />
          <button
            type="button"
            onClick={handleUpload}
            disabled={!canMutate || uploading || !selectedFile}
            className="rounded-full bg-emerald-600 px-4 py-2 text-xs font-medium text-white disabled:opacity-60"
          >
            {uploading ? "Uploading..." : "Upload"}
          </button>
        </div>
        {!canMutate ? <p className="text-xs text-amber-700">Read-only mode: your role cannot mutate data.</p> : null}
        {selectedFile ? (
          <p className="text-xs text-slate">
            Selected: <span className="font-medium text-ink">{selectedFile.name}</span> ({formatBytes(selectedFile.size)})
          </p>
        ) : null}
        {message ? <p className="text-xs text-emerald-700">{message}</p> : null}
        {error ? <p className="text-xs text-rose-600">{error}</p> : null}
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <div className="rounded-2xl border border-ink/10 bg-white p-4">
          <h4 className="text-sm font-semibold">Recent Files</h4>
          <div className="mt-3 max-h-64 overflow-auto text-xs">
            {files.length === 0 ? (
              <p className="text-slate">No files yet.</p>
            ) : (
              <table className="w-full border-collapse">
                <thead>
                  <tr className="border-b border-ink/10 text-left text-slate">
                    <th className="py-1 pr-2">Filename</th>
                    <th className="py-1 pr-2">Size</th>
                    <th className="py-1 pr-2">Status</th>
                    <th className="py-1">Created</th>
                  </tr>
                </thead>
                <tbody>
                  {files.map((row) => (
                    <tr key={row.id} className="border-b border-ink/5">
                      <td className="py-1 pr-2">{row.filename}</td>
                      <td className="py-1 pr-2">{formatBytes(row.size_bytes)}</td>
                      <td className="py-1 pr-2">{row.status}</td>
                      <td className="py-1">{new Date(row.created_at).toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>

      <div className="rounded-2xl border border-ink/10 bg-white p-4">
        <h4 className="text-sm font-semibold">Publish History</h4>
        <div className="mt-3 max-h-64 overflow-auto text-xs">
          {publishHistory.length === 0 ? (
            <p className="text-slate">No push history yet.</p>
          ) : (
            <table className="w-full border-collapse">
              <thead>
                <tr className="border-b border-ink/10 text-left text-slate">
                  <th className="py-1 pr-2">Status</th>
                  <th className="py-1 pr-2">Studies</th>
                  <th className="py-1 pr-2">Rows</th>
                  <th className="py-1 pr-2">Created</th>
                  <th className="py-1">Error</th>
                </tr>
              </thead>
              <tbody>
                {publishHistory.map((row) => (
                  <tr key={row.id} className="border-b border-ink/5">
                    <td className="py-1 pr-2">{row.status}</td>
                    <td className="py-1 pr-2">{Array.isArray(row.payload?.study_ids) ? row.payload?.study_ids.join(", ") : "-"}</td>
                    <td className="py-1 pr-2">
                      J:{row.payload?.journey_rows ?? 0} T:{row.payload?.touchpoint_rows ?? 0}
                    </td>
                    <td className="py-1 pr-2">{new Date(row.created_at).toLocaleString()}</td>
                    <td className="py-1">{row.error_message || "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </section>
  );
}
