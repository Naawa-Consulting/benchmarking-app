"use client";

import { useEffect, useMemo, useState } from "react";

type UploadedFileRow = {
  id: string;
  filename: string;
  status: string;
  created_at: string;
  size_bytes: number;
};

type JobRow = {
  id: string;
  file_id: string | null;
  status: string;
  operation: string;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  error_message: string | null;
};

type DataVersionRow = {
  id: string;
  label: string;
  status: string;
  source_job_id: string | null;
  created_at: string;
  published_at: string | null;
  notes: string | null;
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
  const [jobs, setJobs] = useState<JobRow[]>([]);
  const [files, setFiles] = useState<UploadedFileRow[]>([]);
  const [versions, setVersions] = useState<DataVersionRow[]>([]);
  const [studies, setStudies] = useState<StudyRow[]>([]);
  const [publishHistory, setPublishHistory] = useState<PushHistoryRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [busyJobs, setBusyJobs] = useState<Set<string>>(new Set());
  const [busyVersions, setBusyVersions] = useState<Set<string>>(new Set());

  const [versionJobId, setVersionJobId] = useState<string>("");
  const [versionLabel, setVersionLabel] = useState<string>("");
  const [versionNotes, setVersionNotes] = useState<string>("");
  const [creatingVersion, setCreatingVersion] = useState(false);
  const [pushBusy, setPushBusy] = useState(false);
  const [deleteBusy, setDeleteBusy] = useState(false);

  const jobsByFile = useMemo(() => {
    const map = new Map<string, JobRow[]>();
    for (const job of jobs) {
      if (!job.file_id) continue;
      if (!map.has(job.file_id)) map.set(job.file_id, []);
      map.get(job.file_id)!.push(job);
    }
    return map;
  }, [jobs]);

  const successfulJobs = useMemo(() => jobs.filter((job) => job.status === "success"), [jobs]);
  const canPublishToSupabase = canPushOrDelete(role);
  const selectedStudy = useMemo(
    () => studies.find((study) => study.id === selectedStudyId) || null,
    [studies, selectedStudyId]
  );

  async function loadData() {
    setLoading(true);
    try {
      const [jobsRes, filesRes, versionsRes, studiesRes, historyRes] = await Promise.all([
        fetch("/api/data/jobs?limit=30", { cache: "no-store" }),
        fetch("/api/data/files?limit=30", { cache: "no-store" }),
        fetch("/api/data/versions?limit=30", { cache: "no-store" }),
        fetch("/api/data/studies", { cache: "no-store" }),
        fetch("/api/data/publish/history?limit=20", { cache: "no-store" }),
      ]);
      const jobsJson = await jobsRes.json();
      const filesJson = await filesRes.json();
      const versionsJson = await versionsRes.json();
      const studiesJson = await studiesRes.json();
      const historyJson = await historyRes.json();
      setJobs(Array.isArray(jobsJson?.items) ? jobsJson.items : []);
      setFiles(Array.isArray(filesJson?.items) ? filesJson.items : []);
      setVersions(Array.isArray(versionsJson?.items) ? versionsJson.items : []);
      setStudies(Array.isArray(studiesJson?.studies) ? studiesJson.studies : []);
      setPublishHistory(Array.isArray(historyJson?.items) ? historyJson.items : []);
    } catch {
      setJobs([]);
      setFiles([]);
      setVersions([]);
      setStudies([]);
      setPublishHistory([]);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    loadData();
  }, []);

  useEffect(() => {
    if (!versionJobId && successfulJobs.length > 0) {
      setVersionJobId(successfulJobs[0].id);
    }
  }, [successfulJobs, versionJobId]);

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
        setMessage("File uploaded to local pipeline. Ingestion job created as pending.");
        setSelectedFile(null);
        await loadData();
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
    setBusyJobs((prev) => new Set(prev).add(jobId));
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
    } finally {
      setBusyJobs((prev) => {
        const next = new Set(prev);
        next.delete(jobId);
        return next;
      });
    }
  }

  async function createVersion() {
    if (!canMutate) return;
    setError(null);
    setMessage(null);
    if (!versionLabel.trim()) {
      setError("Version label is required.");
      return;
    }

    setCreatingVersion(true);
    try {
      const response = await fetch("/api/data/versions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          source_job_id: versionJobId || null,
          label: versionLabel.trim(),
          notes: versionNotes.trim() || null,
        }),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        setError(data?.detail || "Failed to create data version.");
      } else {
        setMessage("Data version created as draft.");
        setVersionLabel("");
        setVersionNotes("");
        await loadData();
        window.dispatchEvent(new Event("bbs:data-studies-changed"));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create data version.");
    } finally {
      setCreatingVersion(false);
    }
  }

  async function publishVersion(versionId: string) {
    if (!canMutate) return;
    setError(null);
    setMessage(null);
    setBusyVersions((prev) => new Set(prev).add(versionId));
    try {
      const response = await fetch(`/api/data/versions/${versionId}/publish`, { method: "POST" });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        setError(data?.detail || "Failed to publish version.");
      } else {
        setMessage("Data version published.");
        await loadData();
        window.dispatchEvent(new Event("bbs:data-studies-changed"));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to publish version.");
    } finally {
      setBusyVersions((prev) => {
        const next = new Set(prev);
        next.delete(versionId);
        return next;
      });
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
          <p className="text-xs text-slate">Run ingestion/mapping locally, then push snapshots to Supabase manually.</p>
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
                    <th className="py-1">Job</th>
                  </tr>
                </thead>
                <tbody>
                  {files.map((row) => (
                    <tr key={row.id} className="border-b border-ink/5">
                      <td className="py-1 pr-2">{row.filename}</td>
                      <td className="py-1 pr-2">{formatBytes(row.size_bytes)}</td>
                      <td className="py-1 pr-2">{row.status}</td>
                      <td className="py-1">{jobsByFile.get(row.id)?.[0]?.status || "-"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        <div className="rounded-2xl border border-ink/10 bg-white p-4">
          <h4 className="text-sm font-semibold">Recent Jobs</h4>
          <div className="mt-3 max-h-64 overflow-auto text-xs">
            {jobs.length === 0 ? (
              <p className="text-slate">No jobs yet.</p>
            ) : (
              <table className="w-full border-collapse">
                <thead>
                  <tr className="border-b border-ink/10 text-left text-slate">
                    <th className="py-1 pr-2">Status</th>
                    <th className="py-1 pr-2">Operation</th>
                    <th className="py-1 pr-2">Created</th>
                    <th className="py-1 pr-2">Error</th>
                    <th className="py-1">Action</th>
                  </tr>
                </thead>
                <tbody>
                  {jobs.map((row) => {
                    const canRun = canMutate && (row.status === "pending" || row.status === "error");
                    const isBusy = busyJobs.has(row.id);
                    return (
                      <tr key={row.id} className="border-b border-ink/5">
                        <td className="py-1 pr-2">{row.status}</td>
                        <td className="py-1 pr-2">{row.operation}</td>
                        <td className="py-1 pr-2">{new Date(row.created_at).toLocaleString()}</td>
                        <td className="py-1 pr-2">{row.error_message || "-"}</td>
                        <td className="py-1">
                          {canRun ? (
                            <button
                              type="button"
                              onClick={() => runJob(row.id)}
                              disabled={isBusy}
                              className="rounded-full border border-ink/10 px-3 py-1 text-[11px] font-medium"
                            >
                              {isBusy ? "Running..." : "Run"}
                            </button>
                          ) : (
                            "-"
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>

      <div className="rounded-2xl border border-ink/10 bg-white p-4 space-y-3">
        <h4 className="text-sm font-semibold">Create Data Version</h4>
        <div className="grid gap-3 md:grid-cols-3">
          <div>
            <p className="text-xs text-slate">Source Job (success)</p>
            <select
              value={versionJobId}
              onChange={(event) => setVersionJobId(event.target.value)}
              disabled={!canMutate || creatingVersion}
              className="mt-1 w-full rounded-xl border border-ink/10 px-3 py-2 text-xs"
            >
              <option value="">No source job</option>
              {successfulJobs.map((job) => (
                <option key={job.id} value={job.id}>
                  {job.id.slice(0, 8)} - {job.operation}
                </option>
              ))}
            </select>
          </div>
          <div>
            <p className="text-xs text-slate">Label</p>
            <input
              value={versionLabel}
              onChange={(event) => setVersionLabel(event.target.value)}
              disabled={!canMutate || creatingVersion}
              placeholder="v2026.03.23"
              className="mt-1 w-full rounded-xl border border-ink/10 px-3 py-2 text-xs"
            />
          </div>
          <div>
            <p className="text-xs text-slate">Notes</p>
            <input
              value={versionNotes}
              onChange={(event) => setVersionNotes(event.target.value)}
              disabled={!canMutate || creatingVersion}
              placeholder="Optional"
              className="mt-1 w-full rounded-xl border border-ink/10 px-3 py-2 text-xs"
            />
          </div>
        </div>
        <button
          type="button"
          onClick={createVersion}
          disabled={!canMutate || creatingVersion}
          className="rounded-full bg-emerald-600 px-4 py-2 text-xs font-medium text-white disabled:opacity-60"
        >
          {creatingVersion ? "Creating..." : "Create Draft Version"}
        </button>
      </div>

      <div className="rounded-2xl border border-ink/10 bg-white p-4">
        <h4 className="text-sm font-semibold">Data Versions</h4>
        <div className="mt-3 max-h-64 overflow-auto text-xs">
          {versions.length === 0 ? (
            <p className="text-slate">No versions yet.</p>
          ) : (
            <table className="w-full border-collapse">
              <thead>
                <tr className="border-b border-ink/10 text-left text-slate">
                  <th className="py-1 pr-2">Label</th>
                  <th className="py-1 pr-2">Status</th>
                  <th className="py-1 pr-2">Source Job</th>
                  <th className="py-1 pr-2">Created</th>
                  <th className="py-1 pr-2">Published</th>
                  <th className="py-1">Action</th>
                </tr>
              </thead>
              <tbody>
                {versions.map((row) => {
                  const isBusy = busyVersions.has(row.id);
                  const canPublish = canMutate && row.status !== "published";
                  return (
                    <tr key={row.id} className="border-b border-ink/5">
                      <td className="py-1 pr-2">{row.label}</td>
                      <td className="py-1 pr-2">{row.status}</td>
                      <td className="py-1 pr-2">{row.source_job_id ? row.source_job_id.slice(0, 8) : "-"}</td>
                      <td className="py-1 pr-2">{new Date(row.created_at).toLocaleString()}</td>
                      <td className="py-1 pr-2">{row.published_at ? new Date(row.published_at).toLocaleString() : "-"}</td>
                      <td className="py-1">
                        {canPublish ? (
                          <button
                            type="button"
                            onClick={() => publishVersion(row.id)}
                            disabled={isBusy}
                            className="rounded-full border border-ink/10 px-3 py-1 text-[11px] font-medium"
                          >
                            {isBusy ? "Publishing..." : "Publish"}
                          </button>
                        ) : (
                          "-"
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
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
