const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8000";

export type ApiResult<T = unknown> = {
  ok: boolean;
  status: number;
  data: T | null;
  error: string | null;
  url: string;
};

export function getApiBaseUrl() {
  return API_BASE_URL;
}

async function request(path: string, options?: RequestInit) {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }

  return response.json();
}

export function fetchStudies() {
  return request("/studies");
}

export function fetchJourney(studyId: string) {
  return request(`/analytics/journey?study_id=${encodeURIComponent(studyId)}`);
}

export function fetchJourneyTableDetailed(studyId: string) {
  return requestDetailed(`/analytics/journey/table?study_id=${encodeURIComponent(studyId)}`);
}

export function seedDemo() {
  return request("/demo/seed", { method: "POST" });
}

async function requestDetailed(path: string, options?: RequestInit): Promise<ApiResult> {
  const url = `${API_BASE_URL}${path}`;
  try {
    const response = await fetch(url, {
      headers: { "Content-Type": "application/json" },
      ...options,
    });

    const text = await response.text();
    let data: unknown = null;
    if (text) {
      try {
        data = JSON.parse(text);
      } catch {
        data = text;
      }
    }

    return {
      ok: response.ok,
      status: response.status,
      data,
      error: response.ok ? null : response.statusText || "Request failed",
      url,
    };
  } catch (error) {
    return {
      ok: false,
      status: 0,
      data: null,
      error: error instanceof Error ? error.message : String(error),
      url,
    };
  }
}

export function pingHealthDetailed() {
  return requestDetailed("/health");
}

export function runIngestDetailed() {
  return requestDetailed("/ingest/run", { method: "POST" });
}

export function fetchStudiesDetailed() {
  return requestDetailed("/studies");
}

export function getStudiesDetailed(sync = false) {
  const suffix = sync ? "?sync=1" : "";
  return requestDetailed(`/studies${suffix}`);
}

export function fetchStudyPreviewDetailed(studyId: string) {
  return requestDetailed(`/studies/${encodeURIComponent(studyId)}/preview`);
}

export function suggestMappingDetailed(studyId: string) {
  return requestDetailed(`/mapping/suggest?study_id=${encodeURIComponent(studyId)}`);
}

export function loadMappingDetailed(studyId: string) {
  return requestDetailed(`/mapping?study_id=${encodeURIComponent(studyId)}`);
}

export function saveMappingDetailed(payload: unknown) {
  return requestDetailed("/mapping/save", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function buildJourneyMartDetailed(studyId: string) {
  return requestDetailed(`/marts/journey/build?study_id=${encodeURIComponent(studyId)}`, {
    method: "POST",
  });
}

export function getMappingTemplateUrl(studyId: string) {
  return `${API_BASE_URL}/mapping/template?study_id=${encodeURIComponent(studyId)}`;
}

export function getRulesDetailed() {
  return requestDetailed("/rules");
}

export function saveRulesDetailed(payload: unknown) {
  return requestDetailed("/rules", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function runRulesDetailed(studyId: string) {
  return requestDetailed(`/rules/run?study_id=${encodeURIComponent(studyId)}`, {
    method: "POST",
  });
}

export function coverageRulesDetailed(studyId: string) {
  return requestDetailed(`/rules/coverage?study_id=${encodeURIComponent(studyId)}`);
}

export function getQuestionsDetailed(studyId: string, includeStats = false, limit = 200) {
  const params = new URLSearchParams({ study_id: studyId });
  if (includeStats) {
    params.set("include_stats", "1");
    params.set("limit", String(limit));
  }
  return requestDetailed(`/questions?${params.toString()}`);
}

export function getStudyRuleScopeDetailed(studyId: string) {
  const params = new URLSearchParams({ study_id: studyId });
  return requestDetailed(`/rules/study?${params.toString()}`);
}

export function saveStudyRuleScopeDetailed(studyId: string, payload: unknown) {
  const params = new URLSearchParams({ study_id: studyId });
  return requestDetailed(`/rules/study?${params.toString()}`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getJourneyStatusDetailed(studyId: string) {
  return requestDetailed(`/pipeline/journey/status?study_id=${encodeURIComponent(studyId)}`);
}

export function ensureJourneyDetailed(studyId: string, force = false) {
  const params = new URLSearchParams({ study_id: studyId, sync_raw: "1", force: force ? "1" : "0" });
  return requestDetailed(`/pipeline/journey/ensure?${params.toString()}`, {
    method: "POST",
  });
}

export function getTaxonomyDetailed() {
  return requestDetailed("/taxonomy");
}

export function getStudyClassificationDetailed(studyId: string) {
  const params = new URLSearchParams({ study_id: studyId });
  return requestDetailed(`/taxonomy/study?${params.toString()}`);
}

export function saveStudyClassificationDetailed(studyId: string, payload: unknown) {
  const params = new URLSearchParams({ study_id: studyId });
  return requestDetailed(`/taxonomy/study?${params.toString()}`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}
