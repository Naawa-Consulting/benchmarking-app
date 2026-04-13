const LEGACY_API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || "";
const NEXT_API_BASE_URL = process.env.NEXT_PUBLIC_NEXT_API_BASE_URL || "";

function normalizeBase(base: string) {
  return base.replace(/\/+$/, "");
}

function isAbsoluteUrl(path: string) {
  return /^https?:\/\//i.test(path);
}

function useNextApiProxy(path: string) {
  return (
    path.startsWith("/analytics/") ||
    path.startsWith("/filters/") ||
    path === "/network" ||
    path.startsWith("/network?")
  );
}

function buildUrl(path: string) {
  if (isAbsoluteUrl(path)) return path;
  const base = useNextApiProxy(path) ? normalizeBase(NEXT_API_BASE_URL) : normalizeBase(LEGACY_API_BASE_URL);
  return `${base}${path}`;
}

export type ApiResult<T = unknown> = {
  ok: boolean;
  status: number;
  data: T | null;
  error: string | null;
  url: string;
};

export function getApiBaseUrl() {
  return LEGACY_API_BASE_URL;
}

async function request(path: string, options?: RequestInit) {
  const response = await fetch(buildUrl(path), {
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

export function fetchJourneyTableMultiDetailed(
  studies: string | null,
  limitMode: "top10" | "top25" | "all"
) {
  const params = new URLSearchParams({ limit_mode: limitMode });
  if (studies) {
    params.set("studies", studies);
  }
  return requestDetailed(`/analytics/journey/table_multi?${params.toString()}`);
}

export function fetchTouchpointsTableMultiDetailed(
  studies: string | null,
  limitMode: "top10" | "top25" | "all"
) {
  const params = new URLSearchParams({ limit_mode: limitMode });
  if (studies) {
    params.set("studies", studies);
  }
  return requestDetailed(`/analytics/touchpoints/table_multi?${params.toString()}`);
}

export function postJourneyTableMultiDetailed(
  payload: unknown,
  limitMode: "top10" | "top25" | "all",
  sortBy = "brand_awareness",
  sortDir: "asc" | "desc" = "desc",
  options?: {
    includeGlobalBenchmark?: boolean;
    signal?: AbortSignal;
    responseMode?: "benchmark_global" | "benchmark_selection" | "full";
  }
) {
  const params = new URLSearchParams({
    limit_mode: limitMode,
    sort_by: sortBy,
    sort_dir: sortDir,
  });
  if (options?.includeGlobalBenchmark) {
    params.set("include_global_benchmark", "1");
  }
  if (options?.responseMode) {
    params.set("response_mode", options.responseMode);
  }
  return requestDetailed(`/analytics/journey/table_multi?${params.toString()}`, {
    method: "POST",
    body: JSON.stringify(payload),
    signal: options?.signal,
  });
}

export function postTouchpointsTableMultiDetailed(
  payload: unknown,
  limitMode: "top10" | "top25" | "all",
  sortBy = "recall",
  sortDir: "asc" | "desc" = "desc",
  options?: { signal?: AbortSignal }
) {
  const params = new URLSearchParams({
    limit_mode: limitMode,
    sort_by: sortBy,
    sort_dir: sortDir,
  });
  return requestDetailed(`/analytics/touchpoints/table_multi?${params.toString()}`, {
    method: "POST",
    body: JSON.stringify(payload),
    signal: options?.signal,
  });
}

export function postTrackingSeriesDetailed(payload: unknown, options?: { signal?: AbortSignal }) {
  return requestDetailed(`/analytics/tracking/series`, {
    method: "POST",
    body: JSON.stringify(payload),
    signal: options?.signal,
  });
}

async function requestDetailed(path: string, options?: RequestInit): Promise<ApiResult> {
  const url = buildUrl(path);
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


async function requestDetailedLocal(path: string, options?: RequestInit): Promise<ApiResult> {
  const url = path;
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

export function getDataStudiesDetailed() {
  return requestDetailedLocal("/api/data/studies");
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
  return `${normalizeBase(LEGACY_API_BASE_URL)}/mapping/template?study_id=${encodeURIComponent(studyId)}`;
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

export function getMarketTaxonomyDetailed() {
  return requestDetailed("/taxonomy/market");
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

export function getStudyConfigDetailed(studyId: string) {
  const params = new URLSearchParams({ study_id: studyId });
  return requestDetailed(`/study-config?${params.toString()}`);
}

export function saveStudyConfigDetailed(studyId: string, payload: unknown) {
  const params = new URLSearchParams({ study_id: studyId });
  return requestDetailed(`/study-config?${params.toString()}`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getStudyVariablesDetailed(studyId: string) {
  const params = new URLSearchParams({ study_id: studyId });
  return requestDetailed(`/study/variables?${params.toString()}`);
}

export function getStudyBasePreviewDetailed(studyId: string, limit = 5) {
  const params = new URLSearchParams({ study_id: studyId, n: String(limit) });
  return requestDetailed(`/study/base/preview?${params.toString()}`);
}

export function rebuildBaseDetailed(studyId: string, force = false) {
  const params = new URLSearchParams({ study_id: studyId, force: force ? "1" : "0" });
  return requestDetailed(`/pipeline/base/rebuild?${params.toString()}`, {
    method: "POST",
  });
}

export function getDemographicsSchemaDetailed(studyId: string) {
  const params = new URLSearchParams({ study_id: studyId });
  return requestDetailed(`/demographics/schema?${params.toString()}`);
}

export function getDemographicsConfigDetailed(studyId: string) {
  const params = new URLSearchParams({ study_id: studyId });
  return requestDetailed(`/demographics/config?${params.toString()}`);
}

export function saveDemographicsConfigDetailed(studyId: string, payload: unknown) {
  const params = new URLSearchParams({ study_id: studyId });
  return requestDetailed(`/demographics/config?${params.toString()}`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getDemographicsValueLabelsDetailed(studyId: string, varCode: string) {
  const params = new URLSearchParams({ study_id: studyId, var_code: varCode });
  return requestDetailed(`/demographics/value-labels?${params.toString()}`);
}

export function getDemographicsGenderMapPreviewDetailed(studyId: string, varCode: string, limit = 50) {
  const params = new URLSearchParams({ study_id: studyId, var_code: varCode, limit: String(limit) });
  return requestDetailed(`/demographics/gender-map-preview?${params.toString()}`);
}

export function getDemographicsPreviewDetailed(studyId: string, varCode: string, limit = 5) {
  const params = new URLSearchParams({ study_id: studyId, var_code: varCode, n: String(limit) });
  return requestDetailed(`/demographics/preview?${params.toString()}`);
}

export function getDemographicsDatePreview(
  studyId: string,
  mode: "none" | "var" | "constant",
  varCode?: string | null,
  constant?: string | null,
  limit = 10
) {
  const params = new URLSearchParams({ study_id: studyId, mode, n: String(limit) });
  if (varCode) params.set("var_code", varCode);
  if (constant) params.set("constant", constant);
  return requestDetailed(`/demographics/date/preview?${params.toString()}`);
}

export function getQuestionMapDetailed(
  studyId: string,
  q: string | null,
  unmappedOnly: boolean,
  limit = 500,
  offset = 0
) {
  const params = new URLSearchParams({ study_id: studyId, limit: String(limit), offset: String(offset) });
  if (q) params.set("q", q);
  if (unmappedOnly) params.set("unmapped_only", "1");
  return requestDetailed(`/question-map?${params.toString()}`);
}

export function bulkUpdateQuestionMapDetailed(studyId: string, payload: unknown) {
  const params = new URLSearchParams({ study_id: studyId });
  return requestDetailed(`/question-map/bulk-update?${params.toString()}`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function applyQuestionMapSuggestionsDetailed(studyId: string, payload: unknown) {
  const params = new URLSearchParams({ study_id: studyId });
  return requestDetailed(`/question-map/apply-suggestions?${params.toString()}`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function getQuestionMapValuePreviewDetailed(
  studyId: string,
  varCode: string,
  mode: "labels" | "samples" = "labels",
  limit = 12
) {
  const params = new URLSearchParams({
    study_id: studyId,
    var_code: varCode,
    mode,
    n: String(limit),
  });
  return requestDetailed(`/question-map/value-preview?${params.toString()}`);
}

export function getFilterStudyOptionsDetailed() {
  return requestDetailed("/filters/options/studies");
}

export function getFilterTaxonomyOptionsDetailed() {
  return requestDetailed("/filters/options/taxonomy?view=market");
}

export function getFilterTaxonomyOptionsByViewDetailed(view: "market" | "standard") {
  return requestDetailed(`/filters/options/taxonomy?view=${encodeURIComponent(view)}`);
}

export function getFilterDemographicsOptionsDetailed(studyIds: string[] | null) {
  const params = new URLSearchParams();
  if (studyIds && studyIds.length > 0) {
    params.set("study_ids", studyIds.join(","));
  }
  const suffix = params.toString();
  return requestDetailed(`/filters/options/demographics${suffix ? `?${suffix}` : ""}`);
}

export function getFilterDateOptionsDetailed(studyIds: string[] | null) {
  const params = new URLSearchParams();
  if (studyIds && studyIds.length > 0) {
    params.set("study_ids", studyIds.join(","));
  }
  const suffix = params.toString();
  return requestDetailed(`/filters/options/date${suffix ? `?${suffix}` : ""}`);
}

export function getAdminUsersDetailed() {
  return requestDetailedLocal("/api/admin/users");
}

export function createAdminUserDetailed(payload: { email: string; role: "owner" | "admin" | "analyst" | "viewer" }) {
  return requestDetailedLocal("/api/admin/users", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export function patchAdminUserRoleDetailed(
  userId: string,
  payload: { role?: "owner" | "admin" | "analyst" | "viewer"; disabled?: boolean }
) {
  return requestDetailedLocal(`/api/admin/users/${encodeURIComponent(userId)}`, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
}

export function deleteAdminUserDetailed(userId: string) {
  return requestDetailedLocal(`/api/admin/users/${encodeURIComponent(userId)}`, {
    method: "DELETE",
  });
}

export function getAdminUserAccessDetailed(userId: string) {
  return requestDetailedLocal(`/api/admin/users/${encodeURIComponent(userId)}/access`);
}

export function patchAdminUserAccessDetailed(
  userId: string,
  payload: {
    can_toggle_brands: boolean;
    scopes: { market_sector: string[]; market_subsector: string[]; market_category: string[] };
  }
) {
  return requestDetailedLocal(`/api/admin/users/${encodeURIComponent(userId)}/access`, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
}

export function getAgentConversationsDetailed() {
  return requestDetailedLocal("/api/agent/conversations");
}

export function createAgentConversationDetailed(payload?: { title?: string }) {
  return requestDetailedLocal("/api/agent/conversations", {
    method: "POST",
    body: JSON.stringify(payload || {}),
  });
}

export function deleteAgentConversationDetailed(conversationId: string) {
  return requestDetailedLocal(`/api/agent/conversations/${encodeURIComponent(conversationId)}`, {
    method: "DELETE",
  });
}

export function getAgentMessagesDetailed(conversationId: string) {
  return requestDetailedLocal(`/api/agent/conversations/${encodeURIComponent(conversationId)}/messages`);
}

export function postAgentMessageDetailed(
  conversationId: string,
  payload: {
    message: string;
    context?: {
      taxonomy_view?: "market" | "standard";
      sector?: string | null;
      subsector?: string | null;
      category?: string | null;
      years?: string[] | null;
      gender?: string[] | null;
      nse?: string[] | null;
      age_min?: number | null;
      age_max?: number | null;
      brands?: string[] | null;
    };
  }
) {
  return requestDetailedLocal(`/api/agent/conversations/${encodeURIComponent(conversationId)}/messages`, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}
