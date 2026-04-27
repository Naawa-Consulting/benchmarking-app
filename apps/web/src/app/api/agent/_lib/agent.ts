import { NextRequest, NextResponse } from "next/server";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { getRequestAuthz, type RequestAuthz } from "../../_lib/authz";
import { supabaseAdminPostgrest } from "../../_lib/supabase-admin";

export type AgentContext = {
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

type AgentClassifierResult = {
  in_scope?: boolean;
  needs_brand_level?: boolean;
  tool?: "journey_overview" | "network_overview" | "tracking_series";
  metric?: string | null;
  chart_type?: "bar" | "line" | "table" | null;
  filters?: {
    sector?: string | null;
    subsector?: string | null;
    category?: string | null;
    years?: string[] | null;
  };
};

type AgentResolvedFilters = NonNullable<AgentClassifierResult["filters"]>;

type ToolSummary =
  | {
      type: "tracking_series";
      entity_label: string;
      periods: unknown[];
      rows: unknown[];
    }
  | {
      type: string;
      rows: unknown[];
    };

type AgentChartSpec = {
  type: "bar" | "line" | "table";
  title?: string;
  x?: string[];
  series?: Array<{ name: string; data: number[] }>;
  columns?: string[];
  rows?: Array<Array<string | number | null>>;
  y_label?: string;
};

const DEFAULT_AGENT_GUIDE = `You are BBS Agent. Use only BBS data and respect user scope/permissions. Do not use external sources. Default to concise executive text with finding, implication, recommendation. Use chart/table only if explicitly requested. If out of scope, return a polite BBS-only message.`;
let agentGuideCache: string | null = null;
type AgentResponseLanguage = "es" | "en";

function pickLocalizedText(language: AgentResponseLanguage, es: string, en: string) {
  return language === "es" ? es : en;
}

export function isAgentFeatureEnabled() {
  const raw = (process.env.BBS_AGENT_ENABLED || process.env.NEXT_PUBLIC_BBS_AGENT_ENABLED || "on")
    .trim()
    .toLowerCase();
  return raw !== "off" && raw !== "false" && raw !== "0";
}

export async function requireAgentAccess(request: NextRequest) {
  const authz = await getRequestAuthz(request);
  const authRequired = (process.env.BBS_AUTH_MODE || "off").toLowerCase() === "supabase";
  if (authRequired && !authz.user_id) {
    return {
      denied: NextResponse.json({ detail: "Unauthorized." }, { status: 401 }),
      authz,
    };
  }
  if (!isAgentFeatureEnabled()) {
    return {
      denied: NextResponse.json({ detail: "Agent module is disabled." }, { status: 404 }),
      authz,
    };
  }
  if (!authz.is_agent_module_allowed) {
    return {
      denied: NextResponse.json({ detail: "Forbidden: insufficient permissions" }, { status: 403 }),
      authz,
    };
  }
  return { denied: null, authz };
}

function toIsoNow() {
  return new Date().toISOString();
}

export async function ensureConversationOwner(conversationId: string, authz: RequestAuthz) {
  const safeId = encodeURIComponent(conversationId);
  const result = await supabaseAdminPostgrest(
    `agent_conversations?select=id,user_id,title,created_at,updated_at,last_message_at,archived&id=eq.${safeId}&limit=1`
  );
  if (!result.response.ok) {
    return {
      ok: false as const,
      status: result.response.status || 500,
      detail: "Failed to load conversation.",
      error: result.data,
    };
  }
  const rows = Array.isArray(result.data) ? (result.data as Array<Record<string, unknown>>) : [];
  const row = rows[0];
  if (!row) {
    return { ok: false as const, status: 404, detail: "Conversation not found." };
  }
  const effectiveUserId = await resolveEffectiveAgentUserId(authz);
  if ((row.user_id as string | null) !== effectiveUserId) {
    return { ok: false as const, status: 403, detail: "Forbidden: insufficient permissions" };
  }
  return { ok: true as const, row };
}

export function buildConversationTitle(input: string) {
  const clean = input.replace(/\s+/g, " ").trim();
  if (!clean) return "New chat";
  return clean.length > 64 ? `${clean.slice(0, 61)}...` : clean;
}

export function getEffectiveAgentUserId(authz: RequestAuthz) {
  if (authz.user_id) return authz.user_id;
  const envFallback = (process.env.BBS_AGENT_LOCAL_USER_ID || "").trim();
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(envFallback)) {
    return envFallback;
  }
  return "00000000-0000-4000-8000-000000000001";
}

export async function resolveEffectiveAgentUserId(authz: RequestAuthz) {
  if (authz.user_id) return authz.user_id;

  const envFallback = (process.env.BBS_AGENT_LOCAL_USER_ID || "").trim();
  if (/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(envFallback)) {
    const exists = await supabaseAdminPostgrest(
      `user_roles?select=user_id&user_id=eq.${encodeURIComponent(envFallback)}&limit=1`
    );
    if (exists.response.ok && Array.isArray(exists.data) && exists.data.length > 0) {
      return envFallback;
    }
  }

  const ownerLookup = await supabaseAdminPostgrest("user_roles?select=user_id&role=eq.owner&limit=1");
  if (ownerLookup.response.ok && Array.isArray(ownerLookup.data) && ownerLookup.data.length > 0) {
    const userId = String((ownerLookup.data[0] as { user_id?: string }).user_id || "");
    if (/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(userId)) {
      return userId;
    }
  }

  return "00000000-0000-4000-8000-000000000001";
}

async function callOpenAIJson(messages: Array<{ role: "system" | "user" | "assistant"; content: string }>) {
  const apiKey = process.env.OPENAI_API_KEY || "";
  if (!apiKey) {
    throw new Error("Missing OPENAI_API_KEY.");
  }
  const model = (process.env.BBS_AGENT_MODEL || "gpt-4.1-mini").trim();
  const response = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model,
      temperature: 0.1,
      response_format: { type: "json_object" },
      messages,
    }),
    cache: "no-store",
  });
  const data = (await response.json().catch(() => null)) as
    | {
        choices?: Array<{ message?: { content?: string | null } }>;
        error?: { message?: string };
      }
    | null;
  if (!response.ok) {
    throw new Error(data?.error?.message || `OpenAI request failed (${response.status}).`);
  }
  const raw = data?.choices?.[0]?.message?.content || "{}";
  try {
    return JSON.parse(raw) as Record<string, unknown>;
  } catch {
    return {};
  }
}

async function loadAgentGuide() {
  if (agentGuideCache) return agentGuideCache;
  try {
    const filePath = path.join(process.cwd(), "src", "app", "api", "agent", "AGENTE.md");
    const content = await readFile(filePath, "utf8");
    const trimmed = content.trim();
    agentGuideCache = trimmed || DEFAULT_AGENT_GUIDE;
    return agentGuideCache;
  } catch {
    agentGuideCache = DEFAULT_AGENT_GUIDE;
    return agentGuideCache;
  }
}

async function callInternalApi(
  request: NextRequest,
  path: string,
  method: "GET" | "POST",
  payload?: Record<string, unknown>
) {
  const url = new URL(path, request.url);
  const response = await fetch(url, {
    method,
    headers: {
      "Content-Type": "application/json",
      cookie: request.headers.get("cookie") || "",
    },
    body: method === "POST" ? JSON.stringify(payload || {}) : undefined,
    cache: "no-store",
  });
  const data = await response.json().catch(() => null);
  return { ok: response.ok, status: response.status, data };
}

function sanitizeChartSpec(input: unknown): AgentChartSpec | null {
  if (!input || typeof input !== "object") return null;
  const raw = input as Record<string, unknown>;
  const type = typeof raw.type === "string" ? raw.type.toLowerCase() : "";
  if (type !== "bar" && type !== "line" && type !== "table") return null;
  if (type === "table") {
    const columns = Array.isArray(raw.columns)
      ? raw.columns.filter((item): item is string => typeof item === "string").slice(0, 10)
      : [];
    const rows = Array.isArray(raw.rows)
      ? raw.rows
          .filter((row): row is Array<unknown> => Array.isArray(row))
          .slice(0, 20)
          .map((row) =>
            row.slice(0, 10).map((cell) =>
              typeof cell === "string" || typeof cell === "number" || cell == null ? cell : String(cell)
            )
              .map((cell) => (cell === undefined ? null : cell))
          )
      : [];
    if (!columns.length || !rows.length) return null;
    return { type: "table", title: typeof raw.title === "string" ? raw.title : undefined, columns, rows };
  }

  const x = Array.isArray(raw.x) ? raw.x.filter((item): item is string => typeof item === "string").slice(0, 30) : [];
  const series = Array.isArray(raw.series)
    ? raw.series
        .filter((item): item is Record<string, unknown> => Boolean(item && typeof item === "object"))
        .slice(0, 8)
        .map((item) => ({
          name: typeof item.name === "string" ? item.name : "Series",
          data: Array.isArray(item.data)
            ? item.data
                .map((v) => (typeof v === "number" && Number.isFinite(v) ? Number(v.toFixed(2)) : null))
                .filter((v): v is number => v !== null)
                .slice(0, 30)
            : [],
        }))
        .filter((item) => item.data.length > 0)
    : [];

  if (!x.length || !series.length) return null;
  return {
    type,
    title: typeof raw.title === "string" ? raw.title : undefined,
    x,
    series,
    y_label: typeof raw.y_label === "string" ? raw.y_label : "%",
  };
}

function normalizeContext(context: AgentContext | null | undefined, authz: RequestAuthz): AgentContext {
  const years = Array.isArray(context?.years)
    ? context!.years!.filter((item): item is string => typeof item === "string" && /\d{4}/.test(item)).slice(0, 6)
    : null;
  const gender = Array.isArray(context?.gender)
    ? context!.gender!.filter((item): item is string => typeof item === "string").slice(0, 8)
    : null;
  const nse = Array.isArray(context?.nse)
    ? context!.nse!.filter((item): item is string => typeof item === "string").slice(0, 8)
    : null;
  const brandsAllowed = authz.can_toggle_brands;
  const brands = brandsAllowed
    ? Array.isArray(context?.brands)
      ? context!.brands!.filter((item): item is string => typeof item === "string").slice(0, 20)
      : null
    : null;
  return {
    taxonomy_view: "market",
    // Agent should not inherit screen taxonomy filters by default.
    // Filters are only applied when explicitly requested in the user message.
    sector: null,
    subsector: null,
    category: null,
    years: null,
    gender,
    nse,
    age_min: typeof context?.age_min === "number" ? context.age_min : null,
    age_max: typeof context?.age_max === "number" ? context.age_max : null,
    brands,
  };
}

function hasExplicitChartIntent(message: string) {
  const text = normalizeText(message);
  if (!text) return false;
  return (
    text.includes("grafica") ||
    text.includes("grafico") ||
    text.includes("chart") ||
    text.includes("plot") ||
    text.includes("tabla") ||
    text.includes("table")
  );
}

function isGreetingOnly(message: string) {
  const text = normalizeText(message);
  if (!text) return true;
  const greetings = ["hola", "hello", "hi", "buenas", "quien eres", "quien eres tu", "who are you"];
  return greetings.includes(text.replace(/\s+/g, " ").trim());
}

function getScopeAppliedMeta(authz: RequestAuthz, filters: AgentResolvedFilters | undefined) {
  return {
    has_scope_restrictions: authz.is_viewer,
    market_scope_counts: {
      sector: authz.effective_scopes.market_sector.length,
      subsector: authz.effective_scopes.market_subsector.length,
      category: authz.effective_scopes.market_category.length,
    },
    explicit_filters: filters || null,
  };
}

const SECTOR_ALIASES: Array<{ canonical: string; aliases: string[] }> = [
  { canonical: "Retail & Commerce", aliases: ["retail & commerce", "retail", "comercio", "commerce"] },
  { canonical: "Health & Wellness", aliases: ["health & wellness", "health", "wellness", "salud"] },
  { canonical: "Financial Services", aliases: ["financial services", "financial", "finance", "servicios financieros"] },
  { canonical: "Industry & Manufacturing", aliases: ["industry", "manufacturing", "industria", "industrial"] },
  { canonical: "Media & Entertainment", aliases: ["media", "entertainment", "entretenimiento"] },
  { canonical: "Food & Beverage", aliases: ["food", "beverage", "alimentos", "bebidas"] },
  { canonical: "Services", aliases: ["services", "servicios"] },
];

const SUBSECTOR_ALIASES: Array<{ canonical: string; aliases: string[] }> = [
  { canonical: "Mass Retail", aliases: ["mass retail", "autoservicios", "autoservicio", "discount stores"] },
  { canonical: "Fashion", aliases: ["fashion", "moda"] },
  { canonical: "Specialty Retail", aliases: ["specialty retail", "speciality retail", "retail especializado"] },
  { canonical: "Digital Commerce", aliases: ["digital commerce", "ecommerce", "e commerce", "comercio digital"] },
  { canonical: "Consumer Health", aliases: ["consumer health", "otc"] },
  { canonical: "Pharma Retail", aliases: ["pharma retail", "farmacias", "pharmacies"] },
  { canonical: "Healthcare Services", aliases: ["healthcare services", "servicios de salud"] },
  { canonical: "Banking & Insurance", aliases: ["banking", "insurance", "seguros", "banca"] },
  { canonical: "Experiences", aliases: ["experiences", "experiencia", "experiencias"] },
  { canonical: "Beverage", aliases: ["beverage", "bebidas"] },
  { canonical: "Home Improvement", aliases: ["home improvement", "construccion", "mejoras del hogar"] },
  { canonical: "Manufacturing", aliases: ["manufacturing", "manufactura"] },
  { canonical: "Consumer Services", aliases: ["consumer services", "servicios al consumidor"] },
];

const CATEGORY_ALIASES: Array<{ canonical: string; aliases: string[] }> = [
  { canonical: "Discount Retail", aliases: ["discount retail", "tiendas de descuento"] },
  { canonical: "Apparel Stores", aliases: ["apparel stores", "tiendas de ropa"] },
  { canonical: "Swimwear Brands", aliases: ["swimwear brands", "trajes de bano", "trajes de baño"] },
  { canonical: "Pharmacies", aliases: ["pharmacies", "farmacias"] },
  { canonical: "OTC Brands", aliases: ["otc brands", "marcas otc"] },
  { canonical: "Cinemas", aliases: ["cinemas", "cines", "cinema"] },
  { canonical: "Theme Parks", aliases: ["theme parks", "parques tematicos", "parques temáticos"] },
  { canonical: "Alcoholic Drinks", aliases: ["alcoholic drinks", "bebidas alcoholicas", "bebidas alcohólicas"] },
  { canonical: "Materials & Supplies", aliases: ["materials & supplies", "materiales"] },
  { canonical: "Auto Insurance", aliases: ["auto insurance", "seguro de auto"] },
  { canonical: "Home Insurance", aliases: ["home insurance", "seguro de hogar"] },
  { canonical: "Life Insurance", aliases: ["life insurance", "seguro de vida"] },
  { canonical: "Health Insurance", aliases: ["health insurance", "seguro de salud"] },
];

function containsAlias(normalizedMessage: string, alias: string) {
  const compactMessage = normalizeCompact(normalizedMessage);
  const normalizedAlias = normalizeText(alias);
  if (!normalizedAlias) return false;
  const compactAlias = normalizeCompact(alias);
  return normalizedMessage.includes(normalizedAlias) || compactMessage.includes(compactAlias);
}

function resolveCanonicalFromAliases(
  message: string,
  candidate: string | null | undefined,
  aliasMap: Array<{ canonical: string; aliases: string[] }>
) {
  const normalizedMessage = normalizeText(message);
  if (!normalizedMessage) return null;
  const normalizedCandidate = typeof candidate === "string" && candidate.trim() ? normalizeText(candidate) : null;

  for (const item of aliasMap) {
    const aliases = [item.canonical, ...item.aliases];
    const candidateMatches =
      normalizedCandidate &&
      aliases.some((alias) => normalizeText(alias) === normalizedCandidate || normalizeCompact(alias) === normalizeCompact(normalizedCandidate));
    const messageMatches = aliases.some((alias) => containsAlias(normalizedMessage, alias));
    if ((candidateMatches && messageMatches) || (!normalizedCandidate && messageMatches)) {
      return item.canonical;
    }
  }

  if (!normalizedCandidate) return null;
  // If message explicitly includes the exact candidate text, allow it.
  if (containsAlias(normalizedMessage, normalizedCandidate)) {
    return candidate?.trim() || null;
  }
  return null;
}

function hasExplicitTaxonomyIntent(message: string) {
  const text = normalizeText(message);
  if (!text) return false;
  const cues = [
    "en ",
    "para ",
    "dentro de",
    "de ",
    "for ",
    "in ",
    "segmento",
    "subsector",
    "categoria",
    "categoria comercial",
    "macrosector",
    "sector",
    "category",
    "segment",
    "macrosector",
  ];
  return cues.some((cue) => text.includes(cue));
}

function resolveExplicitFilters(message: string, classifier: AgentClassifierResult): AgentResolvedFilters | undefined {
  const explicit = hasExplicitTaxonomyIntent(message);
  const fromClassifier = classifier.filters || {};
  const resolvedSector = resolveCanonicalFromAliases(message, fromClassifier.sector, SECTOR_ALIASES);
  const resolvedSubsector = resolveCanonicalFromAliases(message, fromClassifier.subsector, SUBSECTOR_ALIASES);
  const resolvedCategory = resolveCanonicalFromAliases(message, fromClassifier.category, CATEGORY_ALIASES);

  // Extract direct year mentions if present.
  const yearMatches = Array.from(message.matchAll(/\b(19|20)\d{2}\b/g)).map((m) => m[0]);
  const uniqueYears = Array.from(new Set(yearMatches)).slice(0, 6);
  const classifierYears = Array.isArray(fromClassifier.years)
    ? fromClassifier.years.filter((item): item is string => typeof item === "string" && /\b(19|20)\d{2}\b/.test(item))
    : [];
  const years = uniqueYears.length > 0 ? uniqueYears : classifierYears;

  const hasAny = Boolean(resolvedSector || resolvedSubsector || resolvedCategory || years.length);
  if (!hasAny || !explicit) {
    return undefined;
  }
  return {
    sector: resolvedSector,
    subsector: resolvedSubsector,
    category: resolvedCategory,
    years: years.length ? years : null,
  };
}


function hasBrandDisclosureIntent(message: string) {
  const text = message.toLowerCase();
  return (
    text.includes("lista de marcas") ||
    text.includes("qué marcas") ||
    text.includes("que marcas") ||
    text.includes("top marcas") ||
    text.includes("brands list") ||
    text.includes("which brands")
  );
}

function looksLikeBbsIntent(message: string, context: AgentContext) {
  const text = message.toLowerCase();
  const keywords = [
    "bbs", "benchmark", "journey", "network", "trends", "trend", "nps", "csat", "kpi",
    "funnel", "heatmap", "awareness", "consideration", "purchase", "satisfaction", "recommendation",
    "touchpoint", "sector", "subsector", "categor", "macrosector", "segmento", "brand", "marca",
    "estudio", "muestra", "entrevista", "resultado", "resultados", "tendencia", "año", "años", "compar", "waldos"
  ];
  if (keywords.some((keyword) => text.includes(keyword))) return true;
  if (context.sector || context.subsector || context.category) return true;
  if ((context.years || []).length > 0) return true;
  if ((context.brands || []).length > 0) return true;
  return false;
}

function inferToolFromMessage(message: string): NonNullable<AgentClassifierResult["tool"]> {
  const text = message.toLowerCase();
  if (text.includes("evolucion") || text.includes("evolución") || text.includes("a lo largo de los anos") || text.includes("a lo largo de los años")) {
    return "journey_overview";
  }
  if (text.includes("touchpoint") || text.includes("canal") || text.includes("recall")) {
    return "network_overview";
  }
  if (
    text.includes("journey") ||
    text.includes("funnel") ||
    text.includes("nps") ||
    text.includes("csat") ||
    text.includes("marca") ||
    text.includes("brand") ||
    text.includes("marketing") ||
    text.includes("recomend") ||
    text.includes("años") ||
    text.includes("tendencia")
  ) {
    return "journey_overview";
  }
  return "journey_overview";
}

function buildAnalyticsPayload(context: AgentContext, classifier: AgentClassifierResult) {
  const filters = classifier.filters || {};
  return {
    taxonomy_view: "market",
    sector: filters.sector ?? context.sector ?? null,
    subsector: filters.subsector ?? context.subsector ?? null,
    category: filters.category ?? context.category ?? null,
    years: Array.isArray(filters.years) && filters.years.length ? filters.years : context.years ?? null,
    gender: context.gender ?? null,
    nse: context.nse ?? null,
    age_min: context.age_min ?? null,
    age_max: context.age_max ?? null,
    brands: context.brands ?? null,
  };
}

async function runTool(
  request: NextRequest,
  tool: NonNullable<AgentClassifierResult["tool"]>,
  context: AgentContext,
  classifier: AgentClassifierResult
) {
  const payload = buildAnalyticsPayload(context, classifier);
  if (tool === "journey_overview") {
    return callInternalApi(
      request,
      "/api/analytics/journey/table_multi?limit_mode=all&sort_by=brand_awareness&sort_dir=desc&response_mode=full",
      "POST",
      payload
    );
  }
  if (tool === "network_overview") {
    return callInternalApi(
      request,
      "/api/analytics/touchpoints/table_multi?limit_mode=all&sort_by=recall&sort_dir=desc",
      "POST",
      payload
    );
  }
  return callInternalApi(request, "/api/analytics/tracking/series", "POST", payload);
}

function fallbackSequenceForTool(tool: NonNullable<AgentClassifierResult["tool"]>) {
  const sequence: Array<NonNullable<AgentClassifierResult["tool"]>> = [tool, "journey_overview", "tracking_series"];
  return Array.from(new Set(sequence));
}

function hasEvolutionIntent(message: string) {
  const text = normalizeText(message);
  return (
    text.includes("evolucion") ||
    text.includes("a lo largo de los anos") ||
    text.includes("tendencia")
  );
}

function extractRequestedJourneyMetrics(message: string) {
  const text = normalizeText(message);
  const metrics: Array<{ key: string; label: string }> = [];
  if (text.includes("consideracion") || text.includes("consideration")) {
    metrics.push({ key: "brand_consideration", label: "Consideracion" });
  }
  if (text.includes("recomendacion") || text.includes("recommendation")) {
    metrics.push({ key: "brand_recommendation", label: "Recomendacion" });
  }
  if (text.includes("awareness")) {
    metrics.push({ key: "brand_awareness", label: "Awareness" });
  }
  if (text.includes("purchase") || text.includes("compra")) {
    metrics.push({ key: "brand_purchase", label: "Purchase" });
  }
  if (!metrics.length) {
    metrics.push({ key: "brand_awareness", label: "Awareness" });
  }
  return metrics;
}

function asFiniteNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function extractYearFromRow(row: Record<string, unknown>): string | null {
  const yearCandidates = [row.year, row.time, row.period, row.wave];
  for (const candidate of yearCandidates) {
    if (typeof candidate === "string") {
      const match = candidate.match(/(19|20)\d{2}/);
      if (match) return match[0];
    }
  }
  const studyId = typeof row.study_id === "string" ? row.study_id : typeof row.studyId === "string" ? row.studyId : null;
  if (studyId) {
    const match = studyId.match(/(19|20)\d{2}/);
    if (match) return match[0];
  }
  return null;
}

function buildEvolutionResponseFromSummary(
  summary: ToolSummary,
  intentMessage: string
): { text: string; chart_spec: AgentChartSpec | null } | null {
  if (summary.type !== "journey_overview" || !hasEvolutionIntent(intentMessage)) return null;
  const rows = summary.rows.filter((row): row is Record<string, unknown> => Boolean(row && typeof row === "object"));
  if (!rows.length) return null;

  const metrics = extractRequestedJourneyMetrics(intentMessage);
  const byYearMetric = new Map<string, Map<string, number[]>>();
  const yearsSet = new Set<string>();

  for (const row of rows) {
    const year = extractYearFromRow(row);
    if (!year) continue;
    yearsSet.add(year);
    if (!byYearMetric.has(year)) byYearMetric.set(year, new Map<string, number[]>());
    const metricMap = byYearMetric.get(year)!;
    for (const metric of metrics) {
      const value = asFiniteNumber(row[metric.key]);
      if (value == null) continue;
      if (!metricMap.has(metric.key)) metricMap.set(metric.key, []);
      metricMap.get(metric.key)!.push(value);
    }
  }

  const years = Array.from(yearsSet).sort();
  if (!years.length) return null;
  const series = metrics
    .map((metric) => {
      const data: number[] = [];
      for (const year of years) {
        const values = byYearMetric.get(year)?.get(metric.key) || [];
        if (!values.length) {
          data.push(NaN);
          continue;
        }
        const avg = values.reduce((sum, item) => sum + item, 0) / values.length;
        data.push(Number(avg.toFixed(1)));
      }
      const sanitized = data.map((v) => (Number.isFinite(v) ? v : 0));
      return { name: metric.label, data: sanitized };
    })
    .filter((s) => s.data.some((v) => Number.isFinite(v)));

  if (!series.length) return null;

  const availableYears = years.join(", ");
  return {
    text: `Evolucion calculada con datos BBS para ${availableYears}.`,
    chart_spec: {
      type: "line",
      title: "Evolucion por ano",
      x: years,
      series,
      y_label: "%",
    },
  };
}

function normalizeText(value: string) {
  return value
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function detectResponseLanguage(message: string): AgentResponseLanguage {
  const raw = (message || "").toLowerCase();
  if (!raw.trim()) return "es";

  if (/[áéíóúñ¿¡]/i.test(raw)) return "es";

  const esHints = [
    "que", "como", "dame", "quiero", "analiza", "marca", "categoria", "categoria", "segmento",
    "macrosector", "años", "ano", "estrategia", "recomendacion", "muestra", "grafica", "tabla"
  ];
  const enHints = [
    "what", "show", "give me", "analyze", "brand", "category", "segment", "sector",
    "year", "strategy", "recommendation", "chart", "table", "insight"
  ];

  const esScore = esHints.reduce((acc, token) => (raw.includes(token) ? acc + 1 : acc), 0);
  const enScore = enHints.reduce((acc, token) => (raw.includes(token) ? acc + 1 : acc), 0);
  return enScore > esScore ? "en" : "es";
}

function looksLikeEnglish(text: string) {
  const raw = (text || "").toLowerCase();
  const hits = [
    "key finding",
    "business implication",
    "tactical recommendation",
    "there is",
    "this query",
    "please",
    "brand awareness",
    "within",
    "shows",
  ].reduce((acc, token) => (raw.includes(token) ? acc + 1 : acc), 0);
  return hits >= 2;
}

function looksLikeSpanish(text: string) {
  const raw = (text || "").toLowerCase();
  if (/[áéíóúñ¿¡]/i.test(raw)) return true;
  const hits = [
    "hallazgo",
    "implicacion",
    "recomendacion",
    "esta consulta",
    "por favor",
    "marca",
    "muestra",
    "categor",
    "macrosector",
  ].reduce((acc, token) => (raw.includes(token) ? acc + 1 : acc), 0);
  return hits >= 2;
}

async function enforceResponseLanguage(
  text: string,
  targetLanguage: AgentResponseLanguage
) {
  const needsFix =
    (targetLanguage === "es" && looksLikeEnglish(text) && !looksLikeSpanish(text)) ||
    (targetLanguage === "en" && looksLikeSpanish(text) && !looksLikeEnglish(text));
  if (!needsFix) return text;

  const rewrite = await callOpenAIJson([
    {
      role: "system",
      content:
        "Rewrite the input text to the requested language, preserving meaning, numbers, entities, and structure. " +
        "Return ONLY JSON with {\"text\":\"...\"}. Do not add new claims.",
    },
    {
      role: "user",
      content: JSON.stringify({
        target_language: targetLanguage === "es" ? "Spanish" : "English",
        text,
      }),
    },
  ]);
  const rewritten =
    typeof rewrite.text === "string" && rewrite.text.trim() ? rewrite.text.trim() : text;
  return rewritten;
}

function normalizeCompact(value: string) {
  return value
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]/g, "");
}

function getRowLabel(row: unknown) {
  const input = (row || {}) as Record<string, unknown>;
  const candidates = [input.brand, input.brand_name, input.brandName, input.entity, input.name, input.label];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim()) return candidate.trim();
  }
  return "";
}

function filterRowsByBrandIntent(rows: unknown[], userMessage: string) {
  const normalizedMessage = normalizeText(userMessage);
  const compactMessage = normalizeCompact(userMessage);
  if (!normalizedMessage) return rows;

  const exactMatches = rows.filter((row) => {
    const brand = getRowLabel(row);
    if (!brand) return false;
    const normalizedBrand = normalizeText(brand);
    const compactBrand = normalizeCompact(brand);
    if (compactBrand.length < 3) return false;
    return (
      normalizedMessage.includes(normalizedBrand) ||
      compactMessage.includes(compactBrand) ||
      compactBrand.includes(compactMessage)
    );
  });
  if (exactMatches.length > 0) return exactMatches;

  const tokens = normalizedMessage
    .split(" ")
    .map((token) => token.trim())
    .filter((token) => token.length >= 4)
    .filter((token) => !["como", "para", "funcion", "resultados", "marca", "trabajando", "deberia", "hacer"].includes(token));
  if (tokens.length === 0) return rows;

  const tokenMatches = rows.filter((row) => {
    const brand = getRowLabel(row);
    if (!brand) return false;
    const normalizedBrand = normalizeText(brand);
    return tokens.some((token) => normalizedBrand.includes(token));
  });
  return tokenMatches.length > 0 ? tokenMatches : rows;
}

function summarizeToolData(tool: string, data: unknown, userMessage: string) {
  if (!data || typeof data !== "object") return { type: tool, rows: [] as unknown[] };
  if (tool === "tracking_series") {
    const payload = Array.isArray(data) ? data[0] : data;
    const series = (payload as { bbs_tracking_series?: Record<string, unknown> })?.bbs_tracking_series;
    const rows = Array.isArray((series as { entity_rows?: unknown[] })?.entity_rows)
      ? (((series as { entity_rows?: unknown[] }).entity_rows || []) as unknown[])
      : [];
    return {
      type: "tracking_series",
      entity_label: (series as { entity_label?: string })?.entity_label || "Entity",
      periods: Array.isArray((series as { periods?: unknown[] })?.periods)
        ? (series as { periods?: unknown[] }).periods
        : [],
      rows: rows.slice(0, 20),
    };
  }
  const rows = Array.isArray((data as { rows?: unknown[] }).rows) ? ((data as { rows?: unknown[] }).rows as unknown[]) : [];
  const filteredRows = filterRowsByBrandIntent(rows, userMessage);
  const effectiveRows = filteredRows.length > 0 ? filteredRows : rows;
  return {
    type: tool,
    rows: effectiveRows.slice(0, 120),
  };
}

function getSummaryRowCount(summary: ToolSummary) {
  return Array.isArray(summary.rows) ? summary.rows.length : 0;
}

export async function generateAgentResponse(params: {
  request: NextRequest;
  authz: RequestAuthz;
  userMessage: string;
  context?: AgentContext | null;
  history: Array<{ role: "user" | "assistant"; content: string }>;
}) {
  const { request, authz, userMessage } = params;
  const agentGuide = await loadAgentGuide();
  const context = normalizeContext(params.context, authz);
  const recentUserContext = params.history
    .filter((item) => item.role === "user")
    .slice(-4)
    .map((item) => item.content)
    .join(" ");
  const intentContext = `${recentUserContext} ${userMessage}`.trim();
  const responseLanguage = detectResponseLanguage(userMessage);
  const explicitChartRequested = hasExplicitChartIntent(intentContext);

  if (isGreetingOnly(userMessage)) {
    return {
      text: pickLocalizedText(
        responseLanguage,
        "Hola! Mucho gusto soy BBS Agent, tu asistente de Inteligencia Artificial. Preguntame lo que necesites sobre Brand Benchmark Score o sobre los datos.",
        "Hi! Nice to meet you, I'm BBS Agent, your AI assistant. Ask me anything you need about Brand Benchmark Score or its data."
      ),
      chart_spec: null as AgentChartSpec | null,
      meta: {
        tool_used: null,
        fallback_used: false,
        row_count: 0,
        denial_reason: null,
        scope_applied: getScopeAppliedMeta(authz, undefined),
      },
    };
  }

  if (!authz.can_toggle_brands && hasBrandDisclosureIntent(intentContext)) {
    return {
      text: pickLocalizedText(
        responseLanguage,
        "Lo siento, no tienes acceso a este tipo de informacion. Contacta a contacto@naawaconsulting.com si necesitas acceso adicional. Puedo ayudarte con un analisis agregado por macrosector, segmento o categoria.",
        "Sorry, you don't have access to this type of information. Contact contacto@naawaconsulting.com if you need additional access. I can help with aggregated analysis by macrosector, segment, or category."
      ),
      chart_spec: null as AgentChartSpec | null,
      meta: {
        tool_used: null,
        fallback_used: false,
        row_count: 0,
        denial_reason: "brands_toggle_disabled",
        scope_applied: getScopeAppliedMeta(authz, undefined),
      },
    };
  }

  const historySnippet = params.history.slice(-6).map((item) => `${item.role.toUpperCase()}: ${item.content}`).join("\n");
  const classifierRaw = await callOpenAIJson([
    {
      role: "system",
      content:
        `${agentGuide}\n\n` +
        "Task: classify request for tool routing. Respond ONLY JSON with fields: in_scope(boolean), needs_brand_level(boolean), tool(journey_overview|network_overview|tracking_series), metric(string|null), chart_type(bar|line|table|null), filters{sector,subsector,category,years[]}.\n" +
        "A request is in_scope when it asks analysis/recommendation based on BBS data.\n" +
        "Mark out_of_scope only for fully external topics.\n" +
        "Do not infer taxonomy filters unless explicitly requested by user text.\n" +
        "If user did not explicitly request chart/table, set chart_type to null.\n" +
        `Target response language: ${responseLanguage === "es" ? "Spanish" : "English"}.`,
    },
    {
      role: "user",
      content: JSON.stringify({
        message: userMessage,
        context,
        history: historySnippet,
      }),
    },
  ]);

  const classifier = classifierRaw as AgentClassifierResult;
  classifier.filters = resolveExplicitFilters(intentContext, classifier);
  if (!explicitChartRequested) classifier.chart_type = null;
  if (classifier.in_scope === false && looksLikeBbsIntent(intentContext, context)) {
    classifier.in_scope = true;
    if (!classifier.tool) {
      classifier.tool = inferToolFromMessage(intentContext);
    }
  }
  if (authz.can_toggle_brands) {
    const strategicBrandQuery =
      /marca|brand/i.test(intentContext) &&
      /deber|recomend|marketing|estrateg|accion|hacer|plan|riesgo/i.test(intentContext);
    if (strategicBrandQuery) {
      classifier.tool = "journey_overview";
      classifier.in_scope = true;
      if (!classifier.chart_type) classifier.chart_type = "table";
      // Evita que el router fuerce filtros de taxonomia para preguntas de marca.
      classifier.filters = undefined;
    }
  }
  if (hasEvolutionIntent(intentContext)) {
    classifier.tool = "journey_overview";
    classifier.in_scope = true;
    if (!classifier.chart_type) classifier.chart_type = "line";
  }
  if (classifier.in_scope === false) {
    return {
      text: pickLocalizedText(
        responseLanguage,
        "Esta consulta esta fuera del alcance de la base de datos de BBS. Para temas externos, usa la IA de tu preferencia.",
        "This query is outside the scope of the BBS database. For external topics, use your preferred AI assistant."
      ),
      chart_spec: null as AgentChartSpec | null,
      meta: {
        tool_used: null,
        fallback_used: false,
        row_count: 0,
        denial_reason: "out_of_scope",
        scope_applied: getScopeAppliedMeta(authz, classifier.filters),
      },
    };
  }
  if (classifier.needs_brand_level && !authz.can_toggle_brands) {
    return {
      text: pickLocalizedText(
        responseLanguage,
        "Lo siento, no tienes acceso a este tipo de informacion. Contacta a contacto@naawaconsulting.com si necesitas acceso adicional. Puedo darte recomendaciones a nivel categoria/segmento.",
        "Sorry, you don't have access to this type of information. Contact contacto@naawaconsulting.com if you need additional access. I can provide recommendations at category/segment level."
      ),
      chart_spec: null as AgentChartSpec | null,
      meta: {
        tool_used: null,
        fallback_used: false,
        row_count: 0,
        denial_reason: "brands_toggle_disabled",
        scope_applied: getScopeAppliedMeta(authz, classifier.filters),
      },
    };
  }

  const tool = classifier.tool || "tracking_series";
  let usedTool = tool;
  let fallbackUsed = false;
  let summary: ToolSummary = { type: tool, rows: [] };
  const toolErrors: Array<Record<string, unknown>> = [];

  const primaryResult = await runTool(request, tool, context, classifier);
  if (primaryResult.ok) {
    summary = summarizeToolData(tool, primaryResult.data, intentContext) as ToolSummary;
  } else {
    toolErrors.push({ tool, status: primaryResult.status, error: primaryResult.data });
  }

  if (getSummaryRowCount(summary) === 0 && classifier.filters) {
    const fallbackClassifier: AgentClassifierResult = { ...classifier, filters: undefined };
    const fallbackResult = await runTool(request, tool, context, fallbackClassifier);
    if (fallbackResult.ok) {
      const fallbackSummary = summarizeToolData(tool, fallbackResult.data, intentContext) as ToolSummary;
      if (getSummaryRowCount(fallbackSummary) > 0) {
        summary = fallbackSummary;
        usedTool = tool;
        fallbackUsed = true;
      }
    } else {
      toolErrors.push({ tool, status: fallbackResult.status, error: fallbackResult.data, stage: "same_tool_unfiltered" });
    }
  }

  if (getSummaryRowCount(summary) === 0) {
    for (const candidateTool of fallbackSequenceForTool(tool)) {
      if (candidateTool === tool) continue;
      const fallbackClassifier: AgentClassifierResult = {
        ...classifier,
        tool: candidateTool,
        filters: undefined,
      };
      const candidateResult = await runTool(request, candidateTool, context, fallbackClassifier);
      if (!candidateResult.ok) {
        toolErrors.push({ tool: candidateTool, status: candidateResult.status, error: candidateResult.data });
        continue;
      }
      const candidateSummary = summarizeToolData(candidateTool, candidateResult.data, intentContext) as ToolSummary;
      if (getSummaryRowCount(candidateSummary) > 0) {
        summary = candidateSummary;
        usedTool = candidateTool;
        fallbackUsed = true;
        break;
      }
    }
  }

  if (getSummaryRowCount(summary) === 0 && toolErrors.length > 0) {
    return {
      text: pickLocalizedText(
        responseLanguage,
        "No pude consultar la base BBS en este momento. Intenta nuevamente en unos segundos.",
        "I couldn't query the BBS database right now. Please try again in a few seconds."
      ),
      chart_spec: null as AgentChartSpec | null,
      meta: {
        tool_used: usedTool,
        fallback_used: fallbackUsed,
        row_count: 0,
        denial_reason: "tool_error",
        scope_applied: getScopeAppliedMeta(authz, classifier.filters),
        tool_errors: toolErrors,
      },
    };
  }

  const deterministicEvolution = buildEvolutionResponseFromSummary(summary, intentContext);
  if (deterministicEvolution) {
    const deterministicText =
      responseLanguage === "es"
        ? deterministicEvolution.text
        : deterministicEvolution.text.replace("Evolucion calculada con datos BBS para", "Evolution computed from BBS data for");
    const localizedDeterministicText = await enforceResponseLanguage(deterministicText, responseLanguage);
    return {
      text: localizedDeterministicText,
      chart_spec: deterministicEvolution.chart_spec,
      meta: {
        tool_used: usedTool,
        fallback_used: fallbackUsed,
        row_count: getSummaryRowCount(summary),
        denial_reason: null,
        scope_applied: getScopeAppliedMeta(authz, classifier.filters),
        classifier,
        used_context: context,
        deterministic: "evolution",
      },
    };
  }

  const synthesisRaw = await callOpenAIJson([
    {
      role: "system",
      content:
        `${agentGuide}\n\n` +
        "You are a BBS analyst synthesizer. Respond ONLY JSON: text(string), chart_spec(object|null).\n" +
        "Text must be concise, actionable, and based only on provided BBS data.\n" +
        "Default style: key finding, implication, tactical recommendation.\n" +
        "Only say no-data if rows is 0.\n" +
        "Only return chart_spec when user explicitly asked for chart/table.\n" +
        "Allowed chart_spec: type(bar|line|table), title, x, series[{name,data}], columns, rows, y_label.\n" +
        `Write the final answer in ${responseLanguage === "es" ? "Spanish" : "English"}.`,
    },
    {
      role: "user",
      content: JSON.stringify({
        question: userMessage,
        tool: usedTool,
        metric: classifier.metric || null,
        preferred_chart: explicitChartRequested ? classifier.chart_type || null : null,
        chart_requested: explicitChartRequested,
        data: summary,
      }),
    },
  ]);

  const text = typeof synthesisRaw.text === "string" && synthesisRaw.text.trim()
    ? synthesisRaw.text.trim()
    : pickLocalizedText(
        responseLanguage,
        "No se encontraron suficientes datos para responder con precision.",
        "Not enough data was found to answer precisely."
      );
  const safeText =
    getSummaryRowCount(summary) > 0 && /no hay datos|sin datos|datos insuficientes|no data|insufficient data/i.test(text)
      ? pickLocalizedText(
          responseLanguage,
          `Si hay datos en BBS para esta consulta (${getSummaryRowCount(summary)} filas analizadas). Intenta pedir un corte mas especifico (anio, macrosector o metrica) para devolver un diagnostico de riesgo mas preciso.`,
          `There is BBS data for this query (${getSummaryRowCount(summary)} rows analyzed). Try asking for a more specific cut (year, macrosector, or metric) for a more precise risk diagnosis.`
        )
      : text;
  const localizedText = await enforceResponseLanguage(safeText, responseLanguage);
  const chartSpec = explicitChartRequested ? sanitizeChartSpec(synthesisRaw.chart_spec) : null;
  return {
    text: localizedText,
    chart_spec: chartSpec,
    meta: {
      tool_used: usedTool,
      fallback_used: fallbackUsed,
      row_count: getSummaryRowCount(summary),
      denial_reason: null,
      scope_applied: getScopeAppliedMeta(authz, classifier.filters),
      classifier,
      used_context: context,
      tool_errors: toolErrors.length ? toolErrors : undefined,
    },
  };
}

export async function createConversationForUser(userId: string, title?: string) {
  const now = toIsoNow();
  const insert = await supabaseAdminPostgrest("agent_conversations", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: [
      {
        user_id: userId,
        title: title?.trim() ? title.trim() : "New chat",
        created_at: now,
        updated_at: now,
        last_message_at: now,
        archived: false,
      },
    ],
  });
  return insert;
}

