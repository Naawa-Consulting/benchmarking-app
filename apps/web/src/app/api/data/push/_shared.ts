import { supabaseAdminPostgrest } from "../../_lib/supabase-admin";
import fs from "node:fs";
import path from "node:path";

export type PushSummary = {
  study_ids: string[];
  journey_rows: number;
  journey_imputed_rows: number;
  journey_satisfaction_imputed_rows: number;
  journey_csat_imputed_rows: number;
  touchpoint_rows: number;
  study_catalog_rows: number;
  taxonomy_rows: number;
  taxonomy_market_rows: number;
  demographic_rows: number;
};

export type JourneyRow = {
  study_id?: string | null;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  market_sector?: string | null;
  market_subsector?: string | null;
  market_category?: string | null;
  year?: number | null;
  brand_consideration?: number | null;
  brand_consideration_imputed?: number | null;
  brand_consideration_source?: "observed" | "imputed" | "none" | null;
  brand_consideration_impute_level?: "category" | "subsector" | "sector" | "global" | "none" | null;
  brand_consideration_impute_version?: string | null;
  brand_satisfaction?: number | null;
  brand_satisfaction_imputed?: number | null;
  brand_satisfaction_source?: "observed" | "imputed" | "none" | null;
  brand_satisfaction_impute_level?: "category" | "subsector" | "sector" | "global" | "none" | null;
  brand_satisfaction_impute_version?: string | null;
  csat?: number | null;
  csat_imputed?: number | null;
  csat_source?: "observed" | "imputed" | "none" | null;
  csat_impute_level?: "category" | "subsector" | "sector" | "global" | "none" | null;
  csat_impute_version?: string | null;
};

type MarketLensRule = {
  sector?: string;
  subsector?: string;
  category?: string;
  market_sector?: string;
  market_subsector?: string;
  market_category?: string;
};

type MarketLensRules = {
  category_rules: MarketLensRule[];
  subsector_rules: MarketLensRule[];
  sector_rules: MarketLensRule[];
};

type StudyTaxonomyResolved = {
  sector: string;
  subsector: string;
  category: string;
  market_sector: string;
  market_subsector: string;
  market_category: string;
  market_source: "rule" | "manual";
};

export function canPush(role: string) {
  return role === "owner" || role === "admin";
}

export function normalizeStudyIds(body: Record<string, unknown> | null) {
  const single = typeof body?.study_id === "string" ? body.study_id : null;
  const list = Array.isArray(body?.study_ids) ? body.study_ids : [];
  const items = new Set<string>();
  if (single && single.trim()) items.add(single.trim());
  for (const value of list) {
    if (typeof value === "string" && value.trim()) items.add(value.trim());
  }
  return Array.from(items);
}

async function upsertRows(table: string, rows: Record<string, unknown>[], onConflict: string) {
  if (rows.length === 0) return;
  const { response, data } = await supabaseAdminPostgrest(`${table}?on_conflict=${encodeURIComponent(onConflict)}`, {
    method: "POST",
    headers: { Prefer: "resolution=merge-duplicates,return=minimal" },
    body: rows,
  });
  if (!response.ok) {
    throw new Error(`Supabase upsert failed on ${table}: ${JSON.stringify(data)}`);
  }
}

function encodeInList(values: string[]) {
  return values
    .map((value) => `"${value.replace(/"/g, '\\"')}"`)
    .join(",");
}

async function deleteRowsByStudyIds(table: string, studyIds: string[]) {
  if (studyIds.length === 0) return;
  const inExpr = encodeInList(studyIds);
  const { response, data } = await supabaseAdminPostgrest(`${table}?study_id=in.(${encodeURIComponent(inExpr)})`, {
    method: "DELETE",
    headers: { Prefer: "return=minimal" },
  });
  if (!response.ok) {
    throw new Error(`Supabase delete failed on ${table}: ${JSON.stringify(data)}`);
  }
}

function deriveDemographicRows(payload: Record<string, unknown> | null) {
  const rows: Record<string, unknown>[] = [];
  const gender = Array.isArray(payload?.gender) ? payload?.gender : [];
  const nse = Array.isArray(payload?.nse) ? payload?.nse : [];
  const state = Array.isArray(payload?.state) ? payload?.state : [];
  const age = payload && typeof payload.age === "object" && payload.age ? (payload.age as Record<string, unknown>) : {};
  const ageMin = age?.min;
  const ageMax = age?.max;

  for (const value of gender) {
    if (typeof value !== "string" || !value.trim()) continue;
    rows.push({ gender: value, nse: null, state: null, age_min: null, age_max: null });
  }
  for (const value of nse) {
    if (typeof value !== "string" || !value.trim()) continue;
    rows.push({ gender: null, nse: value, state: null, age_min: null, age_max: null });
  }
  for (const value of state) {
    if (typeof value !== "string" || !value.trim()) continue;
    rows.push({ gender: null, nse: null, state: value, age_min: null, age_max: null });
  }
  if (ageMin != null || ageMax != null) {
    rows.push({ gender: null, nse: null, state: null, age_min: ageMin ?? null, age_max: ageMax ?? null });
  }
  return rows;
}

function deriveYearFromStudyId(studyId: string | null | undefined): number | null {
  if (!studyId) return null;
  const match = String(studyId).match(/(19|20)\d{2}/);
  if (!match) return null;
  const year = Number(match[0]);
  return Number.isFinite(year) ? year : null;
}

function normalizeTaxonomyValue(value: unknown) {
  if (typeof value !== "string") return "Unassigned";
  const trimmed = value.trim();
  return trimmed || "Unassigned";
}

function normalizeForMatch(value: unknown) {
  if (typeof value !== "string") return "";
  return value
    .normalize("NFD")
    .replace(new RegExp("[\\u0300-\\u036f]", "g"), "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

let cachedMarketRules: MarketLensRules | null = null;
function loadMarketLensRules(): MarketLensRules {
  if (cachedMarketRules) return cachedMarketRules;
  const candidates = [
    path.resolve(process.cwd(), "../../data/warehouse/taxonomy/market_lens_rules_v1.json"),
    path.resolve(process.cwd(), "data/warehouse/taxonomy/market_lens_rules_v1.json"),
  ];
  for (const filePath of candidates) {
    try {
      if (!fs.existsSync(filePath)) continue;
      const parsed = JSON.parse(fs.readFileSync(filePath, "utf8")) as Partial<MarketLensRules>;
      cachedMarketRules = {
        category_rules: Array.isArray(parsed.category_rules) ? parsed.category_rules : [],
        subsector_rules: Array.isArray(parsed.subsector_rules) ? parsed.subsector_rules : [],
        sector_rules: Array.isArray(parsed.sector_rules) ? parsed.sector_rules : [],
      };
      return cachedMarketRules;
    } catch {
      // Try next candidate.
    }
  }
  cachedMarketRules = { category_rules: [], subsector_rules: [], sector_rules: [] };
  return cachedMarketRules;
}

function deriveMarketLensFromStandard(
  sector: string | null | undefined,
  subsector: string | null | undefined,
  category: string | null | undefined
) {
  const s = normalizeTaxonomyValue(sector);
  const ss = normalizeTaxonomyValue(subsector);
  const c = normalizeTaxonomyValue(category);
  const rules = loadMarketLensRules();
  const cNorm = normalizeForMatch(c);
  const ssNorm = normalizeForMatch(ss);
  const sNorm = normalizeForMatch(s);

  const categoryRule = rules.category_rules.find(
    (rule) => normalizeForMatch(rule.category) === cNorm
  );
  if (categoryRule) {
    return {
      market_sector: normalizeTaxonomyValue(categoryRule.market_sector),
      market_subsector: normalizeTaxonomyValue(categoryRule.market_subsector),
      market_category: normalizeTaxonomyValue(categoryRule.market_category),
      market_source: "rule" as const,
    };
  }

  const subsectorRule = rules.subsector_rules.find((rule) => {
    if (normalizeForMatch(rule.subsector) !== ssNorm) return false;
    if (rule.sector && normalizeForMatch(rule.sector) !== sNorm) return false;
    return true;
  });
  if (subsectorRule) {
    return {
      market_sector: normalizeTaxonomyValue(subsectorRule.market_sector),
      market_subsector: normalizeTaxonomyValue(subsectorRule.market_subsector),
      market_category: normalizeTaxonomyValue(subsectorRule.market_category),
      market_source: "rule" as const,
    };
  }

  const sectorRule = rules.sector_rules.find(
    (rule) => normalizeForMatch(rule.sector) === sNorm
  );
  if (sectorRule) {
    return {
      market_sector: normalizeTaxonomyValue(sectorRule.market_sector),
      market_subsector: normalizeTaxonomyValue(sectorRule.market_subsector),
      market_category: normalizeTaxonomyValue(sectorRule.market_category),
      market_source: "rule" as const,
    };
  }

  return {
    market_sector: s,
    market_subsector: ss,
    market_category: c,
    market_source: "rule" as const,
  };
}

function buildStudyTaxonomyFromJourney(studyIds: string[], rows: JourneyRow[]) {
  const counters = new Map<string, Map<string, number>>();

  for (const studyId of studyIds) {
    counters.set(studyId, new Map<string, number>());
  }

  for (const row of rows) {
    const studyId = typeof row.study_id === "string" ? row.study_id.trim() : "";
    if (!studyId || !counters.has(studyId)) continue;
    const sector = normalizeTaxonomyValue(row.sector);
    const subsector = normalizeTaxonomyValue(row.subsector);
    const category = normalizeTaxonomyValue(row.category);
    const key = `${sector}|||${subsector}|||${category}`;
    const map = counters.get(studyId)!;
    map.set(key, (map.get(key) || 0) + 1);
  }

  const resolved = new Map<string, { sector: string; subsector: string; category: string }>();
  for (const [studyId, map] of counters.entries()) {
    const ranked = Array.from(map.entries()).sort((a, b) => {
      if (b[1] !== a[1]) return b[1] - a[1];
      const aIsUnassigned = a[0].startsWith("Unassigned|||");
      const bIsUnassigned = b[0].startsWith("Unassigned|||");
      if (aIsUnassigned !== bIsUnassigned) return aIsUnassigned ? 1 : -1;
      return a[0].localeCompare(b[0]);
    });
    const best = ranked[0]?.[0];
    if (!best) continue;
    const [sector, subsector, category] = best.split("|||");
    resolved.set(studyId, {
      sector: sector || "Unassigned",
      subsector: subsector || "Unassigned",
      category: category || "Unassigned",
    });
  }
  return resolved;
}

export async function computePushSummary(
  studyIds: string[],
  journeyResult: unknown,
  touchpointResult: unknown,
  studiesResult: unknown,
  taxonomyResult: unknown,
  demographicsResult: unknown
): Promise<PushSummary> {
  const journeyRows = Array.isArray((journeyResult as { rows?: unknown[] })?.rows)
    ? ((journeyResult as { rows: JourneyRow[] }).rows || [])
    : [];
  const touchpointRows = Array.isArray((touchpointResult as { rows?: unknown[] })?.rows)
    ? ((touchpointResult as { rows: Record<string, unknown>[] }).rows || [])
    : [];

  const allStudies = Array.isArray(studiesResult)
    ? (studiesResult as Record<string, unknown>[])
    : Array.isArray((studiesResult as { studies?: unknown[] })?.studies)
      ? ((studiesResult as { studies: Record<string, unknown>[] }).studies || [])
      : [];
  const taxonomyByStudy = buildStudyTaxonomyFromJourney(studyIds, journeyRows);
  const studyRows = allStudies
    .filter((row) => typeof row.id === "string" && studyIds.includes(String(row.id)))
    .map((row) => {
      const studyId = String(row.id);
      const taxonomy = taxonomyByStudy.get(studyId);
      const standardSector = normalizeTaxonomyValue(taxonomy?.sector ?? row.sector);
      const standardSubsector = normalizeTaxonomyValue(taxonomy?.subsector ?? row.subsector);
      const standardCategory = normalizeTaxonomyValue(taxonomy?.category ?? row.category);

      const derivedMarket = deriveMarketLensFromStandard(
        standardSector,
        standardSubsector,
        standardCategory
      );
      const hasManualMarket =
        typeof row.market_sector === "string" &&
        row.market_sector.trim() &&
        typeof row.market_subsector === "string" &&
        row.market_subsector.trim() &&
        typeof row.market_category === "string" &&
        row.market_category.trim();
      const marketSector = hasManualMarket
        ? normalizeTaxonomyValue(row.market_sector)
        : derivedMarket.market_sector;
      const marketSubsector = hasManualMarket
        ? normalizeTaxonomyValue(row.market_subsector)
        : derivedMarket.market_subsector;
      const marketCategory = hasManualMarket
        ? normalizeTaxonomyValue(row.market_category)
        : derivedMarket.market_category;
      const marketSource = hasManualMarket && row.market_source === "manual" ? "manual" : "rule";
      return {
        study_id: studyId,
        study_name: row.name || studyId,
        sector: standardSector,
        subsector: standardSubsector,
        category: standardCategory,
        market_sector: marketSector,
        market_subsector: marketSubsector,
        market_category: marketCategory,
        market_source: marketSource,
        has_demographics: true,
        has_date: true,
      };
    });

  const taxonomyByStudyId = new Map<string, StudyTaxonomyResolved>(
    studyRows.map((row) => [
      String(row.study_id),
      {
        sector: String(row.sector || "Unassigned"),
        subsector: String(row.subsector || "Unassigned"),
        category: String(row.category || "Unassigned"),
        market_sector: String(row.market_sector || "Unassigned"),
        market_subsector: String(row.market_subsector || "Unassigned"),
        market_category: String(row.market_category || "Unassigned"),
        market_source: row.market_source === "manual" ? "manual" : "rule",
      },
    ])
  );

  const journeyRowsWithMarket = journeyRows.map((row) => {
    const studyId = typeof row.study_id === "string" ? row.study_id : "";
    const resolved = taxonomyByStudyId.get(studyId);
    const derivedYear = deriveYearFromStudyId(studyId);
    return {
      ...row,
      market_sector: row.market_sector || resolved?.market_sector || null,
      market_subsector: row.market_subsector || resolved?.market_subsector || null,
      market_category: row.market_category || resolved?.market_category || null,
      year: typeof row.year === "number" && Number.isFinite(row.year) ? row.year : derivedYear,
    };
  });

  const touchpointRowsWithMarket = touchpointRows.map((row) => {
    const studyId = typeof row.study_id === "string" ? String(row.study_id) : "";
    const resolved = taxonomyByStudyId.get(studyId);
    const rawYear = (row as Record<string, unknown>).year;
    const derivedYear = deriveYearFromStudyId(studyId);
    return {
      ...row,
      market_sector: row.market_sector || resolved?.market_sector || null,
      market_subsector: row.market_subsector || resolved?.market_subsector || null,
      market_category: row.market_category || resolved?.market_category || null,
      year: typeof rawYear === "number" && Number.isFinite(rawYear) ? rawYear : derivedYear,
    };
  });

  const taxonomyRows = Array.isArray((taxonomyResult as { items?: unknown[] })?.items)
    ? (((taxonomyResult as { items: Record<string, unknown>[] }).items || []).map((item) => ({
        sector: item.sector,
        subsector: item.subsector,
        category: item.category,
      })) as Record<string, unknown>[])
    : [];
  const taxonomyMarketRows = Array.from(
    new Map(
      studyRows.map((row) => [
        `${String(row.market_sector || "")}|||${String(row.market_subsector || "")}|||${String(
          row.market_category || ""
        )}`,
        {
          market_sector: row.market_sector,
          market_subsector: row.market_subsector,
          market_category: row.market_category,
        },
      ])
    ).values()
  ).filter(
    (row) =>
      typeof row.market_sector === "string" &&
      row.market_sector.trim() &&
      typeof row.market_subsector === "string" &&
      row.market_subsector.trim() &&
      typeof row.market_category === "string" &&
      row.market_category.trim()
  );

  const demographicRows = deriveDemographicRows(demographicsResult as Record<string, unknown>);

  // Replace snapshot rows for selected studies to avoid mixed taxonomy/history artifacts.
  await Promise.all([
    deleteRowsByStudyIds("journey_metrics", studyIds),
    deleteRowsByStudyIds("touchpoint_metrics", studyIds),
    deleteRowsByStudyIds("study_catalog", studyIds),
  ]);

  await Promise.all([
    upsertRows("journey_metrics", journeyRowsWithMarket, "study_id,sector,subsector,category,brand"),
    upsertRows("touchpoint_metrics", touchpointRowsWithMarket, "study_id,sector,subsector,category,brand,touchpoint"),
    upsertRows("study_catalog", studyRows, "study_id"),
    upsertRows("taxonomy", taxonomyRows, "sector,subsector,category"),
    upsertRows("taxonomy_market_lens", taxonomyMarketRows, "market_sector,market_subsector,market_category"),
    upsertRows("demographic_options", demographicRows, "gender,nse,state,age_min,age_max"),
  ]);

  return {
    study_ids: studyIds,
    journey_rows: journeyRowsWithMarket.length,
    journey_imputed_rows: journeyRowsWithMarket.filter(
      (row) => row.brand_consideration_source === "imputed"
    ).length,
    journey_satisfaction_imputed_rows: journeyRowsWithMarket.filter(
      (row) => row.brand_satisfaction_source === "imputed"
    ).length,
    journey_csat_imputed_rows: journeyRowsWithMarket.filter(
      (row) => row.csat_source === "imputed"
    ).length,
    touchpoint_rows: touchpointRowsWithMarket.length,
    study_catalog_rows: studyRows.length,
    taxonomy_rows: taxonomyRows.length,
    taxonomy_market_rows: taxonomyMarketRows.length,
    demographic_rows: demographicRows.length,
  };
}

export async function markJobFailed(jobId: string, detail: string, logMessage: string) {
  await supabaseAdminPostgrest("ingestion_jobs?id=eq." + jobId, {
    method: "PATCH",
    body: {
      status: "error",
      finished_at: new Date().toISOString(),
      error_message: detail,
    },
  });
  await supabaseAdminPostgrest("ingestion_job_logs", {
    method: "POST",
    body: [
      {
        job_id: jobId,
        level: "error",
        message: logMessage,
        context: { detail },
      },
    ],
  });
}

export async function finalizePush(
  jobId: string,
  studyIds: string[],
  journeyResult: unknown,
  touchpointResult: unknown,
  studiesResult: unknown,
  taxonomyResult: unknown,
  demographicsResult: unknown,
  pushedBy?: string | null
): Promise<{ ok: boolean; job_id: string; summary?: PushSummary; detail?: string }> {
  try {
    const summary = await computePushSummary(
      studyIds,
      journeyResult,
      touchpointResult,
      studiesResult,
      taxonomyResult,
      demographicsResult
    );

    await supabaseAdminPostgrest("ingestion_jobs?id=eq." + jobId, {
      method: "PATCH",
      body: {
        status: "success",
        finished_at: new Date().toISOString(),
        error_message: null,
        payload: { ...summary, pushed_by: pushedBy || null },
      },
    });

    await supabaseAdminPostgrest("ingestion_job_logs", {
      method: "POST",
      body: [
        {
          job_id: jobId,
          level: "info",
          message: "Snapshot pushed to Supabase.",
          context: summary,
        },
      ],
    });

    return { ok: true, job_id: jobId, summary };
  } catch (error) {
    const detail = error instanceof Error ? error.message : "Push failed.";
    await markJobFailed(jobId, detail, "Snapshot push failed.");
    return { ok: false, job_id: jobId, detail };
  }
}
