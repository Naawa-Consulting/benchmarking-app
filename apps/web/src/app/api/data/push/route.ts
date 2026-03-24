import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../_lib/authz";
import { supabaseAdminPostgrest } from "../../_lib/supabase-admin";
import fs from "node:fs";
import path from "node:path";

export const dynamic = "force-dynamic";

type PushSummary = {
  study_ids: string[];
  journey_rows: number;
  touchpoint_rows: number;
  study_catalog_rows: number;
  taxonomy_rows: number;
  taxonomy_market_rows: number;
  demographic_rows: number;
};

type JourneyRow = {
  study_id?: string | null;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  market_sector?: string | null;
  market_subsector?: string | null;
  market_category?: string | null;
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

function getLegacyApiBaseUrl() {
  const base = process.env.LEGACY_API_BASE_URL || process.env.NEXT_PUBLIC_API_BASE_URL || "";
  if (!base) {
    throw new Error("Missing LEGACY_API_BASE_URL (or NEXT_PUBLIC_API_BASE_URL).");
  }
  return base.replace(/\/+$/, "");
}

function canPush(role: string) {
  return role === "owner" || role === "admin";
}

async function readJsonSafe(response: Response) {
  const text = await response.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { detail: text };
  }
}

async function fetchLegacyJson(pathWithQuery: string) {
  const legacyBase = getLegacyApiBaseUrl();
  const response = await fetch(`${legacyBase}${pathWithQuery}`, { cache: "no-store" });
  const data = await readJsonSafe(response);
  if (!response.ok) {
    throw new Error(`Legacy request failed ${pathWithQuery}: ${JSON.stringify(data)}`);
  }
  return data;
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

function normalizeStudyIds(body: Record<string, unknown> | null) {
  const single = typeof body?.study_id === "string" ? body.study_id : null;
  const list = Array.isArray(body?.study_ids) ? body.study_ids : [];
  const items = new Set<string>();
  if (single && single.trim()) items.add(single.trim());
  for (const value of list) {
    if (typeof value === "string" && value.trim()) items.add(value.trim());
  }
  return Array.from(items);
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
    .replace(/[\u0300-\u036f]/g, "")
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

type StudyTaxonomyResolved = {
  sector: string;
  subsector: string;
  category: string;
  market_sector: string;
  market_subsector: string;
  market_category: string;
  market_source: "rule" | "manual";
};

export async function POST(request: NextRequest) {
  const authz = await getRequestAuthz(request);
  const authRequired = (process.env.BBS_AUTH_MODE || "off").toLowerCase() === "supabase";
  if (authRequired && !authz.user_id) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }
  if (!canPush(authz.role)) {
    return NextResponse.json({ detail: "Forbidden: only owner/admin can push to Supabase." }, { status: 403 });
  }

  const body = (await request.json().catch(() => null)) as Record<string, unknown> | null;
  const studyIds = normalizeStudyIds(body);
  if (studyIds.length === 0) {
    return NextResponse.json({ detail: "Provide study_id or study_ids." }, { status: 400 });
  }

  const created = await supabaseAdminPostgrest("ingestion_jobs", {
    method: "POST",
    headers: { Prefer: "return=representation" },
    body: [
      {
        requested_by: authz.user_id,
        status: "running",
        operation: "push_snapshot",
        payload: { study_ids: studyIds, started_by: authz.email || authz.user_id },
        started_at: new Date().toISOString(),
      },
    ],
  });
  if (!created.response.ok || !Array.isArray(created.data) || !created.data[0]) {
    return NextResponse.json({ detail: "Failed to create push job.", error: created.data }, { status: 500 });
  }
  const pushJob = created.data[0] as { id: string };

  try {
    const studiesParam = encodeURIComponent(studyIds.join(","));

    const [journeyResult, touchpointResult, studiesResult, taxonomyResult, demographicsResult] =
      await Promise.all([
        fetchLegacyJson(`/analytics/journey/table_multi?studies=${studiesParam}&limit_mode=all&sort_by=brand_awareness&sort_dir=desc`),
        fetchLegacyJson(`/analytics/touchpoints/table_multi?studies=${studiesParam}&limit_mode=all&sort_by=recall&sort_dir=desc`),
        fetchLegacyJson("/studies"),
        fetchLegacyJson("/filters/options/taxonomy"),
        fetchLegacyJson(`/filters/options/demographics?study_ids=${studiesParam}`),
      ]);

    const journeyRows = Array.isArray((journeyResult as { rows?: unknown[] }).rows)
      ? ((journeyResult as { rows: JourneyRow[] }).rows || [])
      : [];
    const touchpointRows = Array.isArray((touchpointResult as { rows?: unknown[] }).rows)
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
      return {
        ...row,
        market_sector: row.market_sector || resolved?.market_sector || null,
        market_subsector: row.market_subsector || resolved?.market_subsector || null,
        market_category: row.market_category || resolved?.market_category || null,
      };
    });

    const touchpointRowsWithMarket = touchpointRows.map((row) => {
      const studyId = typeof row.study_id === "string" ? String(row.study_id) : "";
      const resolved = taxonomyByStudyId.get(studyId);
      return {
        ...row,
        market_sector: row.market_sector || resolved?.market_sector || null,
        market_subsector: row.market_subsector || resolved?.market_subsector || null,
        market_category: row.market_category || resolved?.market_category || null,
      };
    });

    const taxonomyRows = Array.isArray((taxonomyResult as { items?: unknown[] }).items)
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

    const summary: PushSummary = {
      study_ids: studyIds,
      journey_rows: journeyRowsWithMarket.length,
      touchpoint_rows: touchpointRowsWithMarket.length,
      study_catalog_rows: studyRows.length,
      taxonomy_rows: taxonomyRows.length,
      taxonomy_market_rows: taxonomyMarketRows.length,
      demographic_rows: demographicRows.length,
    };

    await supabaseAdminPostgrest("ingestion_jobs?id=eq." + pushJob.id, {
      method: "PATCH",
      body: {
        status: "success",
        finished_at: new Date().toISOString(),
        error_message: null,
        payload: { ...summary, pushed_by: authz.email || authz.user_id },
      },
    });

    await supabaseAdminPostgrest("ingestion_job_logs", {
      method: "POST",
      body: [
        {
          job_id: pushJob.id,
          level: "info",
          message: "Snapshot pushed to Supabase.",
          context: summary,
        },
      ],
    });

    return NextResponse.json({ ok: true, job_id: pushJob.id, summary });
  } catch (error) {
    const detail = error instanceof Error ? error.message : "Push failed.";
    await supabaseAdminPostgrest("ingestion_jobs?id=eq." + pushJob.id, {
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
          job_id: pushJob.id,
          level: "error",
          message: "Snapshot push failed.",
          context: { detail },
        },
      ],
    });
    return NextResponse.json({ detail }, { status: 500 });
  }
}
