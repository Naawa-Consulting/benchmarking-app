import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../_lib/authz";
import { supabaseAdminPostgrest } from "../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

type PushSummary = {
  study_ids: string[];
  journey_rows: number;
  touchpoint_rows: number;
  study_catalog_rows: number;
  taxonomy_rows: number;
  demographic_rows: number;
};

type JourneyRow = {
  study_id?: string | null;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
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

export async function POST(request: NextRequest) {
  const authz = await getRequestAuthz(request);
  if (!authz.user_id) {
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
        return {
          study_id: studyId,
          study_name: row.name || studyId,
          sector: taxonomy?.sector ?? row.sector ?? null,
          subsector: taxonomy?.subsector ?? row.subsector ?? null,
          category: taxonomy?.category ?? row.category ?? null,
          has_demographics: true,
          has_date: true,
        };
      });

    const taxonomyRows = Array.isArray((taxonomyResult as { items?: unknown[] }).items)
      ? (((taxonomyResult as { items: Record<string, unknown>[] }).items || []).map((item) => ({
          sector: item.sector,
          subsector: item.subsector,
          category: item.category,
        })) as Record<string, unknown>[])
      : [];

    const demographicRows = deriveDemographicRows(demographicsResult as Record<string, unknown>);

    // Replace snapshot rows for selected studies to avoid mixed taxonomy/history artifacts.
    await Promise.all([
      deleteRowsByStudyIds("journey_metrics", studyIds),
      deleteRowsByStudyIds("touchpoint_metrics", studyIds),
      deleteRowsByStudyIds("study_catalog", studyIds),
    ]);

    await Promise.all([
      upsertRows("journey_metrics", journeyRows, "study_id,sector,subsector,category,brand"),
      upsertRows("touchpoint_metrics", touchpointRows, "study_id,sector,subsector,category,brand,touchpoint"),
      upsertRows("study_catalog", studyRows, "study_id"),
      upsertRows("taxonomy", taxonomyRows, "sector,subsector,category"),
      upsertRows("demographic_options", demographicRows, "gender,nse,state,age_min,age_max"),
    ]);

    const summary: PushSummary = {
      study_ids: studyIds,
      journey_rows: journeyRows.length,
      touchpoint_rows: touchpointRows.length,
      study_catalog_rows: studyRows.length,
      taxonomy_rows: taxonomyRows.length,
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
