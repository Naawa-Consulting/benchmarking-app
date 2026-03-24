import { NextRequest, NextResponse } from "next/server";
import { handleWithDataSource } from "../../../_lib/backend";
import { getScopeContext, parseStudyIdsInput, scopeStudyIds, scopeStudyIdsCsv } from "../../../_lib/access-scope";

export const dynamic = "force-dynamic";

function emptyTrackingSeries() {
  return {
    ok: true,
    periods: [],
    entity_rows: [],
    secondary_rows: [],
    delta_columns: [],
    brand_rows: [],
    touchpoint_rows: [],
    meta: { source: "supabase", warning: "No studies allowed for current user scope." },
  };
}

function withStudyScope(
  query: Record<string, string>,
  payload: Record<string, unknown>,
  allowedStudyIds: string[] | null
) {
  if (allowedStudyIds === null) return { query, payload };
  const scopedFromQuery = scopeStudyIdsCsv(query.studies || query.study_ids, allowedStudyIds);
  const scopedFromPayload = scopeStudyIds(parseStudyIdsInput(payload.study_ids), allowedStudyIds);
  const merged = scopedFromPayload ?? (scopedFromQuery ? scopedFromQuery.split(",") : []);
  return {
    query: {
      ...query,
      studies: merged.join(","),
      study_ids: merged.join(","),
    },
    payload: {
      ...payload,
      study_ids: merged,
    },
  };
}

export async function GET(request: NextRequest) {
  const scopeContext = await getScopeContext(request);
  if (scopeContext.allowedStudyIds && scopeContext.allowedStudyIds.length === 0) {
    return NextResponse.json(emptyTrackingSeries());
  }
  const scoped = withStudyScope(
    Object.fromEntries(request.nextUrl.searchParams.entries()),
    {},
    scopeContext.allowedStudyIds
  );
  const queryString = new URLSearchParams(scoped.query).toString();
  const query = queryString ? `?${queryString}` : "";
  return handleWithDataSource(
    request,
    `/analytics/tracking/series${query}`,
    "bbs_tracking_series",
    {
      query: scoped.query,
      payload: scoped.payload,
    },
    { method: "GET" }
  );
}

export async function POST(request: NextRequest) {
  const scopeContext = await getScopeContext(request);
  if (scopeContext.allowedStudyIds && scopeContext.allowedStudyIds.length === 0) {
    return NextResponse.json(emptyTrackingSeries());
  }
  const payload = (await request.json().catch(() => ({}))) as Record<string, unknown>;
  const scoped = withStudyScope(
    Object.fromEntries(request.nextUrl.searchParams.entries()),
    payload,
    scopeContext.allowedStudyIds
  );
  const queryString = new URLSearchParams(scoped.query).toString();
  const query = queryString ? `?${queryString}` : "";
  return handleWithDataSource(
    request,
    `/analytics/tracking/series${query}`,
    "bbs_tracking_series",
    {
      query: scoped.query,
      payload: scoped.payload,
    },
    { method: "POST" }
  );
}
