import { NextRequest, NextResponse } from "next/server";
import { handleWithDataSource } from "../_lib/backend";
import { getScopeContext, scopeStudyIdsCsv } from "../_lib/access-scope";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const scopeContext = await getScopeContext(request);
  if (scopeContext.allowedStudyIds && scopeContext.allowedStudyIds.length === 0) {
    return NextResponse.json({
      ok: true,
      metric: "recall",
      filters: {},
      nodes: [],
      links: [],
      meta: { source: "supabase", warning: "No studies allowed for current user scope." },
    });
  }

  const queryObj = Object.fromEntries(request.nextUrl.searchParams.entries());
  if (scopeContext.allowedStudyIds !== null) {
    const scopedCsv = scopeStudyIdsCsv(queryObj.studies || queryObj.study_ids, scopeContext.allowedStudyIds);
    queryObj.studies = scopedCsv || "";
    queryObj.study_ids = scopedCsv || "";
  }
  if (!scopeContext.authz.can_toggle_brands) {
    queryObj.brands = "";
    queryObj.brands_mode = "disable";
    queryObj.network_brands = "disable";
  }
  const queryString = new URLSearchParams(queryObj).toString();
  const query = queryString ? `?${queryString}` : "";
  return handleWithDataSource(
    request,
    `/network${query}`,
    "bbs_network",
    {
      query: queryObj,
      payload: {},
    },
    { method: "GET" }
  );
}
