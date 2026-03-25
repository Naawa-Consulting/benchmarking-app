import { NextRequest } from "next/server";
import { handleWithDataSource } from "../../../_lib/backend";
import { getScopeContext, scopeStudyIdsCsv } from "../../../_lib/access-scope";
import { collapseNseOptions } from "../../../_lib/demographics";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const scopeContext = await getScopeContext(request);
  const queryObj = Object.fromEntries(request.nextUrl.searchParams.entries());
  if (scopeContext.allowedStudyIds !== null) {
    const scopedCsv = scopeStudyIdsCsv(queryObj.study_ids || queryObj.studies, scopeContext.allowedStudyIds);
    queryObj.study_ids = scopedCsv || "";
    queryObj.studies = scopedCsv || "";
  }
  const queryString = new URLSearchParams(queryObj).toString();
  const query = queryString ? `?${queryString}` : "";
  const response = await handleWithDataSource(
    request,
    `/filters/options/demographics${query}`,
    "bbs_filters_options_demographics",
    {
      query: queryObj,
      payload: {},
    },
    { method: "GET" }
  );
  if (!response.ok) return response;
  const payload = (await response.json().catch(() => null)) as Record<string, unknown> | null;
  const nse = collapseNseOptions(payload?.nse);
  const normalized = {
    gender: Array.isArray(payload?.gender) ? payload?.gender : [],
    nse,
    state: [],
    age:
      payload && typeof payload.age === "object" && payload.age
        ? payload.age
        : { min: null, max: null },
  };
  return Response.json(normalized, { status: response.status });
}
