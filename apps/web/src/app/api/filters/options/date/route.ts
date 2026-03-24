import { NextRequest } from "next/server";
import { handleWithDataSource } from "../../../_lib/backend";
import { getScopeContext, scopeStudyIdsCsv } from "../../../_lib/access-scope";

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
  return handleWithDataSource(
    request,
    `/filters/options/date${query}`,
    "bbs_filters_options_date",
    {
      query: queryObj,
      payload: {},
    },
    { method: "GET" }
  );
}
