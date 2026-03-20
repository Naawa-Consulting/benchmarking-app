import { NextRequest } from "next/server";
import { handleWithDataSource } from "../../../_lib/backend";

export async function GET(request: NextRequest) {
  const query = request.nextUrl.search || "";
  return handleWithDataSource(
    request,
    `/filters/options/demographics${query}`,
    "bbs_filters_options_demographics",
    {
      query: Object.fromEntries(request.nextUrl.searchParams.entries()),
      payload: {},
    },
    { method: "GET" }
  );
}
