import { NextRequest } from "next/server";
import { handleWithDataSource } from "../../../_lib/backend";

export async function GET(request: NextRequest) {
  return handleWithDataSource(
    request,
    "/filters/options/studies",
    "bbs_filters_options_studies",
    { query: {}, payload: {} },
    { method: "GET" }
  );
}
