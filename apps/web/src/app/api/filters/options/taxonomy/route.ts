import { NextRequest } from "next/server";
import { handleWithDataSource } from "../../../_lib/backend";

export async function GET(request: NextRequest) {
  return handleWithDataSource(
    request,
    "/filters/options/taxonomy",
    "bbs_filters_options_taxonomy",
    { query: {}, payload: {} },
    { method: "GET" }
  );
}
