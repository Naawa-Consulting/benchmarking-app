import { NextRequest } from "next/server";
import { handleWithDataSource } from "../../../_lib/backend";

export async function GET(request: NextRequest) {
  const query = request.nextUrl.search || "";
  return handleWithDataSource(
    request,
    `/analytics/journey/table_multi${query}`,
    "bbs_journey_table_multi",
    {
      query: Object.fromEntries(request.nextUrl.searchParams.entries()),
      payload: {},
    },
    { method: "GET" }
  );
}

export async function POST(request: NextRequest) {
  const query = request.nextUrl.search || "";
  const payload = await request.json().catch(() => ({}));
  return handleWithDataSource(
    request,
    `/analytics/journey/table_multi${query}`,
    "bbs_journey_table_multi",
    {
      query: Object.fromEntries(request.nextUrl.searchParams.entries()),
      payload,
    },
    { method: "POST" }
  );
}
