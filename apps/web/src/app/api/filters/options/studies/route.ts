import { NextRequest, NextResponse } from "next/server";
import { getDataSource, handleWithDataSource } from "../../../_lib/backend";
import { supabaseAdminPostgrest } from "../../../_lib/supabase-admin";
import { getScopeContext } from "../../../_lib/access-scope";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const scopeContext = await getScopeContext(request);

  if (getDataSource() === "supabase") {
    const { response, data } = await supabaseAdminPostgrest(
      "study_catalog?select=study_id,study_name,sector,subsector,category,has_demographics,has_date&order=study_id.asc"
    );
    if (!response.ok) {
      return NextResponse.json(
        { detail: "Failed to load study options from Supabase.", error: data },
        { status: response.status || 500 }
      );
    }
    const items = Array.isArray(data) ? data : [];
    if (scopeContext.allowedStudyIds !== null) {
      const allowed = new Set(scopeContext.allowedStudyIds);
      return NextResponse.json({
        items: items.filter((item) => allowed.has(String((item as { study_id?: string }).study_id || ""))),
      });
    }
    return NextResponse.json({ items });
  }

  const response = await handleWithDataSource(
    request,
    "/filters/options/studies",
    "bbs_filters_options_studies",
    { query: {}, payload: {} },
    { method: "GET" }
  );

  if (scopeContext.allowedStudyIds === null) {
    return response;
  }

  const payload = await response.json().catch(() => ({}));
  const items = Array.isArray(payload?.items) ? payload.items : [];
  const allowed = new Set(scopeContext.allowedStudyIds);
  return NextResponse.json({
    items: items.filter((item: { study_id?: string }) => allowed.has(String(item?.study_id || ""))),
  });
}
