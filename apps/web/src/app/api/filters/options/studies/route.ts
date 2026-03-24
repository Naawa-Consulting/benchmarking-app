import { NextRequest, NextResponse } from "next/server";
import { getDataSource, handleWithDataSource } from "../../../_lib/backend";
import { supabaseAdminPostgrest } from "../../../_lib/supabase-admin";
import { getScopeContext } from "../../../_lib/access-scope";
import { resolveMarketLens } from "../../../_lib/market-lens";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const scopeContext = await getScopeContext(request);

  if (getDataSource() === "supabase") {
    let { response, data } = await supabaseAdminPostgrest(
      "study_catalog?select=study_id,study_name,sector,subsector,category,market_sector,market_subsector,market_category,market_source,has_demographics,has_date&order=study_id.asc"
    );
    if (!response.ok) {
      const fallback = await supabaseAdminPostgrest(
        "study_catalog?select=study_id,study_name,sector,subsector,category,has_demographics,has_date&order=study_id.asc"
      );
      response = fallback.response;
      data = Array.isArray(fallback.data)
        ? (fallback.data as Array<Record<string, unknown>>).map((row) => ({
            ...row,
            market_sector: row.sector ?? null,
            market_subsector: row.subsector ?? null,
            market_category: row.category ?? null,
            market_source: "rule",
          }))
        : fallback.data;
    }
    if (!response.ok) {
      return NextResponse.json(
        { detail: "Failed to load study options from Supabase.", error: data },
        { status: response.status || 500 }
      );
    }
    const items = Array.isArray(data) ? data : [];
    const normalizedItems = items.map((item) => {
      const row = item as Record<string, unknown>;
      const market = resolveMarketLens({
        sector: typeof row.sector === "string" ? row.sector : null,
        subsector: typeof row.subsector === "string" ? row.subsector : null,
        category: typeof row.category === "string" ? row.category : null,
        market_sector: typeof row.market_sector === "string" ? row.market_sector : null,
        market_subsector: typeof row.market_subsector === "string" ? row.market_subsector : null,
        market_category: typeof row.market_category === "string" ? row.market_category : null,
      });
      return {
        ...row,
        market_sector: market.market_sector,
        market_subsector: market.market_subsector,
        market_category: market.market_category,
        market_source: row.market_source ?? "rule",
      };
    });
    if (scopeContext.allowedStudyIds !== null) {
      const allowed = new Set(scopeContext.allowedStudyIds);
      return NextResponse.json({
        items: normalizedItems.filter((item) =>
          allowed.has(String((item as { study_id?: string }).study_id || ""))
        ),
      });
    }
    return NextResponse.json({ items: normalizedItems });
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
