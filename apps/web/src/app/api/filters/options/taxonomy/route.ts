import { NextRequest, NextResponse } from "next/server";
import { getDataSource, handleWithDataSource } from "../../../_lib/backend";
import { getScopeContext } from "../../../_lib/access-scope";
import { supabaseAdminPostgrest } from "../../../_lib/supabase-admin";
import { resolveMarketLens } from "../../../_lib/market-lens";

export const dynamic = "force-dynamic";

type StudyTaxonomyRow = {
  study_id: string;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  market_sector?: string | null;
  market_subsector?: string | null;
  market_category?: string | null;
};

export async function GET(request: NextRequest) {
  const view = request.nextUrl.searchParams.get("view") === "standard" ? "standard" : "market";
  let taxonomyFields =
    view === "market"
      ? { sector: "market_sector", subsector: "market_subsector", category: "market_category" }
      : { sector: "sector", subsector: "subsector", category: "category" };
  const scopeContext = await getScopeContext(request);
  if (getDataSource() === "supabase") {
    let { response, data } = await supabaseAdminPostgrest(
      "study_catalog?select=study_id,sector,subsector,category,market_sector,market_subsector,market_category"
    );
    if (!response.ok) {
      const fallback = await supabaseAdminPostgrest(
        "study_catalog?select=study_id,sector,subsector,category"
      );
      response = fallback.response;
      data = fallback.data;
    }
    if (!response.ok) {
      const errorDetail =
        (data as { message?: string; details?: string; hint?: string } | null)?.message ||
        (data as { message?: string; details?: string; hint?: string } | null)?.details ||
        "Failed to load taxonomy options.";
      return NextResponse.json({ detail: errorDetail }, { status: response.status || 500 });
    }
    const allowed = scopeContext.allowedStudyIds ? new Set(scopeContext.allowedStudyIds) : null;
    const effectiveRows = (Array.isArray(data) ? data : []) as StudyTaxonomyRow[];
    const projectedRows = effectiveRows.map((row) => {
      if (view !== "market") return row;
      const market = resolveMarketLens({
        sector: row.sector,
        subsector: row.subsector,
        category: row.category,
        market_sector: row.market_sector,
        market_subsector: row.market_subsector,
        market_category: row.market_category,
      });
      return {
        ...row,
        market_sector: market.market_sector,
        market_subsector: market.market_subsector,
        market_category: market.market_category,
      } satisfies StudyTaxonomyRow;
    });
    const rawItems = Array.from(
      new Set(
        projectedRows
          .filter((row) => (allowed ? allowed.has(row.study_id) : true))
          .map(
            (row) =>
              `${String(row[taxonomyFields.sector as keyof StudyTaxonomyRow] || "")}|||${String(
                row[taxonomyFields.subsector as keyof StudyTaxonomyRow] || ""
              )}|||${String(row[taxonomyFields.category as keyof StudyTaxonomyRow] || "")}`
          )
      )
    )
      .map((row) => {
        const [sector, subsector, category] = row.split("|||");
        return { sector, subsector, category };
      })
      .filter((item) => item.sector && item.subsector && item.category)
      .sort((a, b) =>
        `${a.sector}|${a.subsector}|${a.category}`.localeCompare(
          `${b.sector}|${b.subsector}|${b.category}`
        )
      );

    // Viewer should only see assigned scopes (with inheritance by sector/subsector).
    if (scopeContext.authz.is_viewer) {
      const sectorSet = new Set(scopeContext.authz.effective_scopes.market_sector.map((v) => v.toLowerCase()));
      const subsectorSet = new Set(scopeContext.authz.effective_scopes.market_subsector.map((v) => v.toLowerCase()));
      const categorySet = new Set(scopeContext.authz.effective_scopes.market_category.map((v) => v.toLowerCase()));
      const scopedItems = rawItems.filter((item) => {
        const sector = item.sector.toLowerCase();
        const subsector = item.subsector.toLowerCase();
        const category = item.category.toLowerCase();
        return categorySet.has(category) || subsectorSet.has(subsector) || sectorSet.has(sector);
      });
      return NextResponse.json({ items: scopedItems });
    }

    return NextResponse.json({ items: rawItems });
  }

  return handleWithDataSource(
    request,
    `/filters/options/taxonomy?view=${view}`,
    "bbs_filters_options_taxonomy",
    { query: { view }, payload: {} },
    { method: "GET" }
  );
}
