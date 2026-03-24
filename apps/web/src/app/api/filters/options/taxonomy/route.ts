import { NextRequest, NextResponse } from "next/server";
import { handleWithDataSource } from "../../../_lib/backend";
import { getScopeContext } from "../../../_lib/access-scope";
import { supabaseAdminPostgrest } from "../../../_lib/supabase-admin";

export const dynamic = "force-dynamic";

type StudyTaxonomyRow = {
  study_id: string;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
};

export async function GET(request: NextRequest) {
  const scopeContext = await getScopeContext(request);
  if (scopeContext.allowedStudyIds !== null) {
    const { response, data } = await supabaseAdminPostgrest(
      "study_catalog?select=study_id,sector,subsector,category"
    );
    if (!response.ok) {
      return NextResponse.json({ detail: "Failed to load taxonomy options." }, { status: 500 });
    }
    const allowed = new Set(scopeContext.allowedStudyIds);
    const rows = (Array.isArray(data) ? data : []) as StudyTaxonomyRow[];
    const rawItems = Array.from(
      new Set(
        rows
          .filter((row) => allowed.has(row.study_id))
          .map((row) => `${row.sector || ""}|||${row.subsector || ""}|||${row.category || ""}`)
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
      const sectorSet = new Set(scopeContext.authz.effective_scopes.sector.map((v) => v.toLowerCase()));
      const subsectorSet = new Set(scopeContext.authz.effective_scopes.subsector.map((v) => v.toLowerCase()));
      const categorySet = new Set(scopeContext.authz.effective_scopes.category.map((v) => v.toLowerCase()));
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
    "/filters/options/taxonomy",
    "bbs_filters_options_taxonomy",
    { query: {}, payload: {} },
    { method: "GET" }
  );
}
