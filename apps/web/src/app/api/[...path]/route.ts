import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz, isMutatingDataPath } from "../_lib/authz";
import { forwardLegacy, getDataSource } from "../_lib/backend";

// Admin/pipeline endpoints have no Supabase RPC equivalent — they always live on the
// FastAPI backend regardless of BBS_DATA_SOURCE, since they operate on the ingestion
// pipeline (raw .sav -> mapping -> rules -> curated), not on the published Postgres
// tables Journey/Network/Tracking read from.
const ALWAYS_LEGACY_PREFIXES = [
  "/mapping",
  "/marts",
  "/rules",
  "/questions",
  "/question-map",
  "/study-config",
  "/studies",
  "/study/",
  "/ingest",
  "/pipeline",
  "/taxonomy",
  "/demographics",
  "/health",
];

function resolvePath(request: NextRequest, path: string[]) {
  const pathname = `/${path.join("/")}`;
  const query = request.nextUrl.search || "";
  return `${pathname}${query}`;
}

function isAlwaysLegacyPath(pathname: string) {
  return ALWAYS_LEGACY_PREFIXES.some((prefix) => pathname === prefix || pathname.startsWith(prefix));
}

function unsupported(pathname: string) {
  return NextResponse.json(
    {
      detail: `Unsupported API path in supabase mode: ${pathname}`,
    },
    { status: 404 }
  );
}

export async function GET(
  request: NextRequest,
  context: { params: { path: string[] } }
) {
  const pathWithQuery = resolvePath(request, context.params.path || []);
  const pathname = pathWithQuery.split("?")[0];
  if (getDataSource() === "legacy" || isAlwaysLegacyPath(pathname)) {
    return forwardLegacy(request, pathWithQuery, { method: "GET" });
  }
  return unsupported(pathWithQuery);
}

export async function POST(
  request: NextRequest,
  context: { params: { path: string[] } }
) {
  const pathWithQuery = resolvePath(request, context.params.path || []);
  const pathname = pathWithQuery.split("?")[0];
  if (getDataSource() === "legacy" || isAlwaysLegacyPath(pathname)) {
    const authz = await getRequestAuthz(request);
    if (authz.is_viewer && isMutatingDataPath(pathWithQuery)) {
      return NextResponse.json({ detail: "Forbidden: insufficient permissions" }, { status: 403 });
    }
    return forwardLegacy(request, pathWithQuery, { method: "POST" });
  }
  return unsupported(pathWithQuery);
}
