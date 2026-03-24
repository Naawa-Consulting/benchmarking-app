import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz, isMutatingDataPath } from "../_lib/authz";
import { forwardLegacy, getDataSource } from "../_lib/backend";

function resolvePath(request: NextRequest, path: string[]) {
  const pathname = `/${path.join("/")}`;
  const query = request.nextUrl.search || "";
  return `${pathname}${query}`;
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
  if (getDataSource() === "legacy") {
    return forwardLegacy(request, pathWithQuery, { method: "GET" });
  }
  return unsupported(pathWithQuery);
}

export async function POST(
  request: NextRequest,
  context: { params: { path: string[] } }
) {
  const pathWithQuery = resolvePath(request, context.params.path || []);
  if (getDataSource() === "legacy") {
    const authz = await getRequestAuthz(request);
    if (authz.is_viewer && isMutatingDataPath(pathWithQuery)) {
      return NextResponse.json({ detail: "Forbidden: insufficient permissions" }, { status: 403 });
    }
    return forwardLegacy(request, pathWithQuery, { method: "POST" });
  }
  return unsupported(pathWithQuery);
}
