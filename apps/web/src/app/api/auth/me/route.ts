import { NextRequest, NextResponse } from "next/server";
import { getRequestAuthz } from "../../_lib/authz";

export const dynamic = "force-dynamic";

export async function GET(request: NextRequest) {
  const authz = await getRequestAuthz(request);
  return NextResponse.json(authz);
}
