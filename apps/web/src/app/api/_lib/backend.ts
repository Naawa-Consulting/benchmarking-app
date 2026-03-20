import { NextRequest, NextResponse } from "next/server";

export type DataSource = "legacy" | "supabase";

export function getDataSource(): DataSource {
  const raw = (process.env.BBS_DATA_SOURCE || "legacy").toLowerCase();
  return raw === "supabase" ? "supabase" : "legacy";
}

function getLegacyBaseUrl() {
  return (
    process.env.LEGACY_API_BASE_URL ||
    process.env.NEXT_PUBLIC_API_BASE_URL ||
    "http://localhost:8000"
  ).replace(/\/+$/, "");
}

function ensureSupabaseConfig() {
  const url = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !serviceRole) {
    throw new Error("Supabase is not configured. Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
  }
  return { url: url.replace(/\/+$/, ""), serviceRole };
}

async function readJsonSafe(response: Response) {
  const text = await response.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return { detail: text };
  }
}

export async function forwardLegacy(
  request: NextRequest,
  pathWithQuery: string,
  options?: { method?: "GET" | "POST" }
) {
  const base = getLegacyBaseUrl();
  const method = options?.method || request.method || "GET";
  const headers: Record<string, string> = {};
  const contentType = request.headers.get("content-type");
  if (contentType) headers["content-type"] = contentType;
  const body = method === "POST" ? await request.text() : undefined;
  const response = await fetch(`${base}${pathWithQuery}`, {
    method,
    headers,
    body,
    cache: "no-store",
  });
  const data = await readJsonSafe(response);
  return NextResponse.json(data, { status: response.status });
}

export async function callSupabaseRpc(functionName: string, payload: Record<string, unknown>) {
  const { url, serviceRole } = ensureSupabaseConfig();
  const response = await fetch(`${url}/rest/v1/rpc/${functionName}`, {
    method: "POST",
    headers: {
      apikey: serviceRole,
      Authorization: `Bearer ${serviceRole}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
    cache: "no-store",
  });
  const data = await readJsonSafe(response);
  return { response, data };
}

export async function handleWithDataSource(
  request: NextRequest,
  pathWithQuery: string,
  supabaseFn: string,
  supabasePayload: Record<string, unknown>,
  options?: { method?: "GET" | "POST" }
) {
  if (getDataSource() === "legacy") {
    return forwardLegacy(request, pathWithQuery, options);
  }

  try {
    const { response, data } = await callSupabaseRpc(supabaseFn, supabasePayload);
    if (!response.ok) {
      return NextResponse.json(
        {
          detail:
            (data as { message?: string; detail?: string } | null)?.message ||
            (data as { message?: string; detail?: string } | null)?.detail ||
            `Supabase RPC ${supabaseFn} failed`,
        },
        { status: response.status || 500 }
      );
    }
    return NextResponse.json(data);
  } catch (error) {
    const detail = error instanceof Error ? error.message : "Unexpected data source error";
    return NextResponse.json({ detail }, { status: 500 });
  }
}
