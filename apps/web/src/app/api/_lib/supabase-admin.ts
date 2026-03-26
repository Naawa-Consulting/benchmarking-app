export function getSupabaseAdminConfig() {
  const url = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
  if (!url || !serviceRoleKey) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
  }
  return { url: url.replace(/\/+$/, ""), serviceRoleKey };
}

function withAuthHeaders(serviceRoleKey: string, extra?: Record<string, string>) {
  return {
    apikey: serviceRoleKey,
    Authorization: `Bearer ${serviceRoleKey}`,
    ...extra,
  };
}

export async function supabaseAdminPostgrest(
  path: string,
  init: {
    method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
    body?: unknown;
    headers?: Record<string, string>;
  } = {}
) {
  const { url, serviceRoleKey } = getSupabaseAdminConfig();
  const method = init.method || "GET";
  const response = await fetch(`${url}/rest/v1/${path}`, {
    method,
    headers: withAuthHeaders(serviceRoleKey, {
      "Content-Type": "application/json",
      ...init.headers,
    }),
    body: init.body == null ? undefined : JSON.stringify(init.body),
    cache: "no-store",
  });
  const text = await response.text();
  let data: unknown = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  return { response, data };
}

export async function supabaseAuthAdmin(
  path: string,
  init: {
    method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
    body?: unknown;
    headers?: Record<string, string>;
  } = {}
) {
  const { url, serviceRoleKey } = getSupabaseAdminConfig();
  const method = init.method || "GET";
  const response = await fetch(`${url}/auth/v1/admin/${path.replace(/^\/+/, "")}`, {
    method,
    headers: withAuthHeaders(serviceRoleKey, {
      "Content-Type": "application/json",
      ...init.headers,
    }),
    body: init.body == null ? undefined : JSON.stringify(init.body),
    cache: "no-store",
  });
  const text = await response.text();
  let data: unknown = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  return { response, data };
}

export async function supabaseAdminStorageUpload(
  bucket: string,
  storagePath: string,
  content: ArrayBuffer,
  contentType: string
) {
  const { url, serviceRoleKey } = getSupabaseAdminConfig();
  const response = await fetch(`${url}/storage/v1/object/${bucket}/${storagePath}`, {
    method: "POST",
    headers: withAuthHeaders(serviceRoleKey, {
      "Content-Type": contentType || "application/octet-stream",
      "x-upsert": "false",
    }),
    body: content,
    cache: "no-store",
  });
  const text = await response.text();
  let data: unknown = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }
  return { response, data };
}

export async function supabaseAdminStorageDownload(bucket: string, storagePath: string) {
  const { url, serviceRoleKey } = getSupabaseAdminConfig();
  const response = await fetch(`${url}/storage/v1/object/${bucket}/${storagePath}`, {
    method: "GET",
    headers: withAuthHeaders(serviceRoleKey),
    cache: "no-store",
  });
  return response;
}
