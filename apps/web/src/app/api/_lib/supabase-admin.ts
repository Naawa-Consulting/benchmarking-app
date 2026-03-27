export function getSupabaseAdminConfig() {
  const url = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
  if (!url || !serviceRoleKey) {
    throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
  }
  return { url: url.replace(/\/+$/, ""), serviceRoleKey };
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetryableFetchError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const message = `${error.name} ${error.message}`.toLowerCase();
  return (
    message.includes("econnreset") ||
    message.includes("etimedout") ||
    message.includes("socket hang up") ||
    message.includes("fetch failed") ||
    message.includes("network")
  );
}

async function fetchWithRetry(
  input: string,
  init: RequestInit,
  attempts = 3
): Promise<Response> {
  let lastError: unknown = null;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    try {
      const response = await fetch(input, { ...init, signal: controller.signal, cache: "no-store" });
      clearTimeout(timeout);
      // Retry transient upstream errors.
      if ((response.status === 502 || response.status === 503 || response.status === 504) && attempt < attempts) {
        await sleep(250 * attempt);
        continue;
      }
      return response;
    } catch (error) {
      clearTimeout(timeout);
      lastError = error;
      if (!isRetryableFetchError(error) || attempt >= attempts) {
        throw error;
      }
      await sleep(250 * attempt);
    }
  }
  throw lastError instanceof Error ? lastError : new Error("Supabase fetch failed");
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
  const response = await fetchWithRetry(`${url}/rest/v1/${path}`, {
    method,
    headers: withAuthHeaders(serviceRoleKey, {
      "Content-Type": "application/json",
      ...init.headers,
    }),
    body: init.body == null ? undefined : JSON.stringify(init.body),
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
  const response = await fetchWithRetry(`${url}/auth/v1/admin/${path.replace(/^\/+/, "")}`, {
    method,
    headers: withAuthHeaders(serviceRoleKey, {
      "Content-Type": "application/json",
      ...init.headers,
    }),
    body: init.body == null ? undefined : JSON.stringify(init.body),
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
  const response = await fetchWithRetry(`${url}/storage/v1/object/${bucket}/${storagePath}`, {
    method: "POST",
    headers: withAuthHeaders(serviceRoleKey, {
      "Content-Type": contentType || "application/octet-stream",
      "x-upsert": "false",
    }),
    body: content,
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
  const response = await fetchWithRetry(`${url}/storage/v1/object/${bucket}/${storagePath}`, {
    method: "GET",
    headers: withAuthHeaders(serviceRoleKey),
  });
  return response;
}
