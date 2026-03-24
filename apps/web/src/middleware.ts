import { NextResponse, type NextRequest } from "next/server";
import { isSupabaseAuthEnabled } from "./lib/supabase/config";
import { createClient as createSupabaseMiddlewareClient } from "./utils/supabase/middleware";

async function resolveRoleWithServiceKey(userId: string) {
  const supabaseUrl = process.env.SUPABASE_URL || process.env.NEXT_PUBLIC_SUPABASE_URL || "";
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || "";
  if (!supabaseUrl || !serviceRoleKey) return null;
  const response = await fetch(
    `${supabaseUrl.replace(/\/+$/, "")}/rest/v1/user_roles?select=role&user_id=eq.${encodeURIComponent(
      userId
    )}&limit=1`,
    {
      headers: {
        apikey: serviceRoleKey,
        Authorization: `Bearer ${serviceRoleKey}`,
      },
      cache: "no-store",
    }
  );
  if (!response.ok) return null;
  const rows = (await response.json()) as Array<{ role?: string }>;
  const raw = rows?.[0]?.role;
  const normalized = typeof raw === "string" ? raw.trim().toLowerCase() : "";
  return normalized === "owner" || normalized === "admin" || normalized === "analyst"
    ? normalized
    : "viewer";
}

function isProtectedPath(pathname: string) {
  return (
    pathname === "/" ||
    pathname.startsWith("/journey") ||
    pathname.startsWith("/demand-network") ||
    pathname.startsWith("/tracking") ||
    pathname.startsWith("/data") ||
    pathname.startsWith("/admin") ||
    pathname.startsWith("/api/")
  );
}

export async function middleware(request: NextRequest) {
  if (!isSupabaseAuthEnabled()) {
    return NextResponse.next();
  }

  const pathname = request.nextUrl.pathname;
  const isDataPath = pathname.startsWith("/data");
  const isAdminPath = pathname.startsWith("/admin");
  const isAuthPath = pathname.startsWith("/auth");
  const isAuthResetPath = pathname.startsWith("/auth/reset");
  const needsAuth = isProtectedPath(pathname);
  if (!isAuthPath && !needsAuth) {
    return NextResponse.next();
  }

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL || "";
  const supabaseAnonKey =
    process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY ||
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ||
    "";
  if (!supabaseUrl || !supabaseAnonKey) {
    return NextResponse.json(
      { detail: "Supabase auth is enabled but env vars are missing." },
      { status: 500 }
    );
  }

  const { supabase, response } = createSupabaseMiddlewareClient(request);

  const {
    data: { user },
  } = await supabase.auth.getUser();

  if (!user && needsAuth) {
    const url = request.nextUrl.clone();
    url.pathname = "/auth";
    if (pathname !== "/") {
      url.searchParams.set("next", `${pathname}${request.nextUrl.search || ""}`);
    }
    return NextResponse.redirect(url);
  }

  if (user && isAuthPath && !isAuthResetPath) {
    const target = request.nextUrl.searchParams.get("next") || "/journey";
    return NextResponse.redirect(new URL(target, request.url));
  }

  if (user && (isDataPath || isAdminPath)) {
    const roleFromService = await resolveRoleWithServiceKey(user.id);
    const role = roleFromService ?? "viewer";
    if (role === "viewer" || (isAdminPath && role === "analyst")) {
      const target = request.nextUrl.clone();
      target.pathname = "/journey";
      target.search = "";
      return NextResponse.redirect(target);
    }
  }

  return response;
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
