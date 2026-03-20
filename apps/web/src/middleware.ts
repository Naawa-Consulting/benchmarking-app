import { NextResponse, type NextRequest } from "next/server";
import { isSupabaseAuthEnabled } from "./lib/supabase/config";
import { createClient as createSupabaseMiddlewareClient } from "./utils/supabase/middleware";

function isProtectedPath(pathname: string) {
  return (
    pathname === "/" ||
    pathname.startsWith("/journey") ||
    pathname.startsWith("/demand-network") ||
    pathname.startsWith("/tracking") ||
    pathname.startsWith("/admin") ||
    pathname.startsWith("/api/")
  );
}

export async function middleware(request: NextRequest) {
  if (!isSupabaseAuthEnabled()) {
    return NextResponse.next();
  }

  const pathname = request.nextUrl.pathname;
  const isAuthPath = pathname.startsWith("/auth");
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

  if (user && isAuthPath) {
    const target = request.nextUrl.searchParams.get("next") || "/journey";
    return NextResponse.redirect(new URL(target, request.url));
  }

  return response;
}

export const config = {
  matcher: ["/((?!_next/static|_next/image|favicon.ico).*)"],
};
