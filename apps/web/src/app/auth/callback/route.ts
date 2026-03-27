import { createServerClient } from "@supabase/ssr";
import { cookies } from "next/headers";
import { NextRequest, NextResponse } from "next/server";

export async function GET(request: NextRequest) {
  const code = request.nextUrl.searchParams.get("code");
  const tokenHash = request.nextUrl.searchParams.get("token_hash");
  const type = request.nextUrl.searchParams.get("type");
  const isPasswordSetupFlow = type === "recovery" || type === "invite";
  const next = request.nextUrl.searchParams.get("next") || (isPasswordSetupFlow ? "/auth/reset" : "/journey");

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL || "";
  const supabaseAnonKey =
    process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY ||
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ||
    "";
  if (!supabaseUrl || !supabaseAnonKey) {
    return NextResponse.redirect(new URL("/auth", request.url));
  }

  const cookieStore = cookies();
  const response = NextResponse.redirect(new URL(next, request.url));

  const supabase = createServerClient(supabaseUrl, supabaseAnonKey, {
    cookies: {
      getAll() {
        return cookieStore.getAll();
      },
      setAll(cookiesToSet) {
        cookiesToSet.forEach(({ name, value, options }) => {
          response.cookies.set(name, value, options);
        });
      },
    },
  });

  if (tokenHash && type) {
    const otpType = ["signup", "recovery", "invite", "magiclink", "email", "email_change"].includes(type)
      ? (type as "signup" | "recovery" | "invite" | "magiclink" | "email" | "email_change")
      : null;
    if (!otpType) {
      return NextResponse.redirect(new URL("/auth?error=invalid_or_expired_link", request.url));
    }
    const { error } = await supabase.auth.verifyOtp({
      type: otpType,
      token_hash: tokenHash,
    });
    if (error) {
      return NextResponse.redirect(new URL("/auth?error=invalid_or_expired_link", request.url));
    }
  } else if (code) {
    const { error } = await supabase.auth.exchangeCodeForSession(code);
    if (error) {
      return NextResponse.redirect(new URL("/auth?error=invalid_or_expired_link", request.url));
    }
  }

  return response;
}
