export function isSupabaseAuthEnabled() {
  return (process.env.BBS_AUTH_MODE || "off").toLowerCase() === "supabase";
}

export function getSupabaseClientEnv() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL || "";
  const anonKey =
    process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_DEFAULT_KEY ||
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY ||
    "";
  return { url, anonKey };
}
