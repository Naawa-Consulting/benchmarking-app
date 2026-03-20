"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { createSupabaseBrowserClient } from "../../lib/supabase/browser";

export default function AuthPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [email, setEmail] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [enabled, setEnabled] = useState(true);

  const nextPath = useMemo(() => searchParams.get("next") || "/journey", [searchParams]);

  useEffect(() => {
    setEnabled((process.env.NEXT_PUBLIC_BBS_AUTH_MODE || "off").toLowerCase() === "supabase");
  }, []);

  useEffect(() => {
    if (!enabled) return;
    const supabase = createSupabaseBrowserClient();
    supabase.auth.getUser().then(({ data }) => {
      if (data.user) router.replace(nextPath);
    });
  }, [enabled, nextPath, router]);

  async function handleSubmit(event: FormEvent) {
    event.preventDefault();
    setError(null);
    setMessage(null);
    if (!email.trim()) {
      setError("Enter your email.");
      return;
    }
    setLoading(true);
    try {
      const supabase = createSupabaseBrowserClient();
      const redirectTo = `${window.location.origin}/auth/callback?next=${encodeURIComponent(nextPath)}`;
      const { error: signInError } = await supabase.auth.signInWithOtp({
        email: email.trim(),
        options: { emailRedirectTo: redirectTo },
      });
      if (signInError) {
        setError(signInError.message);
      } else {
        setMessage("Magic link sent. Check your email.");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to start sign in.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="mx-auto mt-16 max-w-md rounded-2xl border border-ink/10 bg-white p-6 shadow-sm">
      <h1 className="text-xl font-semibold text-ink">BBS Sign in</h1>
      <p className="mt-1 text-sm text-slate">Sign in with Supabase magic link.</p>

      {!enabled ? (
        <p className="mt-4 rounded-lg border border-amber-300/60 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          Auth mode is disabled. Set `BBS_AUTH_MODE=supabase` and `NEXT_PUBLIC_BBS_AUTH_MODE=supabase`.
        </p>
      ) : (
        <form className="mt-5 space-y-3" onSubmit={handleSubmit}>
          <input
            type="email"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            placeholder="you@company.com"
            className="w-full rounded-xl border border-ink/10 px-3 py-2 text-sm outline-none ring-emerald-200 focus:ring"
          />
          <button
            type="submit"
            disabled={loading}
            className="w-full rounded-xl bg-ink px-3 py-2 text-sm font-medium text-white disabled:opacity-60"
          >
            {loading ? "Sending..." : "Send magic link"}
          </button>
          {message ? <p className="text-sm text-emerald-700">{message}</p> : null}
          {error ? <p className="text-sm text-rose-600">{error}</p> : null}
        </form>
      )}
    </div>
  );
}
