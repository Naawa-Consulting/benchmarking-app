"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { createSupabaseBrowserClient } from "../../lib/supabase/browser";

export default function AuthPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loadingLogin, setLoadingLogin] = useState(false);
  const [loadingRecovery, setLoadingRecovery] = useState(false);
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

  async function handleLogin(event: FormEvent) {
    event.preventDefault();
    setError(null);
    setMessage(null);
    if (!email.trim() || !password.trim()) {
      setError("Enter your email and password.");
      return;
    }
    setLoadingLogin(true);
    try {
      const supabase = createSupabaseBrowserClient();
      const { error: signInError } = await supabase.auth.signInWithPassword({
        email: email.trim(),
        password,
      });
      if (signInError) {
        setError(signInError.message);
      } else {
        router.replace(nextPath);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to sign in.");
    } finally {
      setLoadingLogin(false);
    }
  }

  async function handleSendRecovery() {
    setError(null);
    setMessage(null);
    if (!email.trim()) {
      setError("Enter your email to send password recovery link.");
      return;
    }
    setLoadingRecovery(true);
    try {
      const supabase = createSupabaseBrowserClient();
      const redirectTo = `${window.location.origin}/auth/reset?next=${encodeURIComponent(nextPath)}`;
      const { error: recoveryError } = await supabase.auth.resetPasswordForEmail(email.trim(), {
        redirectTo,
      });
      if (recoveryError) {
        setError(recoveryError.message);
      } else {
        setMessage("Recovery link sent. Check your email.");
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unable to send recovery link.");
    } finally {
      setLoadingRecovery(false);
    }
  }

  return (
    <div className="mx-auto mt-16 max-w-md rounded-2xl border border-ink/10 bg-white p-6 shadow-sm">
      <h1 className="text-xl font-semibold text-ink">BBS Sign in</h1>
      <p className="mt-1 text-sm text-slate">Sign in with email and password.</p>

      {!enabled ? (
        <p className="mt-4 rounded-lg border border-amber-300/60 bg-amber-50 px-3 py-2 text-sm text-amber-800">
          Auth mode is disabled. Set `BBS_AUTH_MODE=supabase` and `NEXT_PUBLIC_BBS_AUTH_MODE=supabase`.
        </p>
      ) : (
        <form className="mt-5 space-y-3" onSubmit={handleLogin}>
          <input
            type="email"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
            placeholder="you@company.com"
            className="w-full rounded-xl border border-ink/10 px-3 py-2 text-sm outline-none ring-emerald-200 focus:ring"
          />
          <input
            type="password"
            value={password}
            onChange={(event) => setPassword(event.target.value)}
            placeholder="Password"
            className="w-full rounded-xl border border-ink/10 px-3 py-2 text-sm outline-none ring-emerald-200 focus:ring"
          />
          <button
            type="submit"
            disabled={loadingLogin}
            className="w-full rounded-xl bg-ink px-3 py-2 text-sm font-medium text-white disabled:opacity-60"
          >
            {loadingLogin ? "Signing in..." : "Sign in"}
          </button>
          <button
            type="button"
            disabled={loadingRecovery}
            onClick={handleSendRecovery}
            className="w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm font-medium text-ink disabled:opacity-60"
          >
            {loadingRecovery ? "Sending..." : "Recover password"}
          </button>
          <p className="text-xs text-slate">
            First-time users should use the invitation email. Use this only for password recovery.
          </p>
          {message ? <p className="text-sm text-emerald-700">{message}</p> : null}
          {error ? <p className="text-sm text-rose-600">{error}</p> : null}
        </form>
      )}
    </div>
  );
}
