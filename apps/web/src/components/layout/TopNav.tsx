"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useMemo, useState } from "react";
import { createSupabaseBrowserClient } from "../../lib/supabase/browser";

type NavItem = {
  label: string;
  href: string;
};

type AuthzMe = {
  role?: "owner" | "admin" | "analyst" | "viewer";
  is_admin_module_allowed?: boolean;
};

const NAV_ITEMS: NavItem[] = [
  { label: "Journey", href: "/journey" },
  { label: "Network", href: "/demand-network" },
  { label: "Trends", href: "/tracking" },
  { label: "Data", href: "/data" },
  { label: "Admin", href: "/admin" },
];

function NavTab({ item, active }: { item: NavItem; active: boolean }) {
  return (
    <Link
      href={item.href}
      className={`inline-flex h-9 shrink-0 items-center rounded-full px-4 text-sm font-medium leading-none transition-colors ${
        active
          ? "bg-white text-ink shadow-sm ring-1 ring-ink/10"
          : "text-slate hover:bg-white/75 hover:text-ink"
      } focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-200`}
      aria-current={active ? "page" : undefined}
    >
      {item.label}
    </Link>
  );
}

export default function TopNav() {
  const pathname = usePathname();
  const router = useRouter();
  const authEnabled = (process.env.NEXT_PUBLIC_BBS_AUTH_MODE || "off").toLowerCase() === "supabase";
  const [role, setRole] = useState<AuthzMe["role"]>();
  const [loggingOut, setLoggingOut] = useState(false);

  useEffect(() => {
    if (!authEnabled) {
      setRole(undefined);
      return;
    }
    let active = true;
    const loadRole = async () => {
      for (let i = 0; i < 3; i += 1) {
        try {
          const response = await fetch("/api/auth/me", { cache: "no-store" });
          if (!response.ok) throw new Error(`auth me failed ${response.status}`);
          const data = (await response.json()) as AuthzMe;
          if (active && data?.role) {
            setRole(data.role);
            return;
          }
        } catch {
          // Retry a couple of times to avoid session propagation race on fresh login.
        }
        await new Promise((resolve) => setTimeout(resolve, 250 * (i + 1)));
      }
      if (active) setRole(undefined);
    };
    loadRole();
    return () => {
      active = false;
    };
  }, [authEnabled]);

  const navItems = useMemo(() => {
    if (!authEnabled) return NAV_ITEMS;
    if (!role) return NAV_ITEMS.filter((item) => item.href !== "/data" && item.href !== "/admin");
    if (role === "viewer") return NAV_ITEMS.filter((item) => item.href !== "/data" && item.href !== "/admin");
    if (role === "analyst") return NAV_ITEMS.filter((item) => item.href !== "/admin");
    return NAV_ITEMS;
  }, [authEnabled, role]);

  async function handleLogout() {
    if (!authEnabled || loggingOut) return;
    setLoggingOut(true);
    try {
      const supabase = createSupabaseBrowserClient();
      await supabase.auth.signOut();
      setRole(undefined);
      router.replace("/auth");
      router.refresh();
    } finally {
      setLoggingOut(false);
    }
  }

  return (
    <header className="fixed inset-x-0 top-0 z-40 border-b border-ink/10 bg-[#f7f8fa]/95 backdrop-blur">
      <div className="mx-auto flex h-[68px] w-full max-w-[1800px] items-center gap-4 px-4 sm:px-6 lg:px-8">
        <Link href="/journey" className="min-w-0 shrink-0 leading-tight">
          <p className="text-[10px] font-semibold uppercase tracking-[0.24em] text-slate">BBS</p>
          <p className="truncate text-sm font-semibold text-ink">Brand Benchmark Suite</p>
        </Link>

        <nav className="flex min-w-0 flex-1 items-center gap-1 overflow-x-auto overflow-y-visible py-1">
          {navItems.map((item) => {
            const active =
              item.href === "/data"
                ? pathname?.startsWith("/data")
                : item.href === "/admin"
                  ? pathname?.startsWith("/admin")
                : pathname?.startsWith(item.href);
            return <NavTab key={item.href} item={item} active={Boolean(active)} />;
          })}
        </nav>

        <div className="hidden shrink-0 items-center gap-2 md:flex">
          <button
            type="button"
            className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-ink/10 bg-white text-sm text-slate shadow-sm transition hover:text-ink"
            aria-label="Help"
            title="Help"
          >
            ?
          </button>
          {authEnabled ? (
            <button
              type="button"
              onClick={handleLogout}
              disabled={loggingOut}
              className="inline-flex h-9 items-center justify-center rounded-full border border-ink/10 bg-white px-3 text-xs font-semibold text-slate shadow-sm transition hover:text-ink disabled:opacity-60"
              aria-label="Logout"
              title="Logout"
            >
              {loggingOut ? "Signing out..." : "Logout"}
            </button>
          ) : (
            <button
              type="button"
              className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-ink/10 bg-white text-xs font-semibold text-slate shadow-sm transition hover:text-ink"
              aria-label="User"
              title="User"
            >
              U
            </button>
          )}
        </div>
      </div>
    </header>
  );
}
