"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

type NavItem = {
  label: string;
  href: string;
};

const NAV_ITEMS: NavItem[] = [
  { label: "Journey", href: "/journey" },
  { label: "Network", href: "/demand-network" },
  { label: "Trends", href: "/tracking" },
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

  return (
    <header className="fixed inset-x-0 top-0 z-40 border-b border-ink/10 bg-[#f7f8fa]/95 backdrop-blur">
      <div className="mx-auto flex h-[68px] w-full max-w-[1800px] items-center gap-4 px-4 sm:px-6 lg:px-8">
        <Link href="/journey" className="min-w-0 shrink-0 leading-tight">
          <p className="text-[10px] font-semibold uppercase tracking-[0.24em] text-slate">BBS</p>
          <p className="truncate text-sm font-semibold text-ink">Brand Benchmark Suite</p>
        </Link>

        <nav className="flex min-w-0 flex-1 items-center gap-1 overflow-x-auto overflow-y-visible py-1">
          {NAV_ITEMS.map((item) => {
            const active = pathname?.startsWith(item.href);
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
          <button
            type="button"
            className="inline-flex h-9 w-9 items-center justify-center rounded-full border border-ink/10 bg-white text-xs font-semibold text-slate shadow-sm transition hover:text-ink"
            aria-label="User"
            title="User"
          >
            U
          </button>
        </div>
      </div>
    </header>
  );
}
