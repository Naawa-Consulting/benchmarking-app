"use client";

import { usePathname } from "next/navigation";
import TopNav from "./TopNav";
import ScopeBar from "./ScopeBar";
import { ScopeProvider } from "./ScopeProvider";

type AppShellProps = {
  children: React.ReactNode;
  layoutVariant?: "content" | "canvas";
};

export default function AppShell({ children, layoutVariant = "content" }: AppShellProps) {
  const pathname = usePathname();
  const isAuthPage = pathname?.startsWith("/auth");

  return (
    <ScopeProvider>
      <div className="min-h-screen bg-app">
        {!isAuthPage ? <TopNav /> : null}
        {!isAuthPage ? (
          <div className="pt-[68px]">
            <ScopeBar />
          </div>
        ) : null}
        <main className={`pb-8 ${isAuthPage ? "pt-0" : "pt-6"}`}>
          <div
            className={`mx-auto w-full px-4 sm:px-6 lg:px-8 ${
              layoutVariant === "canvas" ? "max-w-[1800px]" : "max-w-[1440px]"
            }`}
          >
            {children}
          </div>
        </main>
      </div>
    </ScopeProvider>
  );
}
