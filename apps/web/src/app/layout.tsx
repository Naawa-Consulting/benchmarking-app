import "./globals.css";
import { Space_Grotesk } from "next/font/google";
import { Suspense } from "react";
import AppShell from "../components/layout/AppShell";

const body = Space_Grotesk({ subsets: ["latin"], weight: ["400", "500", "600"] });

export const metadata = {
  title: "BBS | Brand Benchmark Suite",
  description: "Brand Benchmark Suite",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={body.className}>
      <body className="min-h-screen text-ink">
        <Suspense fallback={<div className="min-h-screen bg-app" />}>
          <AppShell>{children}</AppShell>
        </Suspense>
      </body>
    </html>
  );
}
