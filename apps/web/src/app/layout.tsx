import "./globals.css";
import Link from "next/link";
import { Fraunces, Space_Grotesk } from "next/font/google";

const display = Fraunces({ subsets: ["latin"], weight: ["600"] });
const body = Space_Grotesk({ subsets: ["latin"], weight: ["400", "500", "600"] });

export const metadata = {
  title: "Benchmarking MVP",
  description: "Consumer insights benchmarking MVP"
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={body.className}>
      <body className="min-h-screen">
        <div className="mx-auto max-w-6xl px-6 py-10">
          <header className="mb-6 flex flex-col gap-3">
            <div>
              <p className={`text-sm uppercase tracking-[0.4em] text-slate ${display.className}`}>
                Benchmarking
              </p>
              <h1 className={`text-4xl md:text-5xl font-semibold ${display.className}`}>
                Benchmarking MVP
              </h1>
            </div>
            <nav className="flex flex-wrap gap-3 text-sm font-medium">
              <Link className="rounded-full border border-ink/10 px-4 py-2" href="/">
                Home
              </Link>
              <Link className="rounded-full border border-ink/10 px-4 py-2" href="/journey">
                Journey
              </Link>
              <Link className="rounded-full border border-ink/10 px-4 py-2" href="/admin">
                Admin
              </Link>
            </nav>
          </header>
          {children}
        </div>
      </body>
    </html>
  );
}
