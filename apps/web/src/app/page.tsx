"use client";

import Link from "next/link";
import { useState } from "react";

import JourneyChart from "../components/JourneyChart";
import SeedDemoButton from "../components/SeedDemoButton";
import StudySelector from "../components/StudySelector";

export default function HomePage() {
  const [studyId, setStudyId] = useState("demo_001");

  return (
    <main className="space-y-8">
      <section className="main-surface rounded-3xl p-6">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <h2 className="text-2xl font-semibold">Quick Actions</h2>
            <p className="text-slate">Seed demo data and explore the journey view.</p>
          </div>
          <div className="flex flex-wrap gap-3">
            <SeedDemoButton />
            <Link
              className="rounded-full border border-ink/10 bg-ink px-5 py-2 text-sm font-medium text-white"
              href="/journey"
            >
              Open Journey
            </Link>
          </div>
        </div>
      </section>

      <section className="main-surface rounded-3xl p-6 space-y-4">
        <div className="flex flex-col gap-2">
          <h3 className="text-xl font-semibold">Current Study</h3>
          <StudySelector value={studyId} onChange={setStudyId} />
        </div>
        <JourneyChart studyId={studyId} />
      </section>
    </main>
  );
}
