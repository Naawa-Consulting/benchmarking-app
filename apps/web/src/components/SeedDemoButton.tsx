"use client";

import { useState } from "react";

import { seedDemo } from "../lib/api";

export default function SeedDemoButton() {
  const [status, setStatus] = useState<"idle" | "loading" | "done" | "error">("idle");

  const handleSeed = async () => {
    setStatus("loading");
    try {
      await seedDemo();
      setStatus("done");
      setTimeout(() => setStatus("idle"), 1500);
    } catch {
      setStatus("error");
    }
  };

  const label = status === "loading" ? "Seeding..." : status === "done" ? "Seeded" : "Seed demo";

  return (
    <button
      className="rounded-full border border-ember/20 bg-ember px-5 py-2 text-sm font-medium text-white"
      onClick={handleSeed}
      type="button"
    >
      {label}
    </button>
  );
}
