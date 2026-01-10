"use client";

import { useEffect, useState } from "react";

import { fetchStudies } from "../lib/api";
import { Study } from "../lib/types";

interface Props {
  value: string;
  onChange: (value: string) => void;
}

export default function StudySelector({ value, onChange }: Props) {
  const [studies, setStudies] = useState<Study[]>([]);

  useEffect(() => {
    fetchStudies()
      .then((data) => setStudies(data))
      .catch(() => setStudies([]));
  }, []);

  useEffect(() => {
    if (!value && studies.length > 0) {
      onChange(studies[0].id);
    }
  }, [value, studies, onChange]);

  return (
    <div className="flex flex-col gap-2">
      <label className="text-sm font-medium text-slate">Study</label>
      <select
        className="rounded-xl border border-ink/10 bg-white px-4 py-2"
        value={value}
        onChange={(event) => onChange(event.target.value)}
      >
        {studies.length === 0 ? (
          <option value="demo_001">demo_001 (seed first)</option>
        ) : (
          studies.map((study) => (
            <option key={study.id} value={study.id}>
              {study.name}
            </option>
          ))
        )}
      </select>
    </div>
  );
}
