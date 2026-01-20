"use client";

import { useMemo } from "react";

export type StudyOption = {
  study_id: string;
  study_name?: string | null;
  sector?: string | null;
  subsector?: string | null;
  category?: string | null;
  has_demographics?: boolean;
  has_date?: boolean;
};

export type TaxonomyItem = {
  sector: string;
  subsector: string;
  category: string;
};

export type DemographicOptions = {
  gender: string[];
  nse: string[];
  state: string[];
  age: { min: number | null; max: number | null };
};

export type DateOptions = {
  quarters: string[];
  min?: string | null;
  max?: string | null;
};

export type FilterState = {
  studyIds: string[];
  sector: string | null;
  subsector: string | null;
  category: string | null;
  gender: string | null;
  nse: string | null;
  state: string | null;
  ageMin: number | null;
  ageMax: number | null;
  dateGrain: "Q";
  quarterFrom: string | null;
  quarterTo: string | null;
};

type FilterBarProps = {
  state: FilterState;
  studies: StudyOption[];
  taxonomyItems: TaxonomyItem[];
  demographics: DemographicOptions;
  dateOptions: DateOptions;
  onChange: (next: Partial<FilterState>) => void;
  onClear: () => void;
};

export function FilterBar({
  state,
  studies,
  taxonomyItems,
  demographics,
  dateOptions,
  onChange,
  onClear,
}: FilterBarProps) {
  const subsectorOptions = useMemo(() => {
    if (!state.sector) {
      return Array.from(new Set(taxonomyItems.map((item) => item.subsector))).sort();
    }
    return Array.from(
      new Set(
        taxonomyItems
          .filter((item) => item.sector === state.sector)
          .map((item) => item.subsector)
      )
    ).sort();
  }, [taxonomyItems, state.sector]);

  const categoryOptions = useMemo(() => {
    if (!state.sector && !state.subsector) {
      return Array.from(new Set(taxonomyItems.map((item) => item.category))).sort();
    }
    return Array.from(
      new Set(
        taxonomyItems
          .filter((item) => (state.sector ? item.sector === state.sector : true))
          .filter((item) => (state.subsector ? item.subsector === state.subsector : true))
          .map((item) => item.category)
      )
    ).sort();
  }, [taxonomyItems, state.sector, state.subsector]);

  return (
    <section className="main-surface rounded-3xl p-6">
      <div className="flex flex-col gap-1">
        <h2 className="text-2xl font-semibold">Filters</h2>
        <p className="text-sm text-slate">Refine consolidated results across studies.</p>
      </div>

      <div className="mt-6 grid gap-6 lg:grid-cols-[1.2fr_1fr_1fr_1fr]">
        <div className="space-y-3">
          <label className="text-sm font-medium text-slate">Study (multi-select)</label>
          <select
            className="h-40 w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
            multiple
            value={state.studyIds}
            onChange={(event) => {
              const selected = Array.from(event.target.selectedOptions).map((option) => option.value);
              onChange({ studyIds: selected });
            }}
          >
            {studies.length === 0 ? (
              <option value="">No studies available</option>
            ) : (
              studies.map((study) => (
                <option key={study.study_id} value={study.study_id}>
                  {study.study_name || study.study_id}
                </option>
              ))
            )}
          </select>
          <p className="text-xs text-slate">Hold Ctrl/Cmd to select multiple studies.</p>
        </div>

        <div className="space-y-3">
          <label className="text-sm font-medium text-slate">Sector</label>
          <select
            className="w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
            value={state.sector || ""}
            onChange={(event) => onChange({ sector: event.target.value || null })}
          >
            <option value="">All sectors</option>
            {Array.from(new Set(taxonomyItems.map((item) => item.sector)))
              .sort()
              .map((sector) => (
                <option key={sector} value={sector}>
                  {sector}
                </option>
              ))}
          </select>

          <label className="text-sm font-medium text-slate">Subsector</label>
          <select
            className="w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
            value={state.subsector || ""}
            onChange={(event) => onChange({ subsector: event.target.value || null })}
          >
            <option value="">All subsectors</option>
            {subsectorOptions.map((subsector) => (
              <option key={subsector} value={subsector}>
                {subsector}
              </option>
            ))}
          </select>

          <label className="text-sm font-medium text-slate">Category</label>
          <select
            className="w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
            value={state.category || ""}
            onChange={(event) => onChange({ category: event.target.value || null })}
          >
            <option value="">All categories</option>
            {categoryOptions.map((category) => (
              <option key={category} value={category}>
                {category}
              </option>
            ))}
          </select>
        </div>

        <div className="space-y-3">
          <label className="text-sm font-medium text-slate">Gender</label>
          <select
            className="w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
            value={state.gender || ""}
            onChange={(event) => onChange({ gender: event.target.value || null })}
          >
            <option value="">All genders</option>
            {demographics.gender.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>

          <label className="text-sm font-medium text-slate">NSE</label>
          <select
            className="w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
            value={state.nse || ""}
            onChange={(event) => onChange({ nse: event.target.value || null })}
          >
            <option value="">All NSE</option>
            {demographics.nse.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>

          <label className="text-sm font-medium text-slate">State</label>
          <select
            className="w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
            value={state.state || ""}
            onChange={(event) => onChange({ state: event.target.value || null })}
          >
            <option value="">All states</option>
            {demographics.state.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>

          <label className="text-sm font-medium text-slate">Age</label>
          <div className="flex gap-2">
            <input
              className="w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
              type="number"
              placeholder={demographics.age.min != null ? `${demographics.age.min}` : "Min"}
              value={state.ageMin ?? ""}
              onChange={(event) =>
                onChange({ ageMin: event.target.value ? Number(event.target.value) : null })
              }
            />
            <input
              className="w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
              type="number"
              placeholder={demographics.age.max != null ? `${demographics.age.max}` : "Max"}
              value={state.ageMax ?? ""}
              onChange={(event) =>
                onChange({ ageMax: event.target.value ? Number(event.target.value) : null })
              }
            />
          </div>
        </div>

        <div className="space-y-3">
          <label className="text-sm font-medium text-slate">Date (Quarter)</label>
          <select
            className="w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
            value={state.quarterFrom || ""}
            onChange={(event) => onChange({ quarterFrom: event.target.value || null })}
          >
            <option value="">From quarter</option>
            {dateOptions.quarters.map((value) => (
              <option key={`from-${value}`} value={value}>
                {value}
              </option>
            ))}
          </select>
          <select
            className="w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
            value={state.quarterTo || ""}
            onChange={(event) => onChange({ quarterTo: event.target.value || null })}
          >
            <option value="">To quarter</option>
            {dateOptions.quarters.map((value) => (
              <option key={`to-${value}`} value={value}>
                {value}
              </option>
            ))}
          </select>

          <button
            type="button"
            onClick={onClear}
            className="mt-2 w-full rounded-xl border border-ink/10 px-3 py-2 text-sm text-slate"
          >
            Clear filters
          </button>
        </div>
      </div>
    </section>
  );
}
