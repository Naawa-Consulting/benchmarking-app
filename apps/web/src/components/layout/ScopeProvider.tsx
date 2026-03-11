"use client";

import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import {
  getFilterDateOptionsDetailed,
  getFilterDemographicsOptionsDetailed,
  getFilterStudyOptionsDetailed,
  getFilterTaxonomyOptionsDetailed,
  postTouchpointsTableMultiDetailed,
} from "../../lib/api";

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

export type ScopeState = {
  studyIds: string[];
  brands: string[];
  sector: string | null;
  subsector: string | null;
  category: string | null;
  gender: string[];
  nse: string[];
  state: string[];
  ageMin: number | null;
  ageMax: number | null;
  timeGranularity: "Q";
  quarterFrom: string | null;
  quarterTo: string | null;
};

const DEFAULT_SCOPE: ScopeState = {
  studyIds: [],
  brands: [],
  sector: null,
  subsector: null,
  category: null,
  gender: [],
  nse: [],
  state: [],
  ageMin: null,
  ageMax: null,
  timeGranularity: "Q",
  quarterFrom: null,
  quarterTo: null,
};

type ScopeContextValue = {
  scope: ScopeState;
  setScope: (partial: Partial<ScopeState>) => void;
  resetScope: () => void;
  studies: StudyOption[];
  taxonomyItems: TaxonomyItem[];
  demographics: DemographicOptions;
  dateOptions: DateOptions;
  optionsLoading: boolean;
  brands: string[];
  setTrackingBrandOptions: (brands: string[] | null) => void;
};

const ScopeContext = createContext<ScopeContextValue | null>(null);

const MANAGED_KEYS = [
  "studies",
  "brands",
  "sector",
  "subsector",
  "category",
  "gender",
  "nse",
  "state",
  "age",
  "time",
  "q_from",
  "q_to",
] as const;

function parseCsv(value: string | null): string[] {
  if (!value) return [];
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseScopeFromQuery(searchParams: URLSearchParams): Partial<ScopeState> {
  const ageRaw = searchParams.get("age");
  let ageMin: number | null = null;
  let ageMax: number | null = null;

  if (ageRaw && ageRaw.includes("-")) {
    const [min, max] = ageRaw.split("-");
    const parsedMin = Number(min);
    const parsedMax = Number(max);
    ageMin = Number.isFinite(parsedMin) ? parsedMin : null;
    ageMax = Number.isFinite(parsedMax) ? parsedMax : null;
  }

  return {
    studyIds: parseCsv(searchParams.get("studies")),
    brands: parseCsv(searchParams.get("brands")),
    sector: searchParams.get("sector"),
    subsector: searchParams.get("subsector"),
    category: searchParams.get("category"),
    gender: parseCsv(searchParams.get("gender")),
    nse: parseCsv(searchParams.get("nse")),
    state: parseCsv(searchParams.get("state")),
    ageMin,
    ageMax,
    timeGranularity: searchParams.get("time") === "Q" ? "Q" : "Q",
    quarterFrom: searchParams.get("q_from"),
    quarterTo: searchParams.get("q_to"),
  };
}

function createManagedQuery(scope: ScopeState): URLSearchParams {
  const params = new URLSearchParams();
  if (scope.studyIds.length) params.set("studies", scope.studyIds.join(","));
  if (scope.brands.length) params.set("brands", scope.brands.join(","));
  if (scope.sector) params.set("sector", scope.sector);
  if (scope.subsector) params.set("subsector", scope.subsector);
  if (scope.category) params.set("category", scope.category);
  if (scope.gender.length) params.set("gender", scope.gender.join(","));
  if (scope.nse.length) params.set("nse", scope.nse.join(","));
  if (scope.state.length) params.set("state", scope.state.join(","));
  if (scope.ageMin !== null || scope.ageMax !== null) {
    params.set("age", `${scope.ageMin ?? ""}-${scope.ageMax ?? ""}`);
  }
  params.set("time", scope.timeGranularity);
  if (scope.quarterFrom) params.set("q_from", scope.quarterFrom);
  if (scope.quarterTo) params.set("q_to", scope.quarterTo);
  return params;
}

export function ScopeProvider({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const initializedRef = useRef(false);

  const [scope, setScopeState] = useState<ScopeState>(DEFAULT_SCOPE);
  const [studies, setStudies] = useState<StudyOption[]>([]);
  const [taxonomyItems, setTaxonomyItems] = useState<TaxonomyItem[]>([]);
  const [demographics, setDemographics] = useState<DemographicOptions>({
    gender: [],
    nse: [],
    state: [],
    age: { min: null, max: null },
  });
  const [dateOptions, setDateOptions] = useState<DateOptions>({ quarters: [] });
  const [optionsLoading, setOptionsLoading] = useState(false);
  const [brands, setBrands] = useState<string[]>([]);
  const [trackingBrandOptions, setTrackingBrandOptionsState] = useState<string[] | null>(null);

  const selectedStudyIdsOrNull = useMemo<string[] | null>(() => {
    return scope.studyIds.length > 0 ? scope.studyIds : null;
  }, [scope.studyIds]);

  useEffect(() => {
    if (initializedRef.current) return;
    const parsed = parseScopeFromQuery(searchParams);
    setScopeState((prev) => ({ ...prev, ...parsed }));
    initializedRef.current = true;
  }, [searchParams]);

  useEffect(() => {
    const loadStaticOptions = async () => {
      setOptionsLoading(true);
      const [studyResult, taxonomyResult] = await Promise.all([
        getFilterStudyOptionsDetailed(),
        getFilterTaxonomyOptionsDetailed(),
      ]);
      if (studyResult.ok && studyResult.data) {
        const payload = studyResult.data as { items?: StudyOption[] };
        setStudies(Array.isArray(payload.items) ? payload.items : []);
      }
      if (taxonomyResult.ok && taxonomyResult.data) {
        const payload = taxonomyResult.data as { items?: TaxonomyItem[] };
        setTaxonomyItems(Array.isArray(payload.items) ? payload.items : []);
      }
      setOptionsLoading(false);
    };
    loadStaticOptions();
  }, []);

  useEffect(() => {
    const loadBrandOptions = async () => {
      const result = await postTouchpointsTableMultiDetailed(
        {
          study_ids: selectedStudyIdsOrNull,
          sector: scope.sector,
          subsector: scope.subsector,
          category: scope.category,
          gender: scope.gender.length ? scope.gender[0] : null,
          nse: scope.nse.length ? scope.nse[0] : null,
          state: scope.state.length ? scope.state[0] : null,
          age_min: scope.ageMin,
          age_max: scope.ageMax,
          date_grain: scope.timeGranularity,
          quarter_from: scope.quarterFrom,
          quarter_to: scope.quarterTo,
        },
        "all"
      );

      if (!result.ok || !result.data || typeof result.data !== "object") {
        setBrands([]);
        return;
      }

      const payload = result.data as {
        rows?: Array<{ brand?: string }>;
      };
      const rows = Array.isArray(payload.rows) ? payload.rows : [];
      const nextBrands = Array.from(
        new Set(
          rows
            .map((row) => (typeof row.brand === "string" ? row.brand.trim() : ""))
            .filter(Boolean)
        )
      ).sort((a, b) => a.localeCompare(b));
      setBrands(nextBrands);
    };
    loadBrandOptions();
  }, [
    selectedStudyIdsOrNull,
    scope.sector,
    scope.subsector,
    scope.category,
    scope.gender,
    scope.nse,
    scope.state,
    scope.ageMin,
    scope.ageMax,
    scope.timeGranularity,
    scope.quarterFrom,
    scope.quarterTo,
  ]);

  useEffect(() => {
    const loadVariableOptions = async () => {
      const [demoResult, dateResult] = await Promise.all([
        getFilterDemographicsOptionsDetailed(selectedStudyIdsOrNull),
        getFilterDateOptionsDetailed(selectedStudyIdsOrNull),
      ]);
      if (demoResult.ok && demoResult.data) {
        const payload = demoResult.data as DemographicOptions;
        setDemographics({
          gender: Array.isArray(payload.gender) ? payload.gender : [],
          nse: Array.isArray(payload.nse) ? payload.nse : [],
          state: Array.isArray(payload.state) ? payload.state : [],
          age: payload.age || { min: null, max: null },
        });
      }
      if (dateResult.ok && dateResult.data) {
        const payload = dateResult.data as DateOptions;
        setDateOptions({
          quarters: Array.isArray(payload.quarters) ? payload.quarters : [],
          min: payload.min ?? null,
          max: payload.max ?? null,
        });
      }
    };
    loadVariableOptions();
  }, [selectedStudyIdsOrNull]);

  useEffect(() => {
    if (!initializedRef.current) return;

    const existing = new URLSearchParams(searchParams.toString());
    MANAGED_KEYS.forEach((key) => existing.delete(key));
    const managed = createManagedQuery(scope);
    managed.forEach((value, key) => existing.set(key, value));

    const next = existing.toString();
    const current = searchParams.toString();
    if (next !== current) {
      router.replace(next ? `${pathname}?${next}` : pathname, { scroll: false });
    }
  }, [scope, router, pathname, searchParams]);

  const setScope = useCallback((partial: Partial<ScopeState>) => {
    setScopeState((prev) => {
      const merged: ScopeState = { ...prev, ...partial };

      const hasSector = Object.prototype.hasOwnProperty.call(partial, "sector");
      const hasSubsector = Object.prototype.hasOwnProperty.call(partial, "subsector");
      const hasCategory = Object.prototype.hasOwnProperty.call(partial, "category");

      if (hasSector && partial.sector !== prev.sector) {
        if (!hasSubsector) merged.subsector = null;
        if (!hasCategory) merged.category = null;
      }
      if (hasSubsector && partial.subsector !== prev.subsector) {
        if (!hasCategory) merged.category = null;
      }

      const changed =
        merged.studyIds.join("|") !== prev.studyIds.join("|") ||
        merged.brands.join("|") !== prev.brands.join("|") ||
        merged.sector !== prev.sector ||
        merged.subsector !== prev.subsector ||
        merged.category !== prev.category ||
        merged.gender.join("|") !== prev.gender.join("|") ||
        merged.nse.join("|") !== prev.nse.join("|") ||
        merged.state.join("|") !== prev.state.join("|") ||
        merged.ageMin !== prev.ageMin ||
        merged.ageMax !== prev.ageMax ||
        merged.timeGranularity !== prev.timeGranularity ||
        merged.quarterFrom !== prev.quarterFrom ||
        merged.quarterTo !== prev.quarterTo;

      return changed ? merged : prev;
    });
  }, []);

  const resetScope = useCallback(() => setScopeState(DEFAULT_SCOPE), []);

  const effectiveBrands = trackingBrandOptions ?? brands;

  const setTrackingBrandOptions = useCallback((nextBrands: string[] | null) => {
    setTrackingBrandOptionsState(nextBrands);
  }, []);

  const value = useMemo<ScopeContextValue>(
    () => ({
      scope,
      setScope,
      resetScope,
      studies,
      taxonomyItems,
      demographics,
      dateOptions,
      optionsLoading,
      brands: effectiveBrands,
      setTrackingBrandOptions,
    }),
    [
      scope,
      setScope,
      resetScope,
      studies,
      taxonomyItems,
      demographics,
      dateOptions,
      optionsLoading,
      effectiveBrands,
      setTrackingBrandOptions,
    ]
  );

  return <ScopeContext.Provider value={value}>{children}</ScopeContext.Provider>;
}

export function useScope() {
  const value = useContext(ScopeContext);
  if (!value) {
    throw new Error("useScope must be used inside ScopeProvider");
  }
  return value;
}
