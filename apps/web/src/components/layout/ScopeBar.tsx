"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import * as Popover from "@radix-ui/react-popover";
import { useScope } from "./ScopeProvider";

function summaryLabel(values: string[], fallback: string) {
  if (!values.length) return fallback;
  if (values.length === 1) return values[0];
  return `${values.length} selected`;
}

function extractYear(value: string | null | undefined): string | null {
  if (!value) return null;
  const match = String(value).match(/(19|20)\d{2}/);
  return match ? match[0] : null;
}

export default function ScopeBar() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { scope, setScope, resetScope, studies, taxonomyItems, demographics, dateOptions, optionsLoading, brands } =
    useScope();
  const taxonomyLabels =
    scope.taxonomyView === "market"
      ? { sector: "Macrosector", subsector: "Segmento", category: "Categoría comercial" }
      : { sector: "Sector", subsector: "Subsector", category: "Categoría" };

  const advancedOpen = searchParams.get("scope_advanced") === "1";
  const [brandsOpen, setBrandsOpen] = useState(false);
  const [demographicsOpen, setDemographicsOpen] = useState(false);
  const [timeOpen, setTimeOpen] = useState(false);
  const [brandSearch, setBrandSearch] = useState("");
  const [brandsPrunedHint, setBrandsPrunedHint] = useState<string | null>(null);

  const brandsButtonRef = useRef<HTMLButtonElement | null>(null);
  const demographicsButtonRef = useRef<HTMLButtonElement | null>(null);
  const timeButtonRef = useRef<HTMLButtonElement | null>(null);

  const isPresentationMode = pathname === "/demand-network" && searchParams.get("presentation") === "1";
  const isAgentPage = pathname?.startsWith("/agent");
  const sharedBrandsMode = searchParams.get("brands_mode");
  const legacyJourneyBrandsEnabled = searchParams.get("journey_brands") === "1";
  const legacyNetworkBrandsEnabled = searchParams.get("network_brands") === "enable";
  const isJourneyBrandsEnabled =
    pathname !== "/journey" ||
    sharedBrandsMode === "enable" ||
    (sharedBrandsMode !== "disable" && legacyJourneyBrandsEnabled);
  const isNetworkBrandsEnabled =
    pathname !== "/demand-network" ||
    sharedBrandsMode === "enable" ||
    (sharedBrandsMode !== "disable" && legacyNetworkBrandsEnabled);
  const isTrackingBrandsEnabled = pathname !== "/tracking" || Boolean(scope.category);
  const areBrandsEnabled = isJourneyBrandsEnabled && isNetworkBrandsEnabled && isTrackingBrandsEnabled;

  const setAdvancedOpen = (nextOpen: boolean) => {
    const params = new URLSearchParams(searchParams.toString());
    if (nextOpen) {
      params.set("scope_advanced", "1");
    } else {
      params.delete("scope_advanced");
    }
    router.replace(`${pathname}${params.toString() ? `?${params.toString()}` : ""}`, { scroll: false });
  };

  const scopedStudies = useMemo(
    () =>
      scope.studyIds.length
        ? studies.filter((study) => scope.studyIds.includes(study.study_id))
        : studies,
    [scope.studyIds, studies]
  );

  const sectorOptions = useMemo(
    () => Array.from(new Set(taxonomyItems.map((item) => item.sector))).sort(),
    [taxonomyItems]
  );

  const subsectorOptions = useMemo(() => {
    if (!scope.sector) return [];
    return Array.from(
      new Set(taxonomyItems.filter((item) => item.sector === scope.sector).map((item) => item.subsector))
    ).sort();
  }, [scope.sector, taxonomyItems]);

  const categoryOptions = useMemo(() => {
    if (!scope.sector || !scope.subsector) return [];
    return Array.from(
      new Set(
        taxonomyItems
          .filter((item) => item.sector === scope.sector)
          .filter((item) => item.subsector === scope.subsector)
          .map((item) => item.category)
      )
    ).sort();
  }, [scope.sector, scope.subsector, taxonomyItems]);

  const enabledSectors = useMemo(
    () =>
      new Set(
        scopedStudies
          .map((study) =>
            scope.taxonomyView === "market"
              ? typeof study.market_sector === "string"
                ? study.market_sector.trim()
                : ""
              : typeof study.sector === "string"
                ? study.sector.trim()
                : ""
          )
          .filter(Boolean)
      ),
    [scope.taxonomyView, scopedStudies]
  );

  const enabledSubsectors = useMemo(() => {
    if (!scope.sector) return new Set<string>();
    return new Set(
      scopedStudies
        .filter((study) =>
          scope.taxonomyView === "market" ? study.market_sector === scope.sector : study.sector === scope.sector
        )
        .map((study) =>
          scope.taxonomyView === "market"
            ? typeof study.market_subsector === "string"
              ? study.market_subsector.trim()
              : ""
            : typeof study.subsector === "string"
              ? study.subsector.trim()
              : ""
        )
        .filter(Boolean)
    );
  }, [scope.sector, scope.taxonomyView, scopedStudies]);

  const enabledCategories = useMemo(() => {
    if (!scope.sector || !scope.subsector) return new Set<string>();
    return new Set(
      scopedStudies
        .filter((study) =>
          scope.taxonomyView === "market"
            ? study.market_sector === scope.sector && study.market_subsector === scope.subsector
            : study.sector === scope.sector && study.subsector === scope.subsector
        )
        .map((study) =>
          scope.taxonomyView === "market"
            ? typeof study.market_category === "string"
              ? study.market_category.trim()
              : ""
            : typeof study.category === "string"
              ? study.category.trim()
              : ""
        )
        .filter(Boolean)
    );
  }, [scope.sector, scope.subsector, scope.taxonomyView, scopedStudies]);

  const filteredBrands = useMemo(() => {
    const needle = brandSearch.trim().toLowerCase();
    if (!needle) return brands;
    return brands.filter((brand) => brand.toLowerCase().includes(needle));
  }, [brandSearch, brands]);

  const years = useMemo(() => {
    const set = new Set<string>();
    for (const value of dateOptions.quarters) {
      const year = extractYear(value);
      if (year) set.add(year);
    }
    return Array.from(set).sort((a, b) => Number(a) - Number(b));
  }, [dateOptions.quarters]);

  const toggleYearValue = (year: string) => {
    const next = scope.years.includes(year) ? scope.years.filter((item) => item !== year) : [...scope.years, year];
    setScope({ years: next.sort((a, b) => Number(a) - Number(b)) });
  };

  const closeBrandsPopover = () => {
    setBrandsOpen(false);
  };
  const closeDemographicsPopover = () => {
    setDemographicsOpen(false);
  };
  const closeTimePopover = () => {
    setTimeOpen(false);
  };

  useEffect(() => {
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setAdvancedOpen(false);
        closeBrandsPopover();
        closeDemographicsPopover();
        closeTimePopover();
      }
    };
    window.addEventListener("keydown", handleEscape);
    return () => window.removeEventListener("keydown", handleEscape);
  }, []);

  const toggleListValue = (key: "brands" | "gender" | "nse" | "state", value: string) => {
    const current = scope[key];
    const next = current.includes(value) ? current.filter((item) => item !== value) : [...current, value];
    setScope({ [key]: next } as Partial<typeof scope>);
  };

  useEffect(() => {
    if (scope.state.length === 0) return;
    setScope({ state: [] });
  }, [scope.state, setScope]);

  useEffect(() => {
    if (!scope.brands.length) return;
    const availableSet = new Set(brands);
    const valid = scope.brands.filter((brand) => availableSet.has(brand));
    if (valid.length === scope.brands.length) return;
    setScope({ brands: valid });
    setBrandsPrunedHint("Some brands were removed due to scope change.");
    const timer = window.setTimeout(() => setBrandsPrunedHint(null), 3200);
    return () => window.clearTimeout(timer);
  }, [brands, scope.brands, setScope]);

  useEffect(() => {
    if (pathname !== "/tracking") return;
    if (scope.category || !scope.brands.length) return;
    setScope({ brands: [] });
  }, [pathname, scope.brands, scope.category, setScope]);

  useEffect(() => {
    if (scope.sector && enabledSectors.size > 0 && !enabledSectors.has(scope.sector)) {
      setScope({ sector: null, subsector: null, category: null, brands: [] });
      return;
    }
    if (scope.subsector && enabledSubsectors.size > 0 && !enabledSubsectors.has(scope.subsector)) {
      setScope({ subsector: null, category: null, brands: [] });
      return;
    }
    if (scope.category && enabledCategories.size > 0 && !enabledCategories.has(scope.category)) {
      setScope({ category: null, brands: [] });
    }
  }, [
    enabledCategories,
    enabledSectors,
    enabledSubsectors,
    scope.category,
    scope.sector,
    scope.subsector,
    setScope,
  ]);

  if (isPresentationMode || isAgentPage) {
    return null;
  }

  return (
    <div className="sticky top-[68px] z-30 border-b border-ink/10 bg-[#f7f8fa]/95 backdrop-blur">
      <div className="mx-auto w-full max-w-[1800px] px-4 py-3 sm:px-6 lg:px-8">
        <div className="flex flex-nowrap items-center gap-2 overflow-x-auto pb-1">
          <select
            className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm"
            value={scope.sector || ""}
            onChange={(event) => setScope({ sector: event.target.value || null })}
            aria-label="Sector filter"
          >
            <option value="">{taxonomyLabels.sector}: All</option>
            {sectorOptions.map((value) => (
              <option
                key={value}
                value={value}
                disabled={enabledSectors.size > 0 && !enabledSectors.has(value)}
              >
                {value}
              </option>
            ))}
          </select>

          <select
            className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm disabled:cursor-not-allowed disabled:bg-slate-100 disabled:text-slate"
            value={scope.subsector || ""}
            onChange={(event) => setScope({ subsector: event.target.value || null })}
            aria-label="Subsector filter"
            disabled={!scope.sector}
            title={!scope.sector ? `Select ${taxonomyLabels.sector} first` : undefined}
          >
            <option value="">{taxonomyLabels.subsector}: All</option>
            {subsectorOptions.map((value) => (
              <option
                key={value}
                value={value}
                disabled={enabledSubsectors.size > 0 && !enabledSubsectors.has(value)}
              >
                {value}
              </option>
            ))}
          </select>

          <select
            className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm disabled:cursor-not-allowed disabled:bg-slate-100 disabled:text-slate"
            value={scope.category || ""}
            onChange={(event) => setScope({ category: event.target.value || null })}
            aria-label="Category filter"
            disabled={!scope.subsector}
            title={!scope.subsector ? `Select ${taxonomyLabels.subsector} first` : undefined}
          >
            <option value="">{taxonomyLabels.category}: All</option>
            {categoryOptions.map((value) => (
              <option
                key={value}
                value={value}
                disabled={enabledCategories.size > 0 && !enabledCategories.has(value)}
              >
                {value}
              </option>
            ))}
          </select>

          {areBrandsEnabled && (
            <Popover.Root
              open={brandsOpen}
              onOpenChange={(nextOpen) => {
                setBrandsOpen(nextOpen);
                if (nextOpen) {
                  setAdvancedOpen(false);
                  closeDemographicsPopover();
                  closeTimePopover();
                }
              }}
            >
              <Popover.Trigger asChild>
                <button
                  ref={brandsButtonRef}
                  type="button"
                  className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
                  disabled={!scope.category || !brands.length}
                  title={!scope.category ? "Select Category first" : !brands.length ? "Load studies to populate brands" : undefined}
                  aria-label="Brands filter"
                >
                  Brands:{" "}
                  {scope.brands.length === 0 || scope.brands.length === brands.length ? "All" : scope.brands.length}
                </button>
              </Popover.Trigger>
              <Popover.Portal>
                {/* Portal + Popper keeps the dropdown anchored under the trigger across scroll/stacking contexts. */}
                <Popover.Content
                  side="bottom"
                  align="start"
                  sideOffset={8}
                  alignOffset={0}
                  avoidCollisions
                  collisionPadding={8}
                  className="z-[80] w-[var(--radix-popover-trigger-width)] min-w-[280px] max-w-[420px] rounded-2xl border border-ink/10 bg-white p-3 shadow-xl focus:outline-none"
                >
                  <div className="space-y-3">
                    <div className="flex items-center justify-between">
                      <p className="text-xs font-semibold text-ink">Brands</p>
                      <div className="flex gap-2 text-[11px]">
                        <button
                          type="button"
                          className="text-[#008a67] hover:underline"
                          onClick={() => setScope({ brands: filteredBrands })}
                        >
                          Select all
                        </button>
                        <button type="button" className="text-slate hover:underline" onClick={() => setScope({ brands: [] })}>
                          Clear
                        </button>
                      </div>
                    </div>
                    <input
                      type="text"
                      value={brandSearch}
                      onChange={(event) => setBrandSearch(event.target.value)}
                      placeholder="Search brands"
                      className="sticky top-0 z-[1] w-full rounded-xl border border-ink/10 bg-white px-3 py-2 text-sm"
                    />
                    <div className="max-h-[320px] space-y-1 overflow-auto pr-1">
                      {filteredBrands.map((brand) => (
                        <label key={brand} className="flex cursor-pointer items-center gap-2 rounded-lg px-2 py-1.5 hover:bg-slate-50">
                          <input
                            type="checkbox"
                            checked={scope.brands.includes(brand)}
                            onChange={() => toggleListValue("brands", brand)}
                          />
                          <span className="text-xs text-ink">{brand}</span>
                        </label>
                      ))}
                      {!filteredBrands.length && <p className="px-2 py-3 text-xs text-slate">No brands match your search.</p>}
                    </div>
                  </div>
                </Popover.Content>
              </Popover.Portal>
            </Popover.Root>
          )}

          <button
            type="button"
            className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm transition hover:bg-slate-50"
            onClick={() => setScope({ taxonomyView: scope.taxonomyView === "market" ? "standard" : "market" })}
          >
            View: {scope.taxonomyView === "market" ? "Market Lens" : "Taxonomía Estándar"}
          </button>

          <Popover.Root
            open={demographicsOpen}
            onOpenChange={(nextOpen) => {
              setDemographicsOpen(nextOpen);
              if (nextOpen) {
                setAdvancedOpen(false);
                closeBrandsPopover();
                closeTimePopover();
              }
            }}
          >
            <Popover.Trigger asChild>
              <button
                ref={demographicsButtonRef}
                type="button"
                className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm transition hover:bg-slate-50"
              >
                Demo: {summaryLabel(scope.gender, "All")}
              </button>
            </Popover.Trigger>
            <Popover.Portal>
              <Popover.Content
                side="bottom"
                align="start"
                sideOffset={8}
                alignOffset={0}
                avoidCollisions
                collisionPadding={8}
                className="z-[80] w-[460px] max-w-[92vw] rounded-2xl border border-ink/10 bg-white p-3 shadow-xl focus:outline-none"
              >
                <div className="grid max-h-[360px] grid-cols-2 gap-3 overflow-auto pr-1 text-xs">
                  <div>
                    <p className="mb-2 font-semibold text-ink">Gender</p>
                    <div className="space-y-1 pr-1">
                      {demographics.gender.map((value) => (
                        <label key={value} className="flex items-center gap-2">
                          <input
                            type="checkbox"
                            checked={scope.gender.includes(value)}
                            onChange={() => toggleListValue("gender", value)}
                          />
                          <span>{value}</span>
                        </label>
                      ))}
                    </div>
                  </div>
                  <div>
                    <p className="mb-2 font-semibold text-ink">NSE</p>
                    <div className="space-y-1 pr-1">
                      {demographics.nse.map((value) => (
                        <label key={value} className="flex items-center gap-2">
                          <input
                            type="checkbox"
                            checked={scope.nse.includes(value)}
                            onChange={() => toggleListValue("nse", value)}
                          />
                          <span>{value}</span>
                        </label>
                      ))}
                    </div>
                  </div>
                  <div className="col-span-2">
                    <p className="mb-2 font-semibold text-ink">Age range</p>
                    <div className="flex gap-2">
                      <input
                        type="number"
                        className="w-full rounded-xl border border-ink/10 px-3 py-2"
                        placeholder={demographics.age.min != null ? `${demographics.age.min}` : "Min"}
                        value={scope.ageMin ?? ""}
                        onChange={(event) => setScope({ ageMin: event.target.value ? Number(event.target.value) : null })}
                      />
                      <input
                        type="number"
                        className="w-full rounded-xl border border-ink/10 px-3 py-2"
                        placeholder={demographics.age.max != null ? `${demographics.age.max}` : "Max"}
                        value={scope.ageMax ?? ""}
                        onChange={(event) => setScope({ ageMax: event.target.value ? Number(event.target.value) : null })}
                      />
                    </div>
                  </div>
                </div>
              </Popover.Content>
            </Popover.Portal>
          </Popover.Root>

          <Popover.Root
            open={timeOpen}
            onOpenChange={(nextOpen) => {
              setTimeOpen(nextOpen);
              if (nextOpen) {
                setAdvancedOpen(false);
                closeBrandsPopover();
                closeDemographicsPopover();
              }
            }}
          >
            <Popover.Trigger asChild>
              <button
                ref={timeButtonRef}
                type="button"
                className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm transition hover:bg-slate-50"
              >
                Time: {scope.years.length ? `${scope.years.length} year${scope.years.length > 1 ? "s" : ""}` : "Year"}
              </button>
            </Popover.Trigger>
            <Popover.Portal>
              <Popover.Content
                side="bottom"
                align="start"
                sideOffset={8}
                alignOffset={0}
                avoidCollisions
                collisionPadding={8}
                className="z-[80] w-[320px] max-w-[92vw] rounded-2xl border border-ink/10 bg-white p-3 shadow-xl focus:outline-none"
              >
                <div className="max-h-[320px] overflow-auto">
                  <div className="mb-2 flex items-center justify-between">
                    <p className="text-xs font-semibold text-ink">Select year(s)</p>
                    <button
                      type="button"
                      className="text-[11px] text-slate underline"
                      onClick={() => setScope({ years: [] })}
                    >
                      Clear
                    </button>
                  </div>
                  <div className="space-y-1">
                    {years.map((value) => (
                      <label key={`year-${value}`} className="flex items-center gap-2 rounded-lg px-2 py-1 text-xs text-ink hover:bg-slate-50">
                        <input
                          type="checkbox"
                          checked={scope.years.includes(value)}
                          onChange={() => toggleYearValue(value)}
                        />
                        <span>{value}</span>
                      </label>
                    ))}
                  </div>
                </div>
              </Popover.Content>
            </Popover.Portal>
          </Popover.Root>

          <div className="ml-auto flex items-center gap-2">
            <button
              type="button"
              className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm transition hover:bg-slate-50"
              onClick={() => setAdvancedOpen(!advancedOpen)}
              aria-expanded={advancedOpen}
              aria-controls="scope-advanced-submenu"
            >
              Advanced {advancedOpen ? "^" : "v"}
            </button>
            <button
              type="button"
              className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-slate shadow-sm transition hover:bg-slate-50"
              onClick={resetScope}
            >
              Reset
            </button>
          </div>
        </div>

        {optionsLoading && <p className="mt-2 text-[11px] text-slate">Loading scope options...</p>}
        {brandsPrunedHint && <p className="mt-2 text-[11px] text-slate">{brandsPrunedHint}</p>}

        {advancedOpen && (
          <div
            id="scope-advanced-submenu"
            className="mt-3 w-full rounded-2xl border border-ink/10 bg-white p-3 shadow-sm"
          >
            <div className="grid gap-3 text-xs text-slate md:grid-cols-2">
              <div className="md:col-span-2">
                <p className="font-semibold text-ink">
                  {pathname === "/journey"
                    ? "Journey controls"
                    : pathname === "/tracking"
                      ? "Trends controls"
                      : "Network controls"}
                </p>
              </div>
              {pathname === "/demand-network" && (
                <div className="md:col-span-2">
                  <div id="dn-advanced-controls-slot" />
                </div>
              )}
              {pathname === "/journey" && (
                <div className="md:col-span-2">
                  <div id="journey-advanced-controls-slot" />
                </div>
              )}
              {pathname === "/tracking" && (
                <div className="md:col-span-2">
                  <div id="tracking-advanced-controls-slot" />
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
