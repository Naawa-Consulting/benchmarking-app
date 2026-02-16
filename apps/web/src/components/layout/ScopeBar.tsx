"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { usePathname, useSearchParams } from "next/navigation";
import * as Popover from "@radix-ui/react-popover";
import { useScope } from "./ScopeProvider";

type PanelKey = "brands" | "demographics" | "time" | "advanced";
type PanelPosition = { top: number; left: number; width: number };

function summaryLabel(values: string[], fallback: string) {
  if (!values.length) return fallback;
  if (values.length === 1) return values[0];
  return `${values.length} selected`;
}

export default function ScopeBar() {
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const { scope, setScope, resetScope, taxonomyItems, demographics, dateOptions, optionsLoading, brands } =
    useScope();

  const [openPanel, setOpenPanel] = useState<PanelKey | null>(null);
  const [brandsOpen, setBrandsOpen] = useState(false);
  const [demographicsOpen, setDemographicsOpen] = useState(false);
  const [timeOpen, setTimeOpen] = useState(false);
  const [panelPos, setPanelPos] = useState<PanelPosition | null>(null);
  const [brandSearch, setBrandSearch] = useState("");
  const [brandsPrunedHint, setBrandsPrunedHint] = useState<string | null>(null);

  const panelRef = useRef<HTMLDivElement | null>(null);
  const brandsButtonRef = useRef<HTMLButtonElement | null>(null);
  const demographicsButtonRef = useRef<HTMLButtonElement | null>(null);
  const timeButtonRef = useRef<HTMLButtonElement | null>(null);
  const advancedButtonRef = useRef<HTMLButtonElement | null>(null);

  const isPresentationMode = pathname === "/demand-network" && searchParams.get("presentation") === "1";

  const sectorOptions = useMemo(
    () => Array.from(new Set(taxonomyItems.map((item) => item.sector))).sort(),
    [taxonomyItems]
  );

  const subsectorOptions = useMemo(() => {
    if (!scope.sector) {
      return Array.from(new Set(taxonomyItems.map((item) => item.subsector))).sort();
    }
    return Array.from(
      new Set(taxonomyItems.filter((item) => item.sector === scope.sector).map((item) => item.subsector))
    ).sort();
  }, [taxonomyItems, scope.sector]);

  const categoryOptions = useMemo(() => {
    if (!scope.sector && !scope.subsector) {
      return Array.from(new Set(taxonomyItems.map((item) => item.category))).sort();
    }
    return Array.from(
      new Set(
        taxonomyItems
          .filter((item) => (scope.sector ? item.sector === scope.sector : true))
          .filter((item) => (scope.subsector ? item.subsector === scope.subsector : true))
          .map((item) => item.category)
      )
    ).sort();
  }, [taxonomyItems, scope.sector, scope.subsector]);

  const filteredBrands = useMemo(() => {
    const needle = brandSearch.trim().toLowerCase();
    if (!needle) return brands;
    return brands.filter((brand) => brand.toLowerCase().includes(needle));
  }, [brandSearch, brands]);

  const closePanel = () => {
    setOpenPanel(null);
    setPanelPos(null);
  };

  const getButtonByPanel = (panel: PanelKey | null) => {
    switch (panel) {
      case "brands":
        return brandsButtonRef.current;
      case "demographics":
        return demographicsButtonRef.current;
      case "time":
        return timeButtonRef.current;
      case "advanced":
        return advancedButtonRef.current;
      default:
        return null;
    }
  };

const getPanelWidth = (panel: PanelKey) => {
    if (panel === "demographics") return 460;
    if (panel === "advanced") return 420;
    if (panel === "time") return 320;
    const triggerWidth = brandsButtonRef.current?.getBoundingClientRect().width ?? 260;
    return Math.max(320, Math.min(420, triggerWidth));
  };

  const updatePanelPosition = (panel: PanelKey) => {
    const button = getButtonByPanel(panel);
    if (!button) return;
    const rect = button.getBoundingClientRect();
    const width = getPanelWidth(panel);
    const viewportPadding = 16;
    const panelHeight = panel === "demographics" ? 360 : 320;
    const left = Math.max(viewportPadding, Math.min(rect.left, window.innerWidth - width - viewportPadding));
    const downTop = rect.bottom + 8;
    const upTop = Math.max(viewportPadding, rect.top - panelHeight - 8);
    const top =
      downTop + panelHeight > window.innerHeight - viewportPadding
        ? upTop
        : downTop;
    setPanelPos({ top, left, width });
  };

  const togglePanel = (panel: PanelKey) => {
    if (openPanel === panel) {
      closePanel();
      return;
    }
    setOpenPanel(panel);
    window.requestAnimationFrame(() => updatePanelPosition(panel));
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
    if (!openPanel) return;
    updatePanelPosition(openPanel);
    const handleResize = () => updatePanelPosition(openPanel);
    window.addEventListener("resize", handleResize);
    window.addEventListener("scroll", handleResize, true);
    return () => {
      window.removeEventListener("resize", handleResize);
      window.removeEventListener("scroll", handleResize, true);
    };
  }, [openPanel]);

  useEffect(() => {
    const handlePointerDown = (event: MouseEvent) => {
      if (!openPanel) return;
      const target = event.target as Node;
      const inPanel = panelRef.current?.contains(target);
      const inButton =
        brandsButtonRef.current?.contains(target) ||
        demographicsButtonRef.current?.contains(target) ||
        timeButtonRef.current?.contains(target) ||
        advancedButtonRef.current?.contains(target);
      if (!inPanel && !inButton) closePanel();
    };
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") closePanel();
    };
    window.addEventListener("mousedown", handlePointerDown);
    window.addEventListener("keydown", handleEscape);
    return () => {
      window.removeEventListener("mousedown", handlePointerDown);
      window.removeEventListener("keydown", handleEscape);
    };
  }, [openPanel]);

  useEffect(() => {
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
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
    if (!scope.brands.length) return;
    const availableSet = new Set(brands);
    const valid = scope.brands.filter((brand) => availableSet.has(brand));
    if (valid.length === scope.brands.length) return;
    setScope({ brands: valid });
    setBrandsPrunedHint("Some brands were removed due to scope change.");
    const timer = window.setTimeout(() => setBrandsPrunedHint(null), 3200);
    return () => window.clearTimeout(timer);
  }, [brands, scope.brands, setScope]);

  if (isPresentationMode) {
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
            <option value="">Sector: All</option>
            {sectorOptions.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>

          <select
            className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm"
            value={scope.subsector || ""}
            onChange={(event) => setScope({ subsector: event.target.value || null })}
            aria-label="Subsector filter"
          >
            <option value="">Subsector: All</option>
            {subsectorOptions.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>

          <select
            className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm"
            value={scope.category || ""}
            onChange={(event) => setScope({ category: event.target.value || null })}
            aria-label="Category filter"
          >
            <option value="">Category: All</option>
            {categoryOptions.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>

          <Popover.Root
            open={brandsOpen}
            onOpenChange={(nextOpen) => {
              setBrandsOpen(nextOpen);
              if (nextOpen) {
                closePanel();
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
                disabled={!brands.length}
                title={!brands.length ? "Load studies to populate brands" : undefined}
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

          <Popover.Root
            open={demographicsOpen}
            onOpenChange={(nextOpen) => {
              setDemographicsOpen(nextOpen);
              if (nextOpen) {
                closePanel();
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
                    <p className="mb-2 font-semibold text-ink">State</p>
                    <div className="grid grid-cols-2 gap-1 pr-1">
                      {demographics.state.map((value) => (
                        <label key={value} className="flex items-center gap-2">
                          <input
                            type="checkbox"
                            checked={scope.state.includes(value)}
                            onChange={() => toggleListValue("state", value)}
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
                closePanel();
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
                Time: Quarter
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
                  <p className="mb-2 text-xs font-semibold text-ink">Quarter range</p>
                  <select
                    className="mb-2 w-full rounded-xl border border-ink/10 px-3 py-2 text-xs"
                    value={scope.quarterFrom || ""}
                    onChange={(event) => setScope({ quarterFrom: event.target.value || null })}
                  >
                    <option value="">From quarter</option>
                    {dateOptions.quarters.map((value) => (
                      <option key={`from-${value}`} value={value}>
                        {value}
                      </option>
                    ))}
                  </select>
                  <select
                    className="w-full rounded-xl border border-ink/10 px-3 py-2 text-xs"
                    value={scope.quarterTo || ""}
                    onChange={(event) => setScope({ quarterTo: event.target.value || null })}
                  >
                    <option value="">To quarter</option>
                    {dateOptions.quarters.map((value) => (
                      <option key={`to-${value}`} value={value}>
                        {value}
                      </option>
                    ))}
                  </select>
                </div>
              </Popover.Content>
            </Popover.Portal>
          </Popover.Root>

          <div className="ml-auto flex items-center gap-2">
            <button
              ref={advancedButtonRef}
              type="button"
              className="rounded-full border border-ink/10 bg-white px-3 py-2 text-xs font-medium text-ink shadow-sm transition hover:bg-slate-50"
              onClick={() => togglePanel("advanced")}
            >
              Advanced
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

        {openPanel && panelPos && (
          <div
            ref={panelRef}
            className="fixed z-[80] rounded-2xl border border-ink/10 bg-white p-3 shadow-xl"
            style={{ top: panelPos.top, left: panelPos.left, width: panelPos.width, maxHeight: 360, overflow: "auto" }}
          >
            {openPanel === "advanced" && (
              <div className="grid gap-3 text-xs text-slate md:grid-cols-2">
                <div>
                  <p className="font-semibold text-ink">Advanced options</p>
                  <p className="mt-1">Weighting profile and include/exclude controls will appear here.</p>
                </div>
                <div>
                  <p className="font-semibold text-ink">Network controls</p>
                  <p className="mt-1">Link threshold and scenario presets are reserved for upcoming sprints.</p>
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
