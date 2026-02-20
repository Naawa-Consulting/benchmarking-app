"use client";

type FocusBarProps = {
  availableBrands: string[];
  focusBrand: string | null;
  compareBrand: string | null;
  onFocusBrandChange: (brand: string | null) => void;
  onCompareBrandChange: (brand: string | null) => void;
  onClearFocus: () => void;
};

export default function FocusBar({
  availableBrands,
  focusBrand,
  compareBrand,
  onFocusBrandChange,
  onCompareBrandChange,
  onClearFocus,
}: FocusBarProps) {
  const compareOptions = availableBrands.filter((brand) => brand !== focusBrand);
  return (
    <section className="main-surface p-4">
      <div className="flex flex-wrap items-center gap-2 text-xs">
        <span className="text-slate">Focus mode</span>
        <select
          className="rounded-full border border-ink/10 bg-white px-3 py-1.5"
          value={focusBrand || ""}
          onChange={(event) => onFocusBrandChange(event.target.value || null)}
        >
          <option value="">Focus: None</option>
          {availableBrands.map((brand) => (
            <option key={brand} value={brand}>
              {brand}
            </option>
          ))}
        </select>
        {focusBrand && (
          <>
            <select
              className="rounded-full border border-ink/10 bg-white px-3 py-1.5"
              value={compareBrand || ""}
              onChange={(event) => onCompareBrandChange(event.target.value || null)}
            >
              <option value="">Compare: None</option>
              {compareOptions.map((brand) => (
                <option key={brand} value={brand}>
                  {brand}
                </option>
              ))}
            </select>
            <button
              type="button"
              className="rounded-full border border-ink/10 bg-white px-3 py-1.5 text-slate"
              onClick={onClearFocus}
            >
              Clear focus
            </button>
          </>
        )}
      </div>
    </section>
  );
}

