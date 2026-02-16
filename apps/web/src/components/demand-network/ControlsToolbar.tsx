"use client";

import { type ReactNode } from "react";

type Option<T extends string | number> = {
  label: string;
  value: T;
};

type ToolbarProps = {
  children: ReactNode;
  className?: string;
};

type ToolbarGroupProps = {
  label: string;
  tooltip?: string;
  children: ReactNode;
};

type ChipToggleProps = {
  label: string;
  active?: boolean;
  disabled?: boolean;
  tooltip?: string;
  onClick?: () => void;
};

type ChipToggleGroupProps<T extends string | number> = {
  label: string;
  value: T;
  options: Array<Option<T>>;
  tooltip?: string;
  disabled?: boolean;
  onChange: (value: T) => void;
};

type ChipSelectProps<T extends string | number> = {
  label: string;
  value: T;
  options: Array<Option<T>>;
  tooltip?: string;
  disabled?: boolean;
  onChange: (value: T) => void;
};

const groupLabelClass = "text-[10px] font-semibold uppercase tracking-wide text-slate";
const groupBoxClass = "flex items-center gap-1 rounded-full border border-ink/10 bg-white/90 p-1";
const chipBaseClass =
  "rounded-full border px-3 py-1 text-[11px] transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-200";

export function Toolbar({ children, className }: ToolbarProps) {
  return (
    <div
      className={`rounded-2xl border border-ink/10 bg-slate-50/80 p-3 shadow-sm ${className || ""}`.trim()}
    >
      <div className="flex items-center gap-2 overflow-x-auto whitespace-nowrap pb-1">{children}</div>
    </div>
  );
}

export function ToolbarGroup({ label, tooltip, children }: ToolbarGroupProps) {
  return (
    <div className="inline-flex items-center gap-2">
      <div
        className="flex items-center gap-2 rounded-xl border border-transparent px-1 py-0.5"
        title={tooltip}
        aria-label={tooltip || label}
      >
        <span className={groupLabelClass}>{label}</span>
        <div className={groupBoxClass}>{children}</div>
      </div>
      <span className="h-5 w-px bg-ink/10 last:hidden" />
    </div>
  );
}

export function ChipToggle({ label, active, disabled, tooltip, onClick }: ChipToggleProps) {
  return (
    <button
      type="button"
      title={tooltip}
      aria-label={tooltip || label}
      disabled={disabled}
      onClick={onClick}
      className={`${chipBaseClass} ${
        active
          ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
          : "border-ink/10 bg-white text-slate hover:border-ink/20"
      } ${disabled ? "cursor-not-allowed opacity-50" : ""}`}
    >
      {label}
    </button>
  );
}

export function ChipToggleGroup<T extends string | number>({
  label,
  value,
  options,
  tooltip,
  disabled,
  onChange,
}: ChipToggleGroupProps<T>) {
  return (
    <ToolbarGroup label={label} tooltip={tooltip}>
      {options.map((option) => (
        <ChipToggle
          key={String(option.value)}
          label={option.label}
          active={option.value === value}
          disabled={disabled}
          tooltip={tooltip}
          onClick={() => onChange(option.value)}
        />
      ))}
    </ToolbarGroup>
  );
}

export function ChipSelect<T extends string | number>({
  label,
  value,
  options,
  tooltip,
  disabled,
  onChange,
}: ChipSelectProps<T>) {
  return (
    <ToolbarGroup label={label} tooltip={tooltip}>
      <label className="sr-only" htmlFor={`chip-select-${label}`}>
        {label}
      </label>
      <select
        id={`chip-select-${label}`}
        className="rounded-full border border-ink/10 bg-white px-3 py-1 text-[11px] text-ink focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-emerald-200"
        value={String(value)}
        title={tooltip}
        aria-label={tooltip || label}
        disabled={disabled}
        onChange={(event) => onChange(event.target.value as T)}
      >
        {options.map((option) => (
          <option key={String(option.value)} value={String(option.value)}>
            {option.label}
          </option>
        ))}
      </select>
    </ToolbarGroup>
  );
}
