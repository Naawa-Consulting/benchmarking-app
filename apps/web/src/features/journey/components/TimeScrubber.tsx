"use client";

type TimeScrubberProps = {
  enabled: boolean;
  timeBuckets: string[];
  selectedBucket: string | null;
  isPlaying: boolean;
  speed: 0.5 | 1 | 2;
  compact?: boolean;
  onToggleEnabled: (next: boolean) => void;
  onSelectBucket: (bucket: string) => void;
  onPrev: () => void;
  onNext: () => void;
  onTogglePlay: () => void;
  onSpeedChange: (speed: 0.5 | 1 | 2) => void;
};

export default function TimeScrubber({
  enabled,
  timeBuckets,
  selectedBucket,
  isPlaying,
  speed,
  compact = false,
  onToggleEnabled,
  onSelectBucket,
  onPrev,
  onNext,
  onTogglePlay,
  onSpeedChange,
}: TimeScrubberProps) {
  const activeIndex = selectedBucket ? timeBuckets.indexOf(selectedBucket) : -1;
  const buttonPad = compact ? "px-2.5 py-1" : "px-3 py-1.5";
  const textSize = compact ? "text-[11px]" : "text-xs";
  const lineGap = compact ? "gap-1.5" : "gap-2";

  return (
    <div>
      <div className={`flex flex-wrap items-center ${lineGap} ${textSize}`}>
        <button
          type="button"
          className={`rounded-full border ${buttonPad} ${
            enabled
              ? "border-emerald-500/40 bg-emerald-500/10 text-emerald-700"
              : "border-ink/10 bg-white text-slate"
          }`}
          onClick={() => onToggleEnabled(!enabled)}
        >
          Year: {enabled ? "On" : "Off"}
        </button>

        {enabled && (
          <>
            <button
              type="button"
              className={`rounded-full border border-ink/10 bg-white text-slate ${buttonPad}`}
              onClick={onPrev}
              disabled={timeBuckets.length <= 1}
            >
              Prev
            </button>
            <button
              type="button"
              className={`rounded-full border border-ink/10 bg-white text-slate ${buttonPad}`}
              onClick={onTogglePlay}
              disabled={timeBuckets.length <= 1}
            >
              {isPlaying ? "Pause" : "Play"}
            </button>
            <button
              type="button"
              className={`rounded-full border border-ink/10 bg-white text-slate ${buttonPad}`}
              onClick={onNext}
              disabled={timeBuckets.length <= 1}
            >
              Next
            </button>
            <select
              value={String(speed)}
              onChange={(event) => onSpeedChange(Number(event.target.value) as 0.5 | 1 | 2)}
              className={`rounded-full border border-ink/10 bg-white ${buttonPad}`}
            >
              <option value="0.5">0.5x</option>
              <option value="1">1x</option>
              <option value="2">2x</option>
            </select>
            <span className={`rounded-full border border-ink/10 bg-white text-slate ${buttonPad}`}>
              {selectedBucket ? `Year: ${selectedBucket}` : "Year: --"}
            </span>
          </>
        )}
      </div>

      {enabled && timeBuckets.length > 0 && (
        <div className={compact ? "mt-2 flex items-center gap-2" : "mt-3 flex items-center gap-3"}>
          <input
            type="range"
            min={0}
            max={Math.max(0, timeBuckets.length - 1)}
            value={Math.max(0, activeIndex)}
            onChange={(event) => onSelectBucket(timeBuckets[Number(event.target.value)])}
            className="w-full"
          />
        </div>
      )}
    </div>
  );
}
