# Journey Data Layer (Sprint 1)

## Schema
- Canonical stages:
  - `Brand Awareness`
  - `Ad Awareness`
  - `Brand Consideration`
  - `Brand Purchase`
  - `Brand Satisfaction`
  - `Brand Recommendation`
- Internal normalized rows are `JourneyStageRow` in LONG shape:
  - `studyId`, `brandName`, `dims`, `stage`, `value`, `weight`, `baseN`
- `value` is normalized to `0..1` internally (not `0..100`).

## Missing Stages Rules
- Missing stage rows are never imputed as zero.
- Drop-offs/conversions are computed only from valid study pairs where both stages exist.
- Coverage is tracked by stage and by link:
  - distinct contributing studies
  - summed weights

## Benchmark Definition
- Default benchmark scope: `category` inside the current filtered selection.
- Sector scope is supported by API (`benchmarkScope`) for future use.
- Gaps are stage-level differences:
  - `brandStageValue - benchmarkStageValue`

## CSAT / NPS
- `CSAT`:
  - official if explicit field exists (`csat_score`, `satisfaction_score`, `csat`)
  - otherwise proxy from `Brand Satisfaction` stage
- `NPS`:
  - official if explicit `nps` field exists, or from `promoters_pct - detractors_pct`
  - otherwise proxy from `Brand Recommendation` stage
- Each metric carries metadata:
  - `metricType`: `official | proxy`
  - `explanation`: short reason used by UI

## Hero Sankey Consumption (Sprint 2)
- Journey UI calls:
  - `buildJourneyModel(rawJourneyResults, filters, { includeAdAwareness })`
- Sankey nodes are the ordered funnel stages from `model.stagesOrdered`.
- Sankey links are only rendered for valid adjacent stage pairs with coverage (no zero-imputation).
- For each brand:
  - prefers link conversion data from `model.brandStageAggregates[].links`
  - fallback link value is `min(stage_i, stage_{i+1})` when both stages exist
- Missing stages remain missing; links are not fabricated.

## Insights + Heatmap (Sprint 3)
- Insights consume `JourneyModel` only (no backend recompute):
  - biggest drop-off
  - strongest/weakest stage deltas vs benchmark
  - conversion deltas
  - multi-brand differentiation
  - CSAT/NPS highlights
  - coverage warnings
- Heatmap table:
  - rows: Benchmark + selected brands (display-capped)
  - columns: stage rates, adjacent conversions, CSAT, NPS
  - each cell stores value, benchmark, delta, and coverage
- missing stages/links render as `—` (neutral cell, no imputation)

## Time + Focus (Sprint 4)
- Time mode is frontend-only:
  - derives buckets from normalized Journey rows (`quarter/time/wave/...`)
  - filters model input per selected bucket
  - play/pause animates bucket stepping (no backend changes)
- Focus mode is presentation-only:
  - focuses Sankey on one brand + benchmark (+ optional compare brand)
  - Heatmap dims non-focused rows
  - Insights are regenerated for focus/compare context
- Performance:
  - memoized model, heatmap, and insights slices
  - non-critical insights update via `useTransition`
