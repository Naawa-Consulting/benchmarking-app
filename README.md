# Brand Benchmark Suite (BBS)

Monorepo for the BBS product with three core analytics experiences:
- Journey
- Network (Demand Network)
- Tracking

Plus Admin tools for data ingestion, validation, taxonomy, and rules.

## Stack
- Frontend: Next.js App Router + TypeScript + Tailwind + ECharts + Radix Popover
- Backend: FastAPI + DuckDB + Parquet
- Data: local warehouse under `data/warehouse` (raw + curated + taxonomy)

## Repository Structure
- `apps/web`: Product frontend (`/journey`, `/demand-network`, `/tracking`, `/admin`)
- `services/api`: FastAPI service and analytics routers
- `data`: local parquet warehouse and taxonomy files
- `scripts`: local helper scripts

## Prerequisites
- Node.js 18+
- Python 3.11+ (Windows recommended via `py -3.11`)

## Local Setup

### 1) API (Terminal A)
```powershell
cd "services\api"
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app.main:app --reload
```
API: `http://localhost:8000`

### 2) Web (Terminal B)
```powershell
cd "apps\web"
npm install
npm run dev
```
Web: `http://localhost:3000`

> Note: `predev` clears `.next` automatically to avoid stale chunk/static 404 issues in local dev.

## Navigation
Top-level pages:
- `Journey`
- `Network`
- `Tracking`
- `Admin`

Root `/` redirects to `/journey`.

## Global Filters (Scope Bar)
Common scope dimensions:
- Sector -> Subsector -> Category (hierarchical)
- Brands (contextual enable/disable by page mode)
- Demo: Gender, NSE, State, Age
- Time: `years` multi-select

### Demo Filter Logic
Backend applies:
- OR within each demo dimension (`gender IN (...)`, `nse IN (...)`, `state IN (...)`)
- AND across dimensions

Example:
- `gender = [Mujer, Femenino, Hombre, Masculino]`
- AND `nse = [C, C+, C-]`

## Current Functional Behavior by Page

### Journey
- Progressive loading strategy:
  - Global benchmark first
  - Selection benchmark next
  - Brand detail last (when Brands enabled)
- Benchmarks:
  - `Global Benchmark` (fixed base)
  - `Selection Benchmark` (current scope)
- Brand mode toggle (`Enable/Disable`) integrated in Advanced.
- Heatmap includes benchmark rows and brand rows depending on mode.
- Time mode uses Year buckets.

### Network
- Demand graph with:
  - touchpoints <-> brand/benchmark nodes
  - metric switch: Recall / Consideration / Purchase
  - distance modes and layout controls in Advanced
- Benchmark mode hierarchy (when Brands disabled):
  - no sector selected: group by Sector
  - sector selected: group by Subsector
  - subsector selected: group by Category
  - category selected: single benchmark node
- Stable interaction: no unintended re-layout on hover/click.

### Tracking
- Uses global filters; no manual Base A/Base B workflow in main flow.
- Temporal intelligence:
  - compare by Year when multiple years available
  - fallback to Quarter when only one year is available
- Primary block uses hierarchical breakdown:
  - Sector / Subsector / Category / Brand (depending on selected filters)
- Secondary block is touchpoint analysis (real touchpoint rows, not duplicated primary rows).
- Excel export includes visible comparison + metadata.

## Key API Endpoints

### Filters
- `GET /filters/options/studies`
- `GET /filters/options/taxonomy`
- `GET /filters/options/demographics`
- `GET /filters/options/date`

### Journey
- `GET /analytics/journey`
- `GET /analytics/journey/table`
- `GET|POST /analytics/journey/table_multi`
  - supports `response_mode=benchmark_global|benchmark_selection|full`

### Touchpoints / Network
- `GET|POST /analytics/touchpoints/table_multi`
- `GET /network`

### Tracking
- `GET|POST /analytics/tracking/series`

## Performance & Stability Patterns Already Implemented
- Frontend request race protection in critical pages:
  - `AbortController` + request sequence guards (latest response wins)
- Progressive rendering in Journey to improve first meaningful paint
- Tracking debounce on filter changes
- In-memory TTL cache in API routers for heavy responses
- Unified `years` filtering (legacy quarter range removed from public API usage)

## Troubleshooting
- API not reachable:
  - verify `NEXT_PUBLIC_API_BASE_URL` in `apps/web/.env.local`
- PowerShell execution policy:
  - `Set-ExecutionPolicy -Scope Process Bypass`
- Windows Python command not found:
  - use `py -3.11` instead of `python`
- Next static/chunk 404 in dev:
  - stop dev server, restart `npm run dev` (predev clears `.next`)

## Known Constraints
- Some historical studies have incomplete stages; model handles missingness without zero imputation.
- Taxonomy metadata gaps may produce `Unassigned` buckets.
- Large scope selections can still be expensive in Tracking and Network due to respondent-level filtering.

## Next Steps (Optimization Backlog)

### P0 (High Impact)
1. Shared aggregation service in backend
   - Create shared per-study scoped aggregators used by Journey, Network, and Tracking.
   - Remove duplicated loops and repeated parquet scans across routers.
2. Reduce ScopeBar brand option cost
   - Stop using full touchpoints aggregation to populate brand options.
   - Add lightweight brand-options endpoint (or cached derived index).
3. Tracking query consolidation
   - Move to one-pass per-study temporal aggregation for journey + touchpoints.
   - Avoid period x study nested query patterns.

### P1 (Reliability + Latency)
4. Cache key normalization + shared cache module
   - Centralize TTL cache utilities and metric metadata.
5. Backend query observability
   - Standardize `meta` timings: collect/query/aggregate/total + rows scanned.
6. Partial response streaming/progressive API payloads
   - Return benchmark/core data early, detail blocks later (especially for Tracking).

### P2 (UX + Maintainability)
7. Add non-interactive lint setup in web
   - Ensure `npm run lint` works without first-time interactive prompt.
8. Add regression tests for filter semantics
   - Demo OR/AND behavior
   - Years multi-select behavior
   - Brand enable/disable shared mode across Journey/Network
9. Move heavy client transforms to memoized selectors/modules
   - Keep render-only interactions (hover, focus, legend toggles) free of model recomputation.

## Local Data Notes
- DuckDB reads/writes parquet under `data/warehouse`.
- Ingestion pipeline reads from `data/landing`.
- Curated marts are under `data/warehouse/curated/study_id=...`.

