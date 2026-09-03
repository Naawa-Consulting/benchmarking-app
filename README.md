# Brand Benchmark Suite (BBS)

Monorepo for the BBS product with three core analytics experiences:
- Journey
- Network (Demand Network)
- Tracking

Plus Admin tools for data ingestion, validation, taxonomy, and rules.

## Estado actual
_Última actualización: 2026-09-02_

- **v2 Fase 0 is done: diagnostic (BITACORA 2026-09-01 cont. 9) + all three P0 items shipped
  (BITACORA 2026-09-02).** The framing that drove it: production runs in `supabase` mode, so the
  analytics path never touches FastAPI/Storage, and the real cost was payload size, not the database.
  Measured end to end through the gateway: a Journey page load dropped from **5.51 MB to 0.83 MB**
  with a category selected, and from **12.05 MB to 2.25 MB** unscoped. The shared
  `question_map_v0.csv` is retired (archived under `warehouse/_archive/`), which removes the root
  cause of the Question Mapper race rather than mitigating it.
- Still open from that phase: the demographic-filter gap (the user chose to fix it properly by adding
  a demographic dimension to the marts — blocked on defining which cuts the business uses; the
  filters stay visible and inert until then), the Agent's data access rework, and hardening
  `analytics.py` against Storage's HTTP 429 on multi-study fan-out.
- Vercel + Supabase deployment mode is live in read-only beta alongside the legacy FastAPI backend
  (`BBS_DATA_SOURCE` switches between them; see "Vercel + Supabase Deployment Mode" below).
- Push no longer scans the full curated corpus per study pushed: the 3 cross-study imputation rate
  models now train via a SQL RPC (`bbs_rate_model_training_stats`) over `journey_metrics` instead of
  scanning every curated study's parquet, controlled by `BBS_RATE_MODEL_SOURCE` (`auto` default, falls
  back to the parquet scan on any failure). Supabase Storage reads also gained cache-busting
  (`app/storage/blob.py`) after finding the CDN in front of it could serve reads days stale after a
  successful write. See BITACORA.md 2026-08-31 for the investigation and validation detail.
- Most recent focus has been hardening the Agent module: `AGENTE.md` as the single behavior source,
  es/en response-language enforcement, and access control (`BBS_AGENT_OWNER_ONLY`).
- In progress, not yet committed: trimming/compacting the row payloads sent to the Agent's LLM calls
  and making row/token limits configurable via env vars, to control cost/latency.
- The optimization backlog further down this document is the current known-gaps list. Its "Done"
  section records the 2026-09-02 P0 work; the pre-2026-09-01 P0 #1/#3 targeted `analytics.py`, which
  production does not execute, and stay deferred.
- See [BITACORA.md](BITACORA.md) for the chronological work log and [INDEX.md](INDEX.md) for a
  file-by-file map of the repo.

## Permisos y restricciones del agente

- `data/` is a real local warehouse (parquet/DuckDB + source `.sav` files), not disposable fixtures —
  don't bulk-delete or bulk-rewrite it without confirming with the user first.
- `supabase/sql/*.sql` migrations are applied manually in the Supabase SQL Editor; there is no
  migration runner in this repo. Adding a new numbered file here does not mean it has been applied —
  don't assume schema changes are live without checking.
- `.env*` files are git-ignored by design (see `.gitignore`) — never commit them or paste their
  contents into files that do get committed.
- Changes to `AGENTE.md`, `apps/web/src/middleware.ts`, or `apps/web/src/app/api/_lib/authz.ts`
  affect real production access control (agent behavior, auth gating, role/scope permissions) —
  treat as sensitive and flag them clearly when made.

## Versionado y tags

Convención única para tags de release: **`vMAJOR.MINOR.PATCH`** (semver, `v` en minúscula).

- `v1.0.0` — 2026-08-25, `29689a1` — project-os setup.
- `v1.1.0` — 2026-09-01, `cf5e818` — taxonomía Automotriz y Teléfonos celulares en Market Lens.

Reglas:
- Siempre tag **anotado** (`git tag -a vX.Y.Z -m "..."`), nunca ligero — así el tag guarda autor,
  fecha y mensaje propios, y `git describe` funciona de forma predecible.
- No usar variantes como `V01`, `V1.0` ni `1.0.0` (sin `v`); los tags históricos que seguían ese
  esquema ya fueron renombrados a esta convención.
- Renombrar un tag es crear + borrar (`git tag NUEVO VIEJO && git tag -d VIEJO`). Si el tag ya se
  pusheó, hay que además `git push origin NUEVO` y `git push origin --delete VIEJO`, y avisar al
  equipo de correr `git fetch --prune --prune-tags`.

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
- `GET /filters/options/brands` (FastAPI) / `POST /api/filters/options/brands` (gateway)
  - added 2026-09-02; backs the Scope Bar's Brand dropdown instead of a full touchpoints aggregation
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
- **Demographic filters (Gender/NSE/State/Age) are a no-op in `supabase` mode (production).**
  `journey_metrics`/`touchpoint_metrics` are aggregated at `study × brand [× touchpoint]` with no
  demographic dimension, and none of the four analytics RPCs accept those parameters — the Next
  gateway sends them and the RPC silently ignores them. They do work in `legacy` mode (respondent-level
  filtering in DuckDB). This is the third known legacy→Supabase fidelity gap, alongside `bbs_network`
  (no secondary links, no percentile scaling) and `bbs_tracking_series` (averages unweighted by
  `base_n_population`). Confirmed 2026-09-01 — see BITACORA.md. **The filters are still shown in the
  Scope Bar and still do nothing**: the agreed fix is to add the dimension to the marts, which needs
  the business to define the cuts first.
- Respondent-level filtering in Tracking/Network is expensive, **but only on the `legacy` path**,
  which production does not execute. Do not treat it as a production bottleneck.
- Supabase Storage answers **HTTP 429** when a request fans out reads across all ~51 studies, and
  cache-busting (`app/storage/blob.py`) guarantees every read reaches origin. `filters.py`'s brand
  endpoint retries and degrades per study; **`analytics.py` still has no handling and returns 500** —
  this is the most likely cause of the "transient Render 500s that go away on retry" seen during the
  LMB waves. Confirmed 2026-09-02.

## Next Steps (Optimization Backlog)

_Reprioritized 2026-09-01 after the v2 Fase 0 diagnostic, then updated 2026-09-02 as the P0 items
shipped (see BITACORA.md). The pre-2026-09-01 P0 #1 and #3 targeted `analytics.py`, which production
does not run in `supabase` mode — they are deferred, not done._

### Done (2026-09-02)
- ~~Reduce ScopeBar brand option cost~~ — `bbs_brand_options` RPC (`supabase/sql/029`) +
  `GET /filters/options/brands` (legacy) + `POST /api/filters/options/brands` (gateway). 4.41 MB → 3.3 kB
  unscoped. It also fixed a real bug: the old touchpoints-then-journey fallback hid the 80 brands that
  exist only in `journey_metrics` whenever touchpoints returned anything.
- ~~Make `bbs_journey_table_multi` honor `response_mode`~~ — `supabase/sql/030`, parity verified 7/7 by
  hashing the row-set each mode actually consumes.
- ~~Retire `warehouse/mapping/question_map_v0.csv`~~ — 4 read-modify-write callers, the `marts.py`
  fallback, `GET /mapping` and `POST /mapping/save` all removed; curated marts verified byte-for-byte
  equivalent in content after a forced rebuild.

### P0 (needs a product decision first)
1. Close the demographic-filter gap by adding a demographic dimension to the marts (decided approach).
   **Blocked on defining which demographic cuts the business actually uses** — that choice drives the
   mart size and the Push redesign.
2. Rework the Agent's data access — it calls the same `limit_mode=all` endpoints and picks the 45 rows
   the LLM sees by string-matching brand names in JS, not by querying.
3. Handle Storage's HTTP 429 in `analytics.py` (see Known Constraints) — today it surfaces as a 500.

### Deferred (legacy-only, revisit only if production returns to `legacy`)
- Shared per-study aggregation service in `analytics.py`.
- One-pass per-study temporal aggregation for tracking (journey + touchpoints).
- Memoization in front of `SupabaseStorage.read_bytes` (no cache today;
  `respondents.parquet` is downloaded twice per `_respondent_filter_cte` call).

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


## Vercel + Supabase Deployment Mode (Read-only Beta)
- Added internal API gateway routes in Next under `apps/web/src/app/api/*`.
- Added path rewrites so frontend can keep calling existing paths:
  - `/analytics/*` -> `/api/analytics/*`
  - `/filters/*` -> `/api/filters/*`
  - `/network` -> `/api/network`

### Data source selector
Use `BBS_DATA_SOURCE` to switch runtime:
- `legacy` (default): forwards requests to FastAPI (`LEGACY_API_BASE_URL` / `NEXT_PUBLIC_API_BASE_URL`).
- `supabase`: calls Supabase RPC contract functions.

Required Supabase RPC function names:
- `bbs_journey_table_multi`
- `bbs_touchpoints_table_multi`
- `bbs_tracking_series`
- `bbs_network`
- `bbs_filters_options_studies`
- `bbs_filters_options_taxonomy`
- `bbs_filters_options_demographics`
- `bbs_filters_options_date`

SQL contract scaffold is included at:
- `supabase/sql/001_bbs_rpc_contract.sql`

### Seeding helper
A bootstrap script is included to seed initial read-only tables from local API outputs:
- `scripts/export_supabase_seed.py`

Run this first in Supabase SQL Editor (table prerequisites):
- `supabase/sql/002_bbs_seed_tables.sql`

Then run the seed from this repository root:
```powershell
$env:SUPABASE_URL="https://<project>.supabase.co"
$env:SUPABASE_SERVICE_ROLE_KEY="<service-role-key>"
& "services\api\.venv\Scripts\python.exe" scripts/export_supabase_seed.py
```

### Supabase Auth activation
To require login in Vercel/local:
- `BBS_AUTH_MODE=supabase`
- `NEXT_PUBLIC_BBS_AUTH_MODE=supabase`
- `NEXT_PUBLIC_SUPABASE_URL=...`
- `NEXT_PUBLIC_SUPABASE_ANON_KEY=...`

Auth flow routes:
- `/auth` (magic link form)
- `/auth/callback` (session exchange)

When auth mode is enabled, middleware protects:
- `/journey`, `/demand-network`, `/tracking`, `/admin`, `/api/*`
