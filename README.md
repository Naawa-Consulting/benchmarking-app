# Benchmarking App (MVP)

A lightweight monorepo to bootstrap a Benchmarking web app for consumer-insights .sav studies.

## Architecture
- `apps/web`: Next.js (App Router) + TypeScript + Tailwind + ECharts UI
- `services/api`: FastAPI + DuckDB + Parquet data layer
- `data`: local warehouse (gitignored)
- `scripts`: helper scripts for local dev

## Prerequisites
- Node.js LTS (18+)
- Python 3.11+ recommended (Windows: 3.11/3.12 strongly preferred for prebuilt wheels)

## Setup

### 1) Backend API (Terminal A)
```powershell
cd "services\api"
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app.main:app --reload
```
API runs at `http://localhost:8000`

### 2) Frontend Web (Terminal B)
```powershell
cd "apps\web"
npm install
npm run dev
```
Web runs at `http://localhost:3000`

## Generate Demo Data
```powershell
# from repo root
.\scripts\seed_demo.ps1
```

## Troubleshooting
- If the web cannot reach the API, verify `NEXT_PUBLIC_API_BASE_URL` in `apps/web/.env.local`.
- Ensure the API is running before using the seed button.
- If PowerShell blocks scripts, run `Set-ExecutionPolicy -Scope Process Bypass`.
- If `pip install` tries to build pandas from source on Windows, install Python 3.11+ and recreate the venv with `py -3.11 -m venv .venv`.

## Local Development Notes
- DuckDB reads/writes Parquet in `./data/warehouse`.
- The `.sav` ingestion pipeline is stubbed and ready for future wiring.
