"""
Seed Supabase read-only tables from current local API outputs.

Usage (PowerShell):
  $env:SUPABASE_URL="https://<project>.supabase.co"
  $env:SUPABASE_SERVICE_ROLE_KEY="<service-role-key>"
  & "services\\api\\.venv\\Scripts\\python.exe" scripts/export_supabase_seed.py

Optional:
  $env:LOCAL_API_BASE="http://localhost:8000"
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request


LOCAL_API_BASE = os.getenv("LOCAL_API_BASE", "http://localhost:8000").rstrip("/")
SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""


def _require_env() -> None:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")


def _request_json(url: str, method: str = "GET", payload: dict | None = None, headers: dict | None = None):
    data = None
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, method=method, data=data, headers=req_headers)
    with urllib.request.urlopen(request) as response:
        body = response.read().decode("utf-8")
        return json.loads(body) if body else None


def _load_local_rows(path: str, payload: dict | None = None) -> list[dict]:
    url = f"{LOCAL_API_BASE}{path}"
    result = _request_json(url, method="POST" if payload is not None else "GET", payload=payload)
    return (result or {}).get("rows", [])

def _load_local_object(path: str) -> dict:
    url = f"{LOCAL_API_BASE}{path}"
    result = _request_json(url, method="GET")
    return result or {}

def _derive_year(study_id: str | None) -> int | None:
    if not study_id:
        return None
    match = re.search(r"(19|20)\d{2}", study_id)
    if not match:
        return None
    try:
        return int(match.group(0))
    except ValueError:
        return None


def _upsert_supabase_rows(table: str, rows: list[dict], on_conflict: str):
    if not rows:
        print(f"[seed] {table}: no rows")
        return
    query = urllib.parse.urlencode({"on_conflict": on_conflict})
    url = f"{SUPABASE_URL}/rest/v1/{table}?{query}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    _request_json(url, method="POST", payload=rows, headers=headers)
    print(f"[seed] {table}: upserted {len(rows)} rows")


def main() -> int:
    try:
        _require_env()
        print("[seed] loading local journey rows...")
        journey_rows = _load_local_rows(
            "/analytics/journey/table_multi?taxonomy_view=standard&limit_mode=all&sort_by=brand_awareness&sort_dir=desc",
            payload={},
        )
        print("[seed] loading local touchpoint rows...")
        touchpoint_rows = _load_local_rows(
            "/analytics/touchpoints/table_multi?taxonomy_view=standard&limit_mode=all&sort_by=recall&sort_dir=desc",
            payload={},
        )
        print("[seed] loading local filter options...")
        studies_payload = _load_local_object("/filters/options/studies")
        taxonomy_payload = _load_local_object("/filters/options/taxonomy?view=standard")
        demographics_payload = _load_local_object("/filters/options/demographics")

        for row in journey_rows:
            row.setdefault("year", _derive_year(row.get("study_id")))
        for row in touchpoint_rows:
            row.setdefault("year", _derive_year(row.get("study_id")))

        studies_rows = studies_payload.get("items") if isinstance(studies_payload, dict) else []
        if not isinstance(studies_rows, list):
            studies_rows = []
        if not studies_rows:
            # Fallback from journey rows if studies endpoint is empty.
            seen: set[str] = set()
            derived = []
            for row in journey_rows:
                sid = row.get("study_id")
                if not isinstance(sid, str) or sid in seen:
                    continue
                seen.add(sid)
                derived.append(
                    {
                        "study_id": sid,
                        "study_name": row.get("study_name") or sid,
                        "sector": row.get("sector"),
                        "subsector": row.get("subsector"),
                        "category": row.get("category"),
                        "has_demographics": True,
                        "has_date": True,
                    }
                )
            studies_rows = derived

        taxonomy_rows = taxonomy_payload.get("items") if isinstance(taxonomy_payload, dict) else []
        if not isinstance(taxonomy_rows, list):
            taxonomy_rows = []
        if not taxonomy_rows:
            taxonomy_unique: set[tuple[str, str, str]] = set()
            derived_taxonomy = []
            for row in journey_rows:
                sector = row.get("sector")
                subsector = row.get("subsector")
                category = row.get("category")
                if not all(isinstance(v, str) and v.strip() for v in (sector, subsector, category)):
                    continue
                key = (sector.strip(), subsector.strip(), category.strip())
                if key in taxonomy_unique:
                    continue
                taxonomy_unique.add(key)
                derived_taxonomy.append(
                    {"sector": key[0], "subsector": key[1], "category": key[2]}
                )
            taxonomy_rows = derived_taxonomy

        demographic_rows: list[dict] = []
        gender_values = demographics_payload.get("gender") if isinstance(demographics_payload, dict) else []
        nse_values = demographics_payload.get("nse") if isinstance(demographics_payload, dict) else []
        state_values = demographics_payload.get("state") if isinstance(demographics_payload, dict) else []
        age_payload = demographics_payload.get("age") if isinstance(demographics_payload, dict) else {}
        age_min = age_payload.get("min") if isinstance(age_payload, dict) else None
        age_max = age_payload.get("max") if isinstance(age_payload, dict) else None

        if isinstance(gender_values, list):
            demographic_rows.extend(
                {"gender": value, "nse": None, "state": None, "age_min": None, "age_max": None}
                for value in gender_values
                if isinstance(value, str) and value.strip()
            )
        if isinstance(nse_values, list):
            demographic_rows.extend(
                {"gender": None, "nse": value, "state": None, "age_min": None, "age_max": None}
                for value in nse_values
                if isinstance(value, str) and value.strip()
            )
        if isinstance(state_values, list):
            demographic_rows.extend(
                {"gender": None, "nse": None, "state": value, "age_min": None, "age_max": None}
                for value in state_values
                if isinstance(value, str) and value.strip()
            )
        if age_min is not None or age_max is not None:
            demographic_rows.append(
                {"gender": None, "nse": None, "state": None, "age_min": age_min, "age_max": age_max}
            )

        _upsert_supabase_rows(
            "journey_metrics",
            journey_rows,
            on_conflict="study_id,sector,subsector,category,brand",
        )
        _upsert_supabase_rows(
            "touchpoint_metrics",
            touchpoint_rows,
            on_conflict="study_id,sector,subsector,category,brand,touchpoint",
        )
        _upsert_supabase_rows(
            "study_catalog",
            studies_rows,
            on_conflict="study_id",
        )
        _upsert_supabase_rows(
            "taxonomy",
            taxonomy_rows,
            on_conflict="sector,subsector,category",
        )
        _upsert_supabase_rows(
            "demographic_options",
            demographic_rows,
            on_conflict="gender,nse,state,age_min,age_max",
        )
        print("[seed] done")
        return 0
    except (RuntimeError, urllib.error.HTTPError, urllib.error.URLError, ValueError) as exc:
        print(f"[seed] error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
