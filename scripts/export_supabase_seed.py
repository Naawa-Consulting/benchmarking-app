"""
Seed Supabase read-only tables from current local API outputs.

Usage:
  set SUPABASE_URL=...
  set SUPABASE_SERVICE_ROLE_KEY=...
  python scripts/export_supabase_seed.py

Optional:
  set LOCAL_API_BASE=http://localhost:8000
"""

from __future__ import annotations

import json
import os
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
            "/analytics/journey/table_multi?limit_mode=all&sort_by=brand_awareness&sort_dir=desc",
            payload={},
        )
        print("[seed] loading local touchpoint rows...")
        touchpoint_rows = _load_local_rows(
            "/analytics/touchpoints/table_multi?limit_mode=all&sort_by=recall&sort_dir=desc",
            payload={},
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
        print("[seed] done")
        return 0
    except (RuntimeError, urllib.error.HTTPError, urllib.error.URLError, ValueError) as exc:
        print(f"[seed] error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
