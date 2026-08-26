"""
One-time upload of the local data/ pipeline tree into Supabase Storage, so the
Render-hosted FastAPI backend (which has no persistent disk) starts with the
same ingested/mapped/curated state that today only exists on this machine.

Every file under data/landing and data/warehouse is uploaded with the same
relative path as its Storage key (data/landing/x.sav -> landing/x.sav,
data/warehouse/raw/... -> warehouse/raw/...), matching the key convention the
API now uses (see services/api/app/data/warehouse.py).

Usage (PowerShell), run once against the real Supabase project before pointing
Render at it:
  $env:SUPABASE_URL="https://<project>.supabase.co"
  $env:SUPABASE_SERVICE_ROLE_KEY="<service-role-key>"
  $env:BBS_STORAGE_BUCKET="bbs-pipeline"  # optional, this is the default
  & "services\\api\\.venv\\Scripts\\python.exe" scripts/export_storage_seed.py

The target bucket must already exist (create it once in the Supabase dashboard,
private, service-role only) before running this.
"""

from __future__ import annotations

import mimetypes
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

SUPABASE_URL = (os.getenv("SUPABASE_URL") or "").rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
BUCKET = os.getenv("BBS_STORAGE_BUCKET", "bbs-pipeline")

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = REPO_ROOT / "data"
UPLOAD_SUBDIRS = ["landing", "warehouse"]


def _require_env() -> None:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.")


def _upload(key: str, content: bytes) -> None:
    content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
    encoded_key = urllib.parse.quote(key, safe="/")
    url = f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{encoded_key}"
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Content-Type": content_type,
        "x-upsert": "true",
    }
    request = urllib.request.Request(url, method="POST", data=content, headers=headers)
    with urllib.request.urlopen(request) as response:
        response.read()


def main() -> int:
    try:
        _require_env()
    except RuntimeError as exc:
        print(f"[seed] error: {exc}", file=sys.stderr)
        return 1

    if not DATA_ROOT.exists():
        print(f"[seed] nothing to do, {DATA_ROOT} does not exist")
        return 0

    uploaded = 0
    failed = 0
    for subdir in UPLOAD_SUBDIRS:
        base = DATA_ROOT / subdir
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            key = path.relative_to(DATA_ROOT).as_posix()
            try:
                _upload(key, path.read_bytes())
                uploaded += 1
                print(f"[seed] uploaded {key}")
            except urllib.error.HTTPError as exc:
                failed += 1
                print(f"[seed] FAILED {key}: {exc.code} {exc.read().decode('utf-8', 'ignore')}", file=sys.stderr)

    print(f"[seed] done — {uploaded} uploaded, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
