from __future__ import annotations

import contextlib
import io
import json
import os
import tempfile
from typing import Iterator

import duckdb
import pandas as pd

from app.storage.blob import StorageNotFoundError, get_storage

# Key layout mirrors the old local `data/` tree exactly (just as a Storage key
# instead of a filesystem path), to keep every call site's path-building logic
# recognizable: "landing/{study_id}.sav", "warehouse/raw/study_id={id}/...",
# "warehouse/curated/study_id={id}/...", "warehouse/mapping/...", etc.


def read_parquet_blob(key: str, columns: list[str] | None = None) -> pd.DataFrame:
    data = get_storage().read_bytes(key)
    return pd.read_parquet(io.BytesIO(data), columns=columns)


def write_parquet_blob(key: str, df: pd.DataFrame) -> None:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    get_storage().write_bytes(key, buf.getvalue())


def read_csv_blob(key: str) -> pd.DataFrame | None:
    try:
        data = get_storage().read_bytes(key)
    except StorageNotFoundError:
        return None
    return pd.read_csv(io.BytesIO(data))


def write_csv_blob(key: str, df: pd.DataFrame) -> None:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    get_storage().write_bytes(key, buf.getvalue().encode("utf-8"), content_type="text/csv")


def read_json_blob(key: str, default=None):
    try:
        data = get_storage().read_bytes(key)
    except StorageNotFoundError:
        return default
    try:
        return json.loads(data.decode("utf-8"))
    except json.JSONDecodeError:
        return json.loads(data.decode("utf-8-sig"))


def write_json_blob(key: str, payload) -> None:
    data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    get_storage().write_bytes(key, data, content_type="application/json")


def blob_exists(key: str) -> bool:
    return get_storage().exists(key)


def delete_blob(key: str) -> None:
    get_storage().delete(key)


def list_blob_names(prefix: str) -> list[str]:
    """Immediate child names under `prefix` — replaces `Path.glob('*')`-style listing."""
    return get_storage().list_names(prefix)


def delete_prefix(prefix: str) -> list[str]:
    """Delete every object under `prefix` — replaces `shutil.rmtree(dir)`.

    Returns the full keys that were removed. Only recurses one level (the call
    sites here only ever delete a flat `study_id=X/` folder of files), so nested
    sub-folders under `prefix` are not walked.
    """
    storage = get_storage()
    removed: list[str] = []
    for name in storage.list_names(prefix):
        key = f"{prefix.rstrip('/')}/{name}"
        storage.delete(key)
        removed.append(key)
    return removed


def list_study_ids(base_prefix: str, marker_filename: str) -> list[str]:
    """Study ids under `base_prefix/study_id=X/` that contain `marker_filename`.

    Replaces the old `for path in root.glob("study_id=*"): if (path / marker).exists()`
    discovery pattern used to enumerate studies with raw/curated data ready.
    """
    storage = get_storage()
    study_ids: list[str] = []
    for name in storage.list_names(base_prefix):
        if not name.startswith("study_id="):
            continue
        study_id = name[len("study_id=") :]
        marker_key = f"{base_prefix.rstrip('/')}/{name}/{marker_filename}"
        if storage.exists(marker_key):
            study_ids.append(study_id)
    return sorted(study_ids)


@contextlib.contextmanager
def download_to_tempfile(key: str, suffix: str = "") -> Iterator[str]:
    """Download a blob to a local temp file for libraries that need a real path
    (pyreadstat/.sav reads). The temp file is scoped to this context manager and
    always cleaned up — it never needs to survive past the current request."""
    data = get_storage().read_bytes(key)
    fd, tmp_path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        yield tmp_path
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def load_parquet_as_view(conn: duckdb.DuckDBPyConnection, view_name: str, key: str) -> None:
    df = read_parquet_blob(key)
    conn.register(view_name, df)
