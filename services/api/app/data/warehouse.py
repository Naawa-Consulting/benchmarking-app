from pathlib import Path

import duckdb


def get_repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def ensure_data_dirs() -> dict[str, Path]:
    base = get_repo_root() / "data" / "warehouse"
    raw_dir = base / "raw"
    curated_dir = base / "curated"
    mapping_dir = base / "mapping"
    raw_dir.mkdir(parents=True, exist_ok=True)
    curated_dir.mkdir(parents=True, exist_ok=True)
    mapping_dir.mkdir(parents=True, exist_ok=True)
    return {"base": base, "raw": raw_dir, "curated": curated_dir, "mapping": mapping_dir}


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    dirs = ensure_data_dirs()
    return duckdb.connect(database=str(dirs["base"] / "warehouse.duckdb"))


def load_parquet_as_view(conn: duckdb.DuckDBPyConnection, view_name: str, parquet_path: str) -> None:
    conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{parquet_path}')")
