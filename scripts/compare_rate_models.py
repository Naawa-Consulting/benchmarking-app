"""TEMP — parity check for the rate-model SQL migration (Fase 2 of the benchmark
architecture plan, 2026-08-31). Compares the old parquet-scan rate models against the
new bbs_rate_model_training_stats SQL RPC, bucket by bucket, before flipping
BBS_RATE_MODEL_SOURCE to "sql" in Render. Delete this script (and its BITACORA.md entry)
once the cutover is confirmed and no longer needs re-checking.

Run from the repo root with the services/api venv:
    services/api/.venv/Scripts/python.exe scripts/compare_rate_models.py
"""
import os
import sys

sys.path.insert(0, "services/api")


def _load_env(path: str) -> None:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ[key.strip()] = value.strip().strip('"').strip("'")


_load_env("services/api/.env")

from app.routers import analytics  # noqa: E402

INT_FIELDS_BY_FAMILY = {
    "consideration": ("n_ca", "n_pc", "comparisons_total", "comparisons_p_gt_c"),
    "satisfaction": ("n_sp", "n_sr", "comparisons_total", "comparisons_r_gt_s"),
    "csat": ("n_delta_sc", "n_delta_cr", "comparisons_total", "comparisons_csat_gt_sat"),
}
FLOAT_FIELDS_BY_FAMILY = {
    "consideration": ("r_ca", "r_pc"),
    "satisfaction": ("r_sp", "r_sr"),
    "csat": ("delta_sc", "delta_cr"),
}
FLOAT_TOLERANCE = 1e-6


def _compare_family(name: str, parquet_rates: dict, sql_rates: dict) -> int:
    mismatches = 0
    parquet_keys = set(parquet_rates.keys())
    sql_keys = set(sql_rates.keys())

    only_parquet = parquet_keys - sql_keys
    only_sql = sql_keys - parquet_keys
    if only_parquet:
        print(f"  [{name}] {len(only_parquet)} bucket(s) only in parquet (e.g. curated but never pushed):")
        for key in sorted(only_parquet, key=str)[:5]:
            print(f"    {key}")
    if only_sql:
        print(f"  [{name}] {len(only_sql)} bucket(s) only in SQL (e.g. pushed but removed from Storage):")
        for key in sorted(only_sql, key=str)[:5]:
            print(f"    {key}")

    for key in sorted(parquet_keys & sql_keys, key=str):
        p, s = parquet_rates[key], sql_rates[key]
        for field in INT_FIELDS_BY_FAMILY[name]:
            if int(p.get(field) or 0) != int(s.get(field) or 0):
                mismatches += 1
                print(f"  MISMATCH [{name}] {key} field={field}: parquet={p.get(field)} sql={s.get(field)}")
        for field in FLOAT_FIELDS_BY_FAMILY[name]:
            pv, sv = p.get(field), s.get(field)
            if pv is None and sv is None:
                continue
            if pv is None or sv is None or abs(float(pv) - float(sv)) > FLOAT_TOLERANCE:
                mismatches += 1
                print(f"  MISMATCH [{name}] {key} field={field}: parquet={pv} sql={sv}")

    return mismatches


def main() -> None:
    study_ids = analytics._discover_curated_studies()
    print(f"Comparing rate models across {len(study_ids)} curated studies vs. journey_metrics...\n")

    os.environ["BBS_RATE_MODEL_SOURCE"] = "parquet"
    parquet_consideration, _ = analytics._consideration_rates_from_parquet(study_ids)
    parquet_satisfaction, _ = analytics._satisfaction_rates_from_parquet(study_ids)
    parquet_csat, _ = analytics._csat_rates_from_parquet(study_ids)

    os.environ["BBS_RATE_MODEL_SOURCE"] = "sql"
    analytics._RATE_MODEL_SQL_STATS_CACHE.clear()
    sql_stats = analytics._load_rate_model_training_stats_from_sql()
    if sql_stats is None:
        print("SQL RPC returned no usable data — aborting comparison.")
        return
    sql_consideration, _ = analytics._consideration_rates_from_sql_rows(sql_stats.get("consideration") or [])
    sql_satisfaction, _ = analytics._satisfaction_rates_from_sql_rows(sql_stats.get("satisfaction") or [])
    sql_csat, _ = analytics._csat_rates_from_sql_rows(sql_stats.get("csat") or [])

    total_mismatches = 0
    total_mismatches += _compare_family("consideration", parquet_consideration, sql_consideration)
    total_mismatches += _compare_family("satisfaction", parquet_satisfaction, sql_satisfaction)
    total_mismatches += _compare_family("csat", parquet_csat, sql_csat)

    print(f"\n{'PASS' if total_mismatches == 0 else 'FAIL'}: {total_mismatches} field mismatch(es) total.")


if __name__ == "__main__":
    main()
