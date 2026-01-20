from __future__ import annotations

import csv
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from app.data.ingest_from_landing import ensure_raw_from_landing, rebuild_raw_for_study
from app.data.rule_engine import (
    apply_rules_to_variables,
    filter_rules_by_scope,
    load_rules,
    load_study_rule_scope,
)
from app.data.warehouse import get_repo_root

router = APIRouter()


def _mapping_csv_path() -> Path:
    return get_repo_root() / "data" / "warehouse" / "mapping" / "question_map_v0.csv"


def _mapping_rows_for_study(study_id: str) -> list[dict]:
    path = _mapping_csv_path()
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [row for row in reader if row.get("study_id") == study_id]


@router.post("/pipeline/journey/ensure")
def ensure_journey_pipeline(
    study_id: str = Query(..., description="Study id"),
    sync_raw: bool = Query(True, description="Sync raw from landing"),
    force: bool = Query(False, description="Force rebuild curated mart"),
) -> dict:
    root = get_repo_root()
    base_data_dir = root / "data"

    synced_raw = False
    errors: list[str] = []
    if sync_raw:
        summary = ensure_raw_from_landing(base_data_dir)
        synced_raw = True
        for err in summary.get("errors", []):
            errors.append(f"{err.get('study_id')}: {err.get('error')}")

    variables_path = (
        base_data_dir / "warehouse" / "raw" / f"study_id={study_id}" / "raw_variables.parquet"
    )
    if not variables_path.exists():
        raise HTTPException(status_code=404, detail="raw_variables.parquet not found for study.")

    rules = load_rules()
    scope = load_study_rule_scope(study_id, rules)
    rules = filter_rules_by_scope(rules, scope)
    df_vars = pd.read_parquet(variables_path)
    mapped_df, stats = apply_rules_to_variables(df_vars, rules)

    mapping_path = _mapping_csv_path()
    mapping_path.parent.mkdir(parents=True, exist_ok=True)
    existing_rows: list[dict] = []
    if mapping_path.exists():
        existing_rows = list(pd.read_csv(mapping_path).to_dict(orient="records"))
    remaining = [row for row in existing_rows if row.get("study_id") != study_id]
    mapped_rows = mapped_df.copy()
    mapped_rows.insert(0, "study_id", study_id)
    merged_rows = remaining + mapped_rows.to_dict(orient="records")
    pd.DataFrame(merged_rows).to_csv(mapping_path, index=False)

    curated_path = (
        base_data_dir
        / "warehouse"
        / "curated"
        / f"study_id={study_id}"
        / "fact_journey.parquet"
    )
    curated_path.parent.mkdir(parents=True, exist_ok=True)

    curated_status = "skipped" if curated_path.exists() and not force else "ok"
    if curated_status == "ok":
        responses_path = (
            base_data_dir / "warehouse" / "raw" / f"study_id={study_id}" / "raw_responses.parquet"
        )
        if not responses_path.exists():
            raise HTTPException(status_code=404, detail="raw_responses.parquet not found for study.")

        mapping_df = mapped_df.copy()
        mapping_df.insert(0, "study_id", study_id)
        mapping_df["value_true_codes"] = mapping_df["value_true_codes"].fillna(
            rules.get("defaults", {}).get("value_true_codes", "1")
        )
        mapping_df["true_codes"] = mapping_df["value_true_codes"].astype(str).str.split("|")

        import duckdb

        conn = duckdb.connect()
        conn.execute(
            f"CREATE OR REPLACE VIEW responses AS SELECT * FROM read_parquet('{responses_path}')"
        )
        conn.register("mapping", mapping_df)
        weight_exists = (
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'responses' AND column_name = 'weight'
                """
            ).fetchone()[0]
            > 0
        )
        weight_expr = "COALESCE(TRY_CAST(r.weight AS DOUBLE), 1.0)" if weight_exists else "1.0"
        query = """
            SELECT
                r.study_id,
                r.respondent_id,
                m.stage,
                m.brand,
                m.touchpoint,
                {weight_expr} AS weight,
                TRY_CAST(r.value AS INTEGER) AS value_raw,
                CASE
                    WHEN list_contains(m.true_codes, CAST(r.value AS VARCHAR)) THEN 1
                    ELSE 0
                END AS value
            FROM responses r
            INNER JOIN mapping m
                ON r.var_code = m.var_code
                AND r.study_id = m.study_id
        """
        df = conn.execute(query.format(weight_expr=weight_expr)).df()
        if df.empty:
            curated_status = "error"
            errors.append("No rows matched mapping criteria.")
        else:
            df.to_parquet(curated_path, index=False)

    return {
        "study_id": study_id,
        "synced_raw": synced_raw,
        "mapping": {
            "status": "ok",
            "mapped_rows": stats.get("mapped_rows", 0),
            "unmapped_rows": stats.get("unmapped_rows", 0),
        },
        "curated": {
            "status": curated_status,
            "path": str(curated_path),
        },
        "errors": errors,
    }


@router.get("/pipeline/journey/status")
def journey_pipeline_status(study_id: str = Query(..., description="Study id")) -> dict:
    root = get_repo_root()
    base_data_dir = root / "data"
    raw_dir = base_data_dir / "warehouse" / "raw" / f"study_id={study_id}"
    raw_ready = (raw_dir / "raw_responses.parquet").exists() and (raw_dir / "raw_variables.parquet").exists()
    demographics_ready = (raw_dir / "respondents.parquet").exists()

    mapping_rows = _mapping_rows_for_study(study_id)
    mapping_ready = len(mapping_rows) > 0

    curated_path = (
        base_data_dir / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    )
    curated_ready = curated_path.exists()

    return {
        "study_id": study_id,
        "raw_ready": raw_ready,
        "mapping_ready": mapping_ready,
        "curated_ready": curated_ready,
        "demographics_ready": demographics_ready,
        "paths": {
            "raw_dir": str(raw_dir),
            "mapping_csv": str(_mapping_csv_path()),
            "curated_path": str(curated_path),
        },
    }


@router.post("/pipeline/base/rebuild")
def rebuild_base_pipeline(
    study_id: str = Query(..., description="Study id"),
    force: bool = Query(False, description="Force rebuild raw"),
) -> dict:
    base_data_dir = get_repo_root() / "data"
    raw_summary = rebuild_raw_for_study(base_data_dir, study_id, force=force)

    curated_path = (
        base_data_dir
        / "warehouse"
        / "curated"
        / f"study_id={study_id}"
        / "fact_journey.parquet"
    )
    curated_status = "skipped"
    if curated_path.exists():
        try:
            ensure_journey_pipeline(study_id=study_id, sync_raw=False, force=True)
            curated_status = "ok"
        except HTTPException:
            curated_status = "error"

    return {
        "study_id": study_id,
        "raw": raw_summary,
        "curated": {"status": curated_status},
    }
