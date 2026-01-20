from __future__ import annotations

import csv
import logging
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from app.data.warehouse import get_duckdb_connection, get_repo_root, load_parquet_as_view
from app.models.schemas import MartBuildResponse

logger = logging.getLogger(__name__)

router = APIRouter()


def _mapping_csv_path() -> Path:
    return get_repo_root() / "data" / "warehouse" / "mapping" / "question_map_v0.csv"


def _load_mapping_df(study_id: str) -> pd.DataFrame:
    path = _mapping_csv_path()
    if not path.exists():
        return pd.DataFrame()
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = [row for row in reader if row.get("study_id") == study_id]
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["value_true_codes"] = df["value_true_codes"].fillna("1")
    df["true_codes"] = df["value_true_codes"].astype(str).str.split("|")
    return df


@router.post("/marts/journey/build", response_model=MartBuildResponse)
def build_journey_mart(study_id: str = Query(..., description="Study id")) -> MartBuildResponse:
    logger.info("Building journey mart for %s", study_id)
    root = get_repo_root()
    mapping_df = _load_mapping_df(study_id)
    if mapping_df.empty:
        raise HTTPException(status_code=400, detail="No mapping rows found for study.")

    responses_path = root / "data" / "warehouse" / "raw" / f"study_id={study_id}" / "raw_responses.parquet"
    if not responses_path.exists():
        raise HTTPException(status_code=404, detail="Raw responses parquet not found.")

    curated_dir = root / "data" / "warehouse" / "curated" / f"study_id={study_id}"
    curated_dir.mkdir(parents=True, exist_ok=True)
    output_path = curated_dir / "fact_journey.parquet"

    conn = get_duckdb_connection()
    load_parquet_as_view(conn, "responses", str(responses_path))
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
        raise HTTPException(status_code=400, detail="No rows matched mapping criteria.")

    df.to_parquet(output_path, index=False)

    respondents = int(df["respondent_id"].nunique())
    rows = int(len(df))
    brands = int(df["brand"].nunique())
    stages = int(df["stage"].nunique())

    return MartBuildResponse(
        study_id=study_id,
        respondents=respondents,
        rows=rows,
        brands=brands,
        stages=stages,
        path=str(output_path),
    )
