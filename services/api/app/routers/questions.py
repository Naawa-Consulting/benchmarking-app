from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from app.data.rule_engine import (
    apply_rules_to_variables,
    filter_rules_by_scope,
    load_rules,
    load_study_rule_scope,
)
from app.data.warehouse import get_duckdb_connection, get_repo_root, load_parquet_as_view

router = APIRouter()


@router.get("/questions")
def list_questions(
    study_id: str = Query(..., description="Study id"),
    include_stats: bool = Query(False, description="Include mapping status and value preview"),
    limit: int = Query(200, ge=1, le=1000, description="Max questions to compute value stats for"),
) -> dict:
    variables_path = (
        get_repo_root()
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_variables.parquet"
    )
    responses_path = (
        get_repo_root()
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_responses.parquet"
    )
    if not variables_path.exists():
        raise HTTPException(status_code=404, detail="raw_variables.parquet not found for study.")

    df = pd.read_parquet(variables_path)
    if "var_code" not in df.columns:
        raise HTTPException(status_code=400, detail="raw_variables.parquet missing var_code column.")

    if "question_text" not in df.columns:
        df["question_text"] = None

    df = df[["var_code", "question_text"]].copy()
    df["var_code"] = df["var_code"].astype(str)
    df["question_text"] = df["question_text"].where(pd.notna(df["question_text"]), None)
    df = df.sort_values("var_code")

    items = df.to_dict(orient="records")

    if not include_stats:
        return {"study_id": study_id, "items": items}

    rules = load_rules()
    scope = load_study_rule_scope(study_id, rules)
    rules = filter_rules_by_scope(rules, scope)
    mapped_df, _ = apply_rules_to_variables(df, rules)
    mapping_lookup: dict[str, dict[str, str | None]] = {}
    for _, row in mapped_df.iterrows():
        var_code = str(row.get("var_code", ""))
        if var_code and var_code not in mapping_lookup:
            brand = row.get("brand")
            mapping_lookup[var_code] = {
                "stage": row.get("stage"),
                "brand": brand if isinstance(brand, str) else None,
            }

    conn = None
    if responses_path.exists():
        conn = get_duckdb_connection()
        load_parquet_as_view(conn, "responses", str(responses_path))

    def infer_type(non_null: int, non_numeric: int) -> str:
        if non_null == 0:
            return "unknown"
        if non_numeric == 0:
            return "numeric"
        if non_numeric == non_null:
            return "string"
        return "mixed"

    for index, item in enumerate(items):
        var_code = item.get("var_code", "")
        mapping = mapping_lookup.get(str(var_code))
        mapped_stage = mapping.get("stage") if mapping else None
        mapped_brand = mapping.get("brand") if mapping else None
        item["stage_mapped"] = mapped_stage is not None
        item["brand_mapped"] = mapped_brand is not None and str(mapped_brand).strip() != ""
        item["mapped_stage"] = mapped_stage
        item["mapped_brand_example"] = mapped_brand

        value_preview = None
        if conn is not None and index < limit:
            top_values = conn.execute(
                """
                SELECT value, COUNT(*) AS cnt
                FROM responses
                WHERE var_code = ?
                GROUP BY value
                ORDER BY cnt DESC
                LIMIT 5
                """,
                [var_code],
            ).fetchall()
            distinct_row = conn.execute(
                "SELECT COUNT(DISTINCT value) FROM responses WHERE var_code = ?",
                [var_code],
            ).fetchone()
            type_row = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN value IS NULL THEN 0 WHEN TRY_CAST(value AS DOUBLE) IS NULL THEN 1 ELSE 0 END) AS non_numeric,
                    SUM(CASE WHEN value IS NULL THEN 0 ELSE 1 END) AS non_null
                FROM responses
                WHERE var_code = ?
                """,
                [var_code],
            ).fetchone()
            non_numeric = int(type_row[0]) if type_row else 0
            non_null = int(type_row[1]) if type_row else 0
            value_preview = {
                "type": infer_type(non_null, non_numeric),
                "top_values": [
                    {"value": str(row[0]), "count": int(row[1])} for row in top_values
                ],
                "distinct": int(distinct_row[0]) if distinct_row else 0,
            }

        item["value_preview"] = value_preview

    return {"study_id": study_id, "items": items}
