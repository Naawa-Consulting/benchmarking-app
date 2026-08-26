from __future__ import annotations

import logging

import duckdb
import pyreadstat
from fastapi import APIRouter, HTTPException, Query, Request

from app.data.ingest_from_landing import find_landing_key
from app.data.study_config import (
    load_or_create_study_config,
    load_study_config,
    save_study_config,
)
from app.data.warehouse import blob_exists, download_to_tempfile, load_parquet_as_view, read_parquet_blob

logger = logging.getLogger(__name__)

router = APIRouter()


def _raw_keys(study_id: str) -> tuple[str, str]:
    base = f"warehouse/raw/study_id={study_id}"
    return f"{base}/raw_variables.parquet", f"{base}/raw_responses.parquet"


@router.get("/study-config")
def get_study_config(study_id: str = Query(..., description="Study id")) -> dict:
    variables_key, _ = _raw_keys(study_id)
    if blob_exists(variables_key):
        df = read_parquet_blob(variables_key, columns=["var_code"])
        config = load_or_create_study_config(study_id, df["var_code"].tolist())
        return config

    landing_key = find_landing_key(study_id)
    if landing_key:
        with download_to_tempfile(landing_key, suffix=".sav") as tmp_path:
            df, _ = pyreadstat.read_sav(tmp_path)
        config = load_or_create_study_config(study_id, df.columns)
        return config

    config = load_study_config(study_id)
    if config:
        return config
    raise HTTPException(status_code=404, detail="Study config not found.")


@router.post("/study-config")
async def save_study_config_endpoint(
    study_id: str = Query(..., description="Study id"),
    request: Request = ...,
) -> dict:
    payload = await request.json()
    respondent_id_var = payload.get("respondent_id_var")
    weight_var = payload.get("weight_var")
    source = payload.get("source", "manual")

    variables_key, _ = _raw_keys(study_id)
    if not blob_exists(variables_key):
        raise HTTPException(status_code=404, detail="raw_variables.parquet not found for study.")

    df = read_parquet_blob(variables_key, columns=["var_code"])
    valid_vars = set(df["var_code"].astype(str))

    if respondent_id_var != "__index__" and respondent_id_var not in valid_vars:
        raise HTTPException(status_code=400, detail="respondent_id_var not found in variables.")
    if weight_var != "__default__" and weight_var not in valid_vars:
        raise HTTPException(status_code=400, detail="weight_var not found in variables.")

    config = {
        "study_id": study_id,
        "respondent_id": {
            "source": source,
            "var_code": respondent_id_var,
        },
        "weight": {
            "source": source,
            "var_code": weight_var,
            "default": 1.0,
        },
    }
    key = save_study_config(study_id, config)
    logger.info("Saved study config: %s", key)
    return config


@router.get("/study/variables")
def list_study_variables(study_id: str = Query(..., description="Study id")) -> dict:
    variables_key, _ = _raw_keys(study_id)
    if blob_exists(variables_key):
        df = read_parquet_blob(variables_key)
        if "question_text" not in df.columns:
            df["question_text"] = None
        if "var_type" not in df.columns:
            df["var_type"] = None
        items = [
            {
                "var_code": str(row.get("var_code")),
                "label": row.get("question_text"),
                "type": row.get("var_type") or "unknown",
            }
            for row in df.to_dict(orient="records")
        ]
        return {"study_id": study_id, "variables": items}

    landing_key = find_landing_key(study_id)
    if landing_key:
        with download_to_tempfile(landing_key, suffix=".sav") as tmp_path:
            df, meta = pyreadstat.read_sav(tmp_path)
        column_labels = list(getattr(meta, "column_labels", []))
        items = []
        for idx, var_code in enumerate(df.columns):
            label = column_labels[idx] if idx < len(column_labels) and column_labels[idx] else None
            items.append(
                {
                    "var_code": str(var_code),
                    "label": label,
                    "type": "unknown",
                }
            )
        return {"study_id": study_id, "variables": items}

    raise HTTPException(status_code=404, detail="Study variables not found.")


@router.get("/study/base/preview")
def base_preview(
    study_id: str = Query(..., description="Study id"),
    n: int = Query(5, ge=1, le=50, description="Rows to preview"),
) -> dict:
    _, responses_key = _raw_keys(study_id)
    if not blob_exists(responses_key):
        raise HTTPException(status_code=404, detail="raw_responses.parquet not found for study.")

    conn = duckdb.connect()
    load_parquet_as_view(conn, "responses", responses_key)
    rows = conn.execute(
        """
        SELECT respondent_id, weight
        FROM responses
        WHERE respondent_id IS NOT NULL
        LIMIT ?
        """,
        [n],
    ).fetchall()
    return {
        "study_id": study_id,
        "rows": [{"respondent_id": row[0], "weight": row[1]} for row in rows],
    }
