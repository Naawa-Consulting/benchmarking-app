from __future__ import annotations

import json
import logging
import re
from pathlib import Path

import pandas as pd
import pyreadstat
from fastapi import APIRouter, HTTPException, Query, Request

from app.data.study_config import (
    load_or_create_study_config,
    load_study_config,
    save_study_config,
)
from app.data.warehouse import get_repo_root

logger = logging.getLogger(__name__)

router = APIRouter()


def _raw_paths(study_id: str) -> tuple[Path, Path]:
    base = get_repo_root() / "data" / "warehouse" / "raw" / f"study_id={study_id}"
    return base / "raw_variables.parquet", base / "raw_responses.parquet"


def _landing_sav_path(study_id: str) -> Path | None:
    landing_dir = get_repo_root() / "data" / "landing"
    for path in landing_dir.glob("*.sav"):
        if _slugify(path.stem) == study_id:
            return path
    return None


def _slugify(value: str) -> str:
    normalized = value.strip().lower()
    normalized = re.sub(r"\s+", "_", normalized)
    normalized = re.sub(r"[^a-z0-9_]", "", normalized)
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_") or "study"


@router.get("/study-config")
def get_study_config(study_id: str = Query(..., description="Study id")) -> dict:
    variables_path, _ = _raw_paths(study_id)
    if variables_path.exists():
        df = pd.read_parquet(variables_path, columns=["var_code"])
        config = load_or_create_study_config(study_id, df["var_code"].tolist())
        return config

    landing_path = _landing_sav_path(study_id)
    if landing_path and landing_path.exists():
        df, _ = pyreadstat.read_sav(landing_path)
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

    variables_path, _ = _raw_paths(study_id)
    if not variables_path.exists():
        raise HTTPException(status_code=404, detail="raw_variables.parquet not found for study.")

    df = pd.read_parquet(variables_path, columns=["var_code"])
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
    path = save_study_config(study_id, config)
    logger.info("Saved study config: %s", path)
    return config


@router.get("/study/variables")
def list_study_variables(study_id: str = Query(..., description="Study id")) -> dict:
    variables_path, _ = _raw_paths(study_id)
    if variables_path.exists():
        df = pd.read_parquet(variables_path)
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

    landing_path = _landing_sav_path(study_id)
    if landing_path and landing_path.exists():
        df, meta = pyreadstat.read_sav(landing_path)
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
    _, responses_path = _raw_paths(study_id)
    if not responses_path.exists():
        raise HTTPException(status_code=404, detail="raw_responses.parquet not found for study.")

    import duckdb

    conn = duckdb.connect()
    conn.execute(
        f"CREATE OR REPLACE VIEW responses AS SELECT * FROM read_parquet('{responses_path}')"
    )
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
