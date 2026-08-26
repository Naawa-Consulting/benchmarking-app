from __future__ import annotations

import pandas as pd

from app.data.warehouse import blob_exists, read_parquet_blob, write_parquet_blob

QUESTION_MAP_COLUMNS = [
    "study_id",
    "var_code",
    "question_text",
    "var_type",
    "stage",
    "brand_mode",
    "brand_value",
    "brand_extractor_id",
    "touchpoint_mode",
    "touchpoint_value",
    "touchpoint_rule_id",
    "source_stage",
    "source_brand",
    "source_touchpoint",
    "updated_at",
    "updated_by",
]


def question_map_path(study_id: str) -> str:
    return f"warehouse/raw/study_id={study_id}/question_map.parquet"


def _raw_variables_key(study_id: str) -> str:
    return f"warehouse/raw/study_id={study_id}/raw_variables.parquet"


def ensure_question_map(study_id: str) -> str:
    key = question_map_path(study_id)
    if blob_exists(key):
        return key
    variables_key = _raw_variables_key(study_id)
    if not blob_exists(variables_key):
        raise FileNotFoundError("Study not ingested yet. Ingest first.")

    df = read_parquet_blob(variables_key)
    if "question_text" not in df.columns:
        df["question_text"] = None
    if "var_type" not in df.columns:
        df["var_type"] = None

    created_at = pd.Timestamp.utcnow().isoformat()
    rows = pd.DataFrame(
        {
            "study_id": study_id,
            "var_code": df["var_code"].astype(str),
            "question_text": df["question_text"].astype(str).where(df["question_text"].notna(), None),
            "var_type": df["var_type"].astype(str).where(df["var_type"].notna(), None),
            "stage": None,
            "brand_mode": "none",
            "brand_value": None,
            "brand_extractor_id": None,
            "touchpoint_mode": "none",
            "touchpoint_value": None,
            "touchpoint_rule_id": None,
            "source_stage": "empty",
            "source_brand": "empty",
            "source_touchpoint": "empty",
            "updated_at": created_at,
            "updated_by": None,
        }
    )
    save_question_map(study_id, rows)
    return key


def load_question_map(study_id: str) -> pd.DataFrame:
    key = ensure_question_map(study_id)
    return read_parquet_blob(key)


def save_question_map(study_id: str, df: pd.DataFrame) -> str:
    key = question_map_path(study_id)
    df = df.copy()
    for column in QUESTION_MAP_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df = df[QUESTION_MAP_COLUMNS]
    write_parquet_blob(key, df)
    return key
