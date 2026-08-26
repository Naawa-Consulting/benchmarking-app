from __future__ import annotations

import logging
import re
from typing import Any

import pandas as pd
import pyreadstat
from pandas.api.types import is_categorical_dtype, is_numeric_dtype

from app.data.demographics import build_value_labels_frame, respondents_key, value_labels_key
from app.data.study_config import load_or_create_study_config
from app.data.warehouse import (
    blob_exists,
    delete_blob,
    download_to_tempfile,
    list_blob_names,
    write_parquet_blob,
)

logger = logging.getLogger(__name__)

LANDING_PREFIX = "landing"
RAW_PREFIX = "warehouse/raw"


def _slugify_study_id(name: str) -> str:
    value = name.strip().lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_]", "", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_") or "study"


def _landing_sav_names() -> list[str]:
    return [name for name in list_blob_names(LANDING_PREFIX) if name.lower().endswith(".sav")]


def find_landing_key(study_id: str) -> str | None:
    for name in _landing_sav_names():
        if _slugify_study_id(name[: -len(".sav")]) == study_id:
            return f"{LANDING_PREFIX}/{name}"
    return None


def _infer_var_type(series: pd.Series, has_value_labels: bool) -> str:
    if is_numeric_dtype(series):
        return "numeric"
    if is_categorical_dtype(series) or has_value_labels:
        return "categorical"
    return "string"


def _build_variables_frame(df: pd.DataFrame, meta: Any, study_id: str) -> pd.DataFrame:
    column_labels = list(getattr(meta, "column_labels", []))
    column_names = list(df.columns)
    value_labels = getattr(meta, "variable_value_labels", {}) or {}

    rows = []
    for idx, var_code in enumerate(column_names):
        label = column_labels[idx] if idx < len(column_labels) and column_labels[idx] else var_code
        has_value_labels = var_code in value_labels
        var_type = _infer_var_type(df[var_code], has_value_labels)
        rows.append(
            {
                "study_id": study_id,
                "var_code": str(var_code),
                "question_text": str(label),
                "var_type": var_type,
                "has_value_labels": bool(has_value_labels),
            }
        )
    return pd.DataFrame(rows)

def _build_respondents_frame(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    respondent_cfg = (config.get("respondent_id") or {}) if config else {}
    weight_cfg = (config.get("weight") or {}) if config else {}

    respondent_var = respondent_cfg.get("var_code")
    weight_var = weight_cfg.get("var_code")

    if respondent_var and respondent_var != "__index__" and respondent_var in df.columns:
        respondent_series = df[respondent_var]
    else:
        respondent_series = pd.Series(df.index + 1, name="respondent_id")

    if weight_var and weight_var != "__default__" and weight_var in df.columns:
        weight_series = df[weight_var]
    else:
        weight_series = pd.Series([weight_cfg.get("default", 1.0)] * len(df), name="weight")

    respondent_series = respondent_series.astype("string").fillna(pd.NA)
    weight_series = pd.to_numeric(weight_series, errors="coerce").fillna(1.0)
    weight_series = weight_series.where(weight_series > 0, 1.0)

    return pd.DataFrame({"respondent_id": respondent_series, "weight": weight_series})


def _build_responses_frame(df: pd.DataFrame, study_id: str, config: dict) -> pd.DataFrame:
    respondent_cfg = (config.get("respondent_id") or {}) if config else {}
    weight_cfg = (config.get("weight") or {}) if config else {}

    respondent_var = respondent_cfg.get("var_code")
    weight_var = weight_cfg.get("var_code")

    if respondent_var and respondent_var != "__index__" and respondent_var in df.columns:
        respondent_series = df[respondent_var]
    else:
        respondent_series = pd.Series(df.index + 1, name="respondent_id")

    if weight_var and weight_var != "__default__" and weight_var in df.columns:
        weight_series = df[weight_var]
    else:
        weight_series = pd.Series([weight_cfg.get("default", 1.0)] * len(df), name="weight")

    respondent_series = respondent_series.astype("string").fillna(pd.NA)
    weight_series = pd.to_numeric(weight_series, errors="coerce").fillna(1.0)
    weight_series = weight_series.where(weight_series > 0, 1.0)

    df_payload = df.copy()
    df_payload.insert(0, "respondent_id", respondent_series)
    df_payload.insert(1, "weight", weight_series)

    long_df = df_payload.melt(
        id_vars=["respondent_id", "weight"],
        var_name="var_code",
        value_name="value",
    )
    long_df.insert(0, "study_id", study_id)
    # Coerce mixed SPSS types to string for a stable Parquet schema.
    long_df["value"] = long_df["value"].astype("string")
    long_df["created_at"] = pd.Timestamp.utcnow()
    return long_df


def _ingest_one(landing_key: str, study_id: str) -> dict[str, Any]:
    logger.info("Reading .sav file: %s", landing_key)
    with download_to_tempfile(landing_key, suffix=".sav") as tmp_path:
        df, meta = pyreadstat.read_sav(tmp_path)
    logger.info("Building raw parquet outputs for %s", study_id)

    config = load_or_create_study_config(study_id, df.columns)
    responses_df = _build_responses_frame(df, study_id, config)
    variables_df = _build_variables_frame(df, meta, study_id)
    labels_df = build_value_labels_frame(meta, study_id)
    respondents_df = _build_respondents_frame(df, config)

    write_parquet_blob(f"{RAW_PREFIX}/study_id={study_id}/raw_responses.parquet", responses_df)
    write_parquet_blob(f"{RAW_PREFIX}/study_id={study_id}/raw_variables.parquet", variables_df)
    labels_key = value_labels_key(study_id)
    if labels_df.empty:
        if blob_exists(labels_key):
            delete_blob(labels_key)
    else:
        write_parquet_blob(labels_key, labels_df)
    write_parquet_blob(respondents_key(study_id), respondents_df)

    return {"rows": int(len(responses_df)), "variables": int(len(variables_df))}


def ensure_raw_from_landing() -> dict[str, list[dict[str, Any]]]:
    logger.info("Scanning landing prefix: %s", LANDING_PREFIX)

    processed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for name in sorted(_landing_sav_names()):
        study_id = _slugify_study_id(name[: -len(".sav")])
        landing_key = f"{LANDING_PREFIX}/{name}"
        responses_key = f"{RAW_PREFIX}/study_id={study_id}/raw_responses.parquet"
        variables_key = f"{RAW_PREFIX}/study_id={study_id}/raw_variables.parquet"

        if blob_exists(responses_key) and blob_exists(variables_key):
            logger.info("Skipping %s (raw parquet exists)", study_id)
            skipped.append(
                {
                    "study_id": study_id,
                    "file": name,
                    "status": "skipped",
                    "reason": "already exists",
                }
            )
            continue

        try:
            result = _ingest_one(landing_key, study_id)
            processed.append(
                {
                    "study_id": study_id,
                    "file": name,
                    "rows": result["rows"],
                    "variables": result["variables"],
                    "status": "processed",
                }
            )
        except Exception as exc:
            logger.exception("Failed to ingest %s", name)
            errors.append(
                {
                    "study_id": study_id,
                    "file": name,
                    "status": "error",
                    "error": str(exc),
                }
            )

    return {"processed": processed, "skipped": skipped, "errors": errors}


def rebuild_raw_for_study(study_id: str, force: bool = False) -> dict[str, Any]:
    landing_key = find_landing_key(study_id)
    if not landing_key:
        return {"status": "error", "reason": "landing .sav not found"}

    responses_key = f"{RAW_PREFIX}/study_id={study_id}/raw_responses.parquet"
    variables_key = f"{RAW_PREFIX}/study_id={study_id}/raw_variables.parquet"

    if blob_exists(responses_key) and blob_exists(variables_key) and not force:
        return {"status": "skipped", "reason": "already exists"}

    try:
        result = _ingest_one(landing_key, study_id)
        return {"status": "ok", "rows": result["rows"], "variables": result["variables"]}
    except Exception as exc:
        logger.exception("Failed to rebuild raw for %s", study_id)
        return {"status": "error", "reason": str(exc)}


def ingest_landing_files() -> list[dict[str, Any]]:
    summary = ensure_raw_from_landing()
    return summary["processed"] + summary["skipped"] + summary["errors"]
