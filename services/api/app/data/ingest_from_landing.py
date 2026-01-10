from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
import pyreadstat
from pandas.api.types import is_categorical_dtype, is_numeric_dtype

from app.data.warehouse import get_repo_root

logger = logging.getLogger(__name__)


def _slugify_study_id(name: str) -> str:
    value = name.strip().lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_]", "", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_") or "study"


def _ensure_dirs(base_data_dir: Path) -> tuple[Path, Path]:
    landing = base_data_dir / "landing"
    raw_root = base_data_dir / "warehouse" / "raw"
    landing.mkdir(parents=True, exist_ok=True)
    raw_root.mkdir(parents=True, exist_ok=True)
    return landing, raw_root


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


def _build_responses_frame(df: pd.DataFrame, study_id: str) -> pd.DataFrame:
    if "respondent_id" in df.columns:
        respondent_series = df["respondent_id"]
        df_payload = df.copy()
    else:
        respondent_series = pd.Series(df.index + 1, name="respondent_id")
        df_payload = df.copy()
        df_payload.insert(0, "respondent_id", respondent_series)

    long_df = df_payload.melt(
        id_vars=["respondent_id"],
        var_name="var_code",
        value_name="value",
    )
    long_df.insert(0, "study_id", study_id)
    # Coerce mixed SPSS types to string for a stable Parquet schema.
    long_df["value"] = long_df["value"].astype("string")
    long_df["created_at"] = pd.Timestamp.utcnow()
    return long_df


def ensure_raw_from_landing(base_data_dir: Path) -> dict[str, list[dict[str, Any]]]:
    landing_dir, raw_root = _ensure_dirs(base_data_dir)
    logger.info("Scanning landing folder: %s", landing_dir)

    processed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for path in sorted(landing_dir.glob("*.sav")):
        study_id = _slugify_study_id(path.stem)
        study_dir = raw_root / f"study_id={study_id}"
        responses_path = study_dir / "raw_responses.parquet"
        variables_path = study_dir / "raw_variables.parquet"

        if responses_path.exists() and variables_path.exists():
            logger.info("Skipping %s (raw parquet exists)", study_id)
            skipped.append(
                {
                    "study_id": study_id,
                    "file": path.name,
                    "status": "skipped",
                    "reason": "already exists",
                }
            )
            continue

        try:
            logger.info("Reading .sav file: %s", path)
            df, meta = pyreadstat.read_sav(path)
            logger.info("Building raw parquet outputs for %s", study_id)

            study_dir.mkdir(parents=True, exist_ok=True)
            responses_df = _build_responses_frame(df, study_id)
            variables_df = _build_variables_frame(df, meta, study_id)

            responses_df.to_parquet(responses_path, index=False)
            variables_df.to_parquet(variables_path, index=False)

            processed.append(
                {
                    "study_id": study_id,
                    "file": path.name,
                    "rows": int(len(responses_df)),
                    "variables": int(len(variables_df)),
                    "status": "processed",
                }
            )
        except Exception as exc:
            logger.exception("Failed to ingest %s", path)
            errors.append(
                {
                    "study_id": study_id,
                    "file": path.name,
                    "status": "error",
                    "error": str(exc),
                }
            )

    return {"processed": processed, "skipped": skipped, "errors": errors}


def ingest_landing_files() -> list[dict[str, Any]]:
    base_data_dir = get_repo_root() / "data"
    summary = ensure_raw_from_landing(base_data_dir)
    return summary["processed"] + summary["skipped"] + summary["errors"]
