from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from app.data.warehouse import get_repo_root

logger = logging.getLogger(__name__)


def _config_path(study_id: str) -> Path:
    return (
        get_repo_root()
        / "data"
        / "warehouse"
        / "demographics"
        / f"study_id={study_id}.json"
    )


def load_demographics_config(study_id: str) -> dict:
    path = _config_path(study_id)
    if not path.exists():
        return {
            "study_id": study_id,
            "date": {"mode": "none", "var_code": None, "constant": None},
            "gender_var": None,
            "age_var": None,
            "nse_var": None,
            "state_var": None,
        }
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def normalize_demographics_config(config: dict) -> dict:
    if "date" not in config:
        date_var = config.get("date_var")
        config = {
            "study_id": config.get("study_id"),
            "date": {
                "mode": "var" if date_var else "none",
                "var_code": date_var,
                "constant": None,
            },
            "gender_var": config.get("gender_var"),
            "age_var": config.get("age_var"),
            "nse_var": config.get("nse_var"),
            "state_var": config.get("state_var"),
        }
    config.setdefault("date", {"mode": "none", "var_code": None, "constant": None})
    config["date"].setdefault("mode", "none")
    config["date"].setdefault("var_code", None)
    config["date"].setdefault("constant", None)
    return config


def save_demographics_config(study_id: str, payload: dict) -> Path:
    path = _config_path(study_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)
    return path


def build_value_labels_frame(meta: Any, study_id: str) -> pd.DataFrame:
    value_labels = getattr(meta, "variable_value_labels", {}) or {}
    rows: list[dict[str, str]] = []
    for var_code, labels in value_labels.items():
        if not labels:
            continue
        for code, label in labels.items():
            rows.append(
                {
                    "study_id": study_id,
                    "var_code": str(var_code),
                    "value_code": str(code),
                    "value_label": str(label),
                }
            )
    return pd.DataFrame(rows)


def respondents_path(study_id: str) -> Path:
    return (
        get_repo_root()
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "respondents.parquet"
    )


def value_labels_path(study_id: str) -> Path:
    return (
        get_repo_root()
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_value_labels.parquet"
    )
