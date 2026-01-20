from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable

from app.data.warehouse import get_repo_root

logger = logging.getLogger(__name__)


RESPONDENT_ID_CANDIDATES = [
    "respondent_id",
    "id",
    "folio",
    "uuid",
    "guid",
    "panelist_id",
    "caseid",
    "owid",
    "record",
    "respondent",
]

WEIGHT_CANDIDATES = [
    "weight",
    "w",
    "factor",
    "ponderador",
    "ponderacion",
    "expansion",
    "peso",
]


def _study_config_path(study_id: str) -> Path:
    return (
        get_repo_root()
        / "data"
        / "warehouse"
        / "study_config"
        / f"study_id={study_id}.json"
    )


def _normalize_names(names: Iterable[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for name in names:
        if name is None:
            continue
        mapping[str(name).strip().lower()] = str(name)
    return mapping


def _pick_best_match(candidates: list[str], normalized: dict[str, str]) -> str | None:
    exact = [normalized[key] for key in normalized.keys() if key in candidates]
    if exact:
        return sorted(exact, key=len)[0]

    contains = []
    for key, original in normalized.items():
        for candidate in candidates:
            if candidate in key:
                contains.append(original)
                break
    if contains:
        return sorted(contains, key=len)[0]
    return None


def detect_base_columns(var_codes: Iterable[str]) -> tuple[str | None, str | None]:
    normalized = _normalize_names(var_codes)
    respondent_id_var = _pick_best_match(RESPONDENT_ID_CANDIDATES, normalized)
    weight_var = _pick_best_match(WEIGHT_CANDIDATES, normalized)
    return respondent_id_var, weight_var


def load_study_config(study_id: str) -> dict:
    path = _study_config_path(study_id)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def save_study_config(study_id: str, payload: dict) -> Path:
    path = _study_config_path(study_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)
    return path


def load_or_create_study_config(study_id: str, var_codes: Iterable[str]) -> dict:
    existing = load_study_config(study_id)
    if existing:
        return existing

    respondent_id_var, weight_var = detect_base_columns(var_codes)
    payload = {
        "study_id": study_id,
        "respondent_id": {
            "source": "auto" if respondent_id_var else "auto",
            "var_code": respondent_id_var,
        },
        "weight": {
            "source": "auto" if weight_var else "default",
            "var_code": weight_var,
            "default": 1.0,
        },
    }
    save_study_config(study_id, payload)
    return payload
