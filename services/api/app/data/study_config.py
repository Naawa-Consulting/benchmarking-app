from __future__ import annotations

from typing import Iterable

from app.data.warehouse import read_json_blob, write_json_blob

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


def _study_config_key(study_id: str) -> str:
    return f"warehouse/study_config/study_id={study_id}.json"


def _methodology_overrides_key(study_id: str) -> str:
    # Kept in its own blob, separate from _study_config_key: the study-config POST
    # endpoint (save_study_config_endpoint) always overwrites its file wholesale with
    # only respondent_id/weight, so anything else stored there would get silently
    # wiped by the next base-column save.
    return f"warehouse/study_config/study_id={study_id}.methodology.json"


def load_methodology_overrides(study_id: str) -> dict:
    return read_json_blob(_methodology_overrides_key(study_id), default={}) or {}


def save_methodology_overrides(study_id: str, payload: dict) -> str:
    key = _methodology_overrides_key(study_id)
    write_json_blob(key, payload)
    return key


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
    return read_json_blob(_study_config_key(study_id), default={}) or {}


def save_study_config(study_id: str, payload: dict) -> str:
    key = _study_config_key(study_id)
    write_json_blob(key, payload)
    return key


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
