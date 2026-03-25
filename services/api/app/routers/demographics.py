from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd
import pyreadstat
from fastapi import APIRouter, HTTPException, Query, Request

from app.data.demographics import (
    load_demographics_config,
    normalize_demographics_config,
    respondents_path,
    save_demographics_config,
    value_labels_path,
)
from app.data.warehouse import get_repo_root

router = APIRouter()

GENDER_STANDARD_VALUES = (
    "Male",
    "Female",
    "Non-binary",
    "Prefer not to say",
    "Unknown",
)


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"\s+", " ", text)
    return text


def _standardize_gender(raw_label: object) -> str:
    text = _normalize_text(raw_label)
    if not text:
        return "Unknown"

    unknown_markers = {
        "unknown",
        "unk",
        "desconocido",
        "no sabe",
        "ns/nc",
        "ns",
        "nc",
        "n/a",
        "na",
        "sin dato",
        "sin informacion",
        "otro",
        "otros",
    }
    if text in unknown_markers:
        return "Unknown"

    if (
        "prefiere no" in text
        or "prefer not" in text
        or "declina" in text
        or "no responde" in text
        or "refus" in text
    ):
        return "Prefer not to say"

    if (
        "non-binary" in text
        or "non binary" in text
        or "no binario" in text
        or "no binaria" in text
        or "no binarie" in text
        or text in {"nb", "genderqueer"}
    ):
        return "Non-binary"

    if (
        "female" in text
        or "femen" in text
        or "mujer" in text
        or text in {"f", "fem"}
    ):
        return "Female"

    if (
        "male" in text
        or "mascul" in text
        or "hombre" in text
        or "varon" in text
        or text in {"m", "masc"}
    ):
        return "Male"

    return "Unknown"


@router.get("/demographics/schema")
def demographics_schema(study_id: str = Query(..., description="Study id")) -> dict:
    return {
        "study_id": study_id,
        "fields": [
            {"key": "date", "label": "Date"},
            {"key": "gender", "label": "Gender"},
            {"key": "age", "label": "Age (numeric)"},
            {"key": "nse", "label": "NSE"},
            {"key": "state", "label": "State"},
        ],
    }


@router.get("/demographics/config")
def get_demographics_config(study_id: str = Query(..., description="Study id")) -> dict:
    config = load_demographics_config(study_id)
    return normalize_demographics_config(config)


def _variables_for_study(study_id: str) -> list[str]:
    variables_path = (
        get_repo_root()
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_variables.parquet"
    )
    if not variables_path.exists():
        raise HTTPException(status_code=404, detail="raw_variables.parquet not found for study.")
    df = pd.read_parquet(variables_path, columns=["var_code"])
    return df["var_code"].astype(str).tolist()


def _validate_age_var(study_id: str, var_code: str) -> None:
    responses_path = (
        get_repo_root()
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_responses.parquet"
    )
    if not responses_path.exists():
        raise HTTPException(status_code=404, detail="raw_responses.parquet not found for study.")
    conn = duckdb.connect()
    conn.execute(
        f"CREATE OR REPLACE VIEW responses AS SELECT * FROM read_parquet('{responses_path}')"
    )
    row = conn.execute(
        """
        SELECT
            SUM(CASE WHEN value IS NULL THEN 0 ELSE 1 END) AS non_null,
            SUM(CASE WHEN TRY_CAST(value AS DOUBLE) IS NULL THEN 0 ELSE 1 END) AS numeric_like
        FROM responses
        WHERE var_code = ?
        """,
        [var_code],
    ).fetchone()
    non_null = int(row[0]) if row and row[0] is not None else 0
    numeric_like = int(row[1]) if row and row[1] is not None else 0
    if non_null == 0:
        raise HTTPException(status_code=400, detail="Selected age variable has no values.")
    if numeric_like / non_null < 0.8:
        raise HTTPException(status_code=400, detail="Selected age variable is not numeric enough.")


def _build_respondents_parquet(study_id: str, config: dict) -> None:
    responses_path = (
        get_repo_root()
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_responses.parquet"
    )
    if not responses_path.exists():
        raise HTTPException(status_code=404, detail="raw_responses.parquet not found for study.")

    conn = duckdb.connect()
    conn.execute(
        f"CREATE OR REPLACE VIEW responses AS SELECT * FROM read_parquet('{responses_path}')"
    )

    date_config = config.get("date", {}) if config else {}
    date_mode = date_config.get("mode", "none")
    date_var = date_config.get("var_code")
    date_constant = date_config.get("constant")
    gender_var = config.get("gender_var")
    age_var = config.get("age_var")
    nse_var = config.get("nse_var")
    state_var = config.get("state_var")

    query = """
        SELECT
            respondent_id,
            MAX(weight) AS weight,
            MAX(CASE WHEN var_code = ? THEN value END) AS date,
            MAX(CASE WHEN var_code = ? THEN value END) AS gender_code,
            MAX(CASE WHEN var_code = ? THEN TRY_CAST(value AS DOUBLE) END) AS age,
            MAX(CASE WHEN var_code = ? THEN value END) AS nse_code,
            MAX(CASE WHEN var_code = ? THEN value END) AS state_code
        FROM responses
        GROUP BY respondent_id
    """
    df = conn.execute(
        query,
        [
            date_var,
            gender_var,
            age_var,
            nse_var,
            state_var,
        ],
    ).df()
    gender_labels = {}
    labels_file = value_labels_path(study_id)
    if labels_file.exists() and gender_var:
        try:
            labels_df = pd.read_parquet(labels_file, columns=["var_code", "value_code", "value_label"])
            labels_df = labels_df[labels_df["var_code"].astype(str) == str(gender_var)]
            gender_labels = {
                str(row["value_code"]): str(row["value_label"])
                for row in labels_df.to_dict(orient="records")
                if row.get("value_code") is not None and row.get("value_label") is not None
            }
        except Exception:
            gender_labels = {}

    def _gender_from_code(code: object) -> str:
        if code is None:
            return "Unknown"
        code_key = str(code)
        raw_label = gender_labels.get(code_key, code_key)
        return _standardize_gender(raw_label)

    df["gender"] = df["gender_code"].map(_gender_from_code)
    if not df["gender"].isin(GENDER_STANDARD_VALUES).all():
        df["gender"] = "Unknown"

    if date_mode == "constant" and date_constant:
        df["date"] = date_constant
    elif date_mode == "none":
        df["date"] = None
    else:
        try:
            import dateutil.parser
        except ImportError:
            dateutil = None
        if dateutil:
            parsed = []
            for value in df["date"].tolist():
                if value is None:
                    parsed.append(None)
                    continue
                try:
                    parsed.append(dateutil.parser.parse(str(value)).date().isoformat())
                except (ValueError, TypeError):
                    parsed.append(None)
            df["date"] = parsed
        else:
            df["date"] = None
    path = respondents_path(study_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


@router.post("/demographics/config")
async def save_demographics(
    study_id: str = Query(..., description="Study id"),
    request: Request = ...,
) -> dict:
    payload = await request.json()
    if "date" in payload:
        date_payload = payload.get("date") or {}
        config = {
            "study_id": study_id,
            "date": {
                "mode": date_payload.get("mode", "none"),
                "var_code": date_payload.get("var_code"),
                "constant": date_payload.get("constant"),
            },
            "gender_var": payload.get("gender_var"),
            "age_var": payload.get("age_var"),
            "nse_var": payload.get("nse_var"),
            "state_var": payload.get("state_var"),
        }
    else:
        config = {
            "study_id": study_id,
            "date": {
                "mode": "var" if payload.get("date_var") else "none",
                "var_code": payload.get("date_var"),
                "constant": None,
            },
            "gender_var": payload.get("gender_var"),
            "age_var": payload.get("age_var"),
            "nse_var": payload.get("nse_var"),
            "state_var": payload.get("state_var"),
        }

    config = normalize_demographics_config(config)

    variables = set(_variables_for_study(study_id))
    for key in ("gender_var", "age_var", "nse_var", "state_var"):
        value = config.get(key)
        if value is not None and value not in variables:
            raise HTTPException(status_code=400, detail=f"{key} not found in variables.")

    date_mode = config["date"].get("mode")
    date_var = config["date"].get("var_code")
    date_constant = config["date"].get("constant")
    if date_mode == "var":
        if date_var is None or date_var not in variables:
            raise HTTPException(status_code=400, detail="date.var_code not found in variables.")
    if date_mode == "constant":
        if not date_constant or not isinstance(date_constant, str):
            raise HTTPException(status_code=400, detail="date.constant must be set.")
        if not _is_iso_date(date_constant):
            raise HTTPException(status_code=400, detail="date.constant must be YYYY-MM-DD.")

    if config.get("age_var"):
        _validate_age_var(study_id, config["age_var"])

    save_demographics_config(study_id, config)
    _build_respondents_parquet(study_id, config)
    return config


@router.get("/demographics/value-labels")
def demographics_value_labels(
    study_id: str = Query(..., description="Study id"),
    var_code: str = Query(..., description="Variable code"),
) -> dict:
    labels_path = value_labels_path(study_id)
    if not labels_path.exists():
        return {"study_id": study_id, "var_code": var_code, "items": []}
    df = pd.read_parquet(labels_path)
    df = df[df["var_code"].astype(str) == str(var_code)]
    items = [
        {"value_code": str(row["value_code"]), "value_label": str(row["value_label"])}
        for row in df.to_dict(orient="records")
    ]
    return {"study_id": study_id, "var_code": var_code, "items": items}


@router.get("/demographics/gender-map-preview")
def demographics_gender_map_preview(
    study_id: str = Query(..., description="Study id"),
    var_code: str = Query(..., description="Gender variable code"),
    limit: int = Query(50, ge=1, le=500),
) -> dict:
    responses_path = (
        get_repo_root()
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_responses.parquet"
    )
    if not responses_path.exists():
        raise HTTPException(status_code=404, detail="raw_responses.parquet not found for study.")

    labels_path = value_labels_path(study_id)
    labels: dict[str, str] = {}
    if labels_path.exists():
        try:
            labels_df = pd.read_parquet(labels_path, columns=["var_code", "value_code", "value_label"])
            labels_df = labels_df[labels_df["var_code"].astype(str) == str(var_code)]
            labels = {
                str(row["value_code"]): str(row["value_label"])
                for row in labels_df.to_dict(orient="records")
                if row.get("value_code") is not None and row.get("value_label") is not None
            }
        except Exception:
            labels = {}

    conn = duckdb.connect()
    try:
        conn.execute(
            f"CREATE OR REPLACE VIEW responses AS SELECT * FROM read_parquet('{responses_path}')"
        )
        rows = conn.execute(
            """
            SELECT value, COUNT(*) AS cnt
            FROM responses
            WHERE var_code = ?
              AND value IS NOT NULL
              AND TRIM(CAST(value AS VARCHAR)) <> ''
            GROUP BY value
            ORDER BY cnt DESC
            LIMIT ?
            """,
            [var_code, limit],
        ).fetchall()
    finally:
        conn.close()

    items = []
    for value, count in rows:
        raw_value = str(value)
        raw_label = labels.get(raw_value, raw_value)
        items.append(
            {
                "raw_value": raw_value,
                "raw_label": raw_label,
                "standard_value": _standardize_gender(raw_label),
                "count": int(count or 0),
            }
        )
    return {"study_id": study_id, "var_code": var_code, "items": items}


@router.get("/demographics/preview")
def demographics_preview(
    study_id: str = Query(..., description="Study id"),
    var_code: str = Query(..., description="Variable code"),
    n: int = Query(5, ge=1, le=50, description="Rows to preview"),
) -> dict:
    responses_path = (
        get_repo_root()
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_responses.parquet"
    )
    if not responses_path.exists():
        raise HTTPException(status_code=404, detail="raw_responses.parquet not found for study.")

    conn = duckdb.connect()
    conn.execute(
        f"CREATE OR REPLACE VIEW responses AS SELECT * FROM read_parquet('{responses_path}')"
    )
    rows = conn.execute(
        """
        SELECT value
        FROM responses
        WHERE var_code = ?
        LIMIT ?
        """,
        [var_code, n],
    ).fetchall()
    stats = conn.execute(
        """
        SELECT
            MIN(TRY_CAST(value AS DOUBLE)) AS min_val,
            MAX(TRY_CAST(value AS DOUBLE)) AS max_val
        FROM responses
        WHERE var_code = ?
        """,
        [var_code],
    ).fetchone()
    return {
        "study_id": study_id,
        "var_code": var_code,
        "rows": [str(row[0]) for row in rows],
        "min": stats[0] if stats else None,
        "max": stats[1] if stats else None,
    }


@router.get("/demographics/date/preview")
def demographics_date_preview(
    study_id: str = Query(..., description="Study id"),
    mode: str = Query("none", description="none|var|constant"),
    var_code: str | None = Query(None, description="Variable code"),
    constant: str | None = Query(None, description="Constant date"),
    n: int = Query(10, ge=1, le=50, description="Rows to preview"),
) -> dict:
    if mode == "constant":
        return {
            "raw_samples": [constant] * n,
            "parsed_samples": [constant] * n,
            "parse_success_rate": 1.0,
        }
    if mode != "var" or not var_code:
        return {"raw_samples": [], "parsed_samples": [], "parse_success_rate": 0.0}

    responses_path = (
        get_repo_root()
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_responses.parquet"
    )
    if not responses_path.exists():
        raise HTTPException(status_code=404, detail="raw_responses.parquet not found for study.")

    conn = duckdb.connect()
    conn.execute(
        f"CREATE OR REPLACE VIEW responses AS SELECT * FROM read_parquet('{responses_path}')"
    )
    rows = conn.execute(
        """
        SELECT value
        FROM responses
        WHERE var_code = ?
        LIMIT ?
        """,
        [var_code, n],
    ).fetchall()
    raw_samples = [str(row[0]) for row in rows]
    parsed_samples = []
    success = 0
    try:
        import dateutil.parser
    except ImportError:
        dateutil = None
    for value in raw_samples:
        if not value:
            parsed_samples.append(None)
            continue
        if dateutil:
            try:
                parsed_samples.append(dateutil.parser.parse(value).date().isoformat())
                success += 1
            except (ValueError, TypeError):
                parsed_samples.append(None)
        else:
            parsed_samples.append(None)
    rate = success / len(raw_samples) if raw_samples else 0.0
    return {"raw_samples": raw_samples, "parsed_samples": parsed_samples, "parse_success_rate": rate}


def _is_iso_date(value: str) -> bool:
    return bool(value) and len(value) == 10 and value[4] == "-" and value[7] == "-"
