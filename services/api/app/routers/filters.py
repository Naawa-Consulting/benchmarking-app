from __future__ import annotations

import duckdb
from fastapi import APIRouter, Query

from app.data.demographics import load_demographics_config, normalize_demographics_config, respondents_key, value_labels_key
from app.data.market_lens import market_taxonomy_items_from_standard, resolve_classification
from app.data.warehouse import blob_exists, list_study_ids, load_parquet_as_view, read_json_blob

router = APIRouter()

def _normalize_gender_label(value: object) -> str:
    if value is None:
        return "Unknown"
    text = str(value).strip().lower()
    if not text:
        return "Unknown"
    text = (
        text.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )

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
    if "female" in text or "femen" in text or "mujer" in text or text in {"f", "fem"}:
        return "Female"
    if "male" in text or "mascul" in text or "hombre" in text or "varon" in text or text in {"m", "masc"}:
        return "Male"
    return "Unknown"


def _discover_curated_studies() -> list[str]:
    return list_study_ids("warehouse/curated", "fact_journey.parquet")


def _classification_for_study(study_id: str) -> dict[str, str | None]:
    key = f"warehouse/taxonomy/study_classification/study_id={study_id}.json"
    payload = read_json_blob(key, default=None)
    if payload is None:
        return {
            "sector": None,
            "subsector": None,
            "category": None,
            "market_sector": None,
            "market_subsector": None,
            "market_category": None,
            "market_source": None,
        }
    return resolve_classification(payload)


@router.get("/filters/options/studies")
def filter_study_options() -> dict:
    study_ids = _discover_curated_studies()
    items = []
    for study_id in study_ids:
        classification = _classification_for_study(study_id)
        config = normalize_demographics_config(load_demographics_config(study_id))
        respondents_exists = blob_exists(respondents_key(study_id))
        date_mode = (config.get("date") or {}).get("mode", "none")
        items.append(
            {
                "study_id": study_id,
                "study_name": study_id,
                "sector": classification.get("sector"),
                "subsector": classification.get("subsector"),
                "category": classification.get("category"),
                "market_sector": classification.get("market_sector"),
                "market_subsector": classification.get("market_subsector"),
                "market_category": classification.get("market_category"),
                "market_source": classification.get("market_source"),
                "has_demographics": respondents_exists,
                "has_date": respondents_exists and date_mode != "none",
            }
        )
    return {"items": items}


@router.get("/filters/options/taxonomy")
def filter_taxonomy_options(view: str = Query("market", description="market|standard")) -> dict:
    normalized_view = view.lower().strip() if isinstance(view, str) else "market"
    if normalized_view == "market":
        items = market_taxonomy_items_from_standard()
        sectors = sorted({item.get("sector") for item in items if item.get("sector")})
        subsectors = sorted({item.get("subsector") for item in items if item.get("subsector")})
        categories = sorted({item.get("category") for item in items if item.get("category")})
        return {"items": items, "sectors": sectors, "subsectors": subsectors, "categories": categories}

    taxonomy_key = "warehouse/taxonomy/sector_subsector_category_v1.json"
    payload = read_json_blob(taxonomy_key, default=None)
    if payload is None:
        return {"items": [], "sectors": [], "subsectors": [], "categories": []}
    items = payload.get("items", [])
    sectors = sorted({item.get("sector") for item in items if item.get("sector")})
    subsectors = sorted({item.get("subsector") for item in items if item.get("subsector")})
    categories = sorted({item.get("category") for item in items if item.get("category")})
    return {"items": items, "sectors": sectors, "subsectors": subsectors, "categories": categories}


def _parse_study_ids(raw: str | None) -> list[str]:
    discovered = _discover_curated_studies()
    if not raw:
        return discovered
    requested = [item.strip() for item in raw.split(",") if item.strip()]
    return [study_id for study_id in requested if study_id in discovered]


@router.get("/filters/options/demographics")
def filter_demographic_options(
    study_ids: str | None = Query(None, description="Comma-separated study ids"),
) -> dict:
    selected = _parse_study_ids(study_ids)

    gender_values: set[str] = set()
    nse_values: set[str] = set()
    state_values: set[str] = set()
    age_min = None
    age_max = None

    for study_id in selected:
        resp_key = respondents_key(study_id)
        if not blob_exists(resp_key):
            continue
        conn = duckdb.connect()
        try:
            config = normalize_demographics_config(load_demographics_config(study_id))
            load_parquet_as_view(conn, "respondents", resp_key)

            if config.get("age_var"):
                row = conn.execute(
                    "SELECT MIN(age) AS min_age, MAX(age) AS max_age FROM respondents WHERE age IS NOT NULL"
                ).fetchone()
                if row:
                    if row[0] is not None:
                        age_min = row[0] if age_min is None else min(age_min, row[0])
                    if row[1] is not None:
                        age_max = row[1] if age_max is None else max(age_max, row[1])

            labels_key = value_labels_key(study_id)
            if not blob_exists(labels_key):
                continue
            load_parquet_as_view(conn, "labels", labels_key)

            respondent_columns = {
                column[0]
                for column in conn.execute("SELECT * FROM respondents LIMIT 0").description
            }

            if "gender" in respondent_columns:
                rows = conn.execute(
                    """
                    SELECT DISTINCT gender
                    FROM respondents
                    WHERE gender IS NOT NULL
                    """
                ).fetchall()
                gender_values.update(
                    {
                        _normalize_gender_label(row[0])
                        for row in rows
                        if row[0] is not None and str(row[0]).strip()
                    }
                )
            else:
                gender_var = config.get("gender_var")
                if gender_var:
                    rows = conn.execute(
                        """
                        SELECT DISTINCT l.value_label
                        FROM labels l
                        WHERE l.var_code = ?
                          AND l.value_code IN (
                            SELECT DISTINCT CAST(gender_code AS VARCHAR)
                            FROM respondents
                            WHERE gender_code IS NOT NULL
                          )
                        """,
                        [gender_var],
                    ).fetchall()
                    gender_values.update(
                        {
                            _normalize_gender_label(row[0])
                            for row in rows
                            if row[0] is not None and str(row[0]).strip()
                        }
                    )

            nse_var = config.get("nse_var")
            if nse_var:
                rows = conn.execute(
                    """
                    SELECT DISTINCT l.value_label
                    FROM labels l
                    WHERE l.var_code = ?
                      AND l.value_code IN (
                        SELECT DISTINCT CAST(nse_code AS VARCHAR)
                        FROM respondents
                        WHERE nse_code IS NOT NULL
                      )
                    """,
                    [nse_var],
                ).fetchall()
                nse_values.update({str(row[0]).strip() for row in rows if row[0] is not None and str(row[0]).strip()})

            state_var = config.get("state_var")
            if state_var:
                rows = conn.execute(
                    """
                    SELECT DISTINCT l.value_label
                    FROM labels l
                    WHERE l.var_code = ?
                      AND l.value_code IN (
                        SELECT DISTINCT CAST(state_code AS VARCHAR)
                        FROM respondents
                        WHERE state_code IS NOT NULL
                      )
                    """,
                    [state_var],
                ).fetchall()
                state_values.update({str(row[0]).strip() for row in rows if row[0] is not None and str(row[0]).strip()})
        except Exception:
            continue
        finally:
            conn.close()

    ordered_gender = ["Male", "Female", "Non-binary", "Prefer not to say", "Unknown"]
    normalized_gender = [value for value in ordered_gender if value in gender_values]
    return {
        "gender": normalized_gender,
        "nse": sorted(nse_values),
        "state": sorted(state_values),
        "age": {"min": age_min, "max": age_max},
    }


@router.get("/filters/options/date")
def filter_date_options(
    study_ids: str | None = Query(None, description="Comma-separated study ids"),
) -> dict:
    selected = _parse_study_ids(study_ids)
    quarters: set[int] = set()

    for study_id in selected:
        resp_key = respondents_key(study_id)
        if not blob_exists(resp_key):
            continue
        conn = duckdb.connect()
        try:
            load_parquet_as_view(conn, "respondents", resp_key)
            rows = conn.execute(
                """
                SELECT DISTINCT
                    EXTRACT(year FROM TRY_CAST(date AS DATE)) * 10
                        + EXTRACT(quarter FROM TRY_CAST(date AS DATE)) AS q_key
                FROM respondents
                WHERE TRY_CAST(date AS DATE) IS NOT NULL
                """
            ).fetchall()
            for row in rows:
                if row and row[0] is not None:
                    quarters.add(int(row[0]))
        except Exception:
            continue
        finally:
            conn.close()

    sorted_keys = sorted(quarters)
    quarter_labels = [f"{key // 10}-Q{key % 10}" for key in sorted_keys]
    return {
        "quarters": quarter_labels,
        "min": quarter_labels[0] if quarter_labels else None,
        "max": quarter_labels[-1] if quarter_labels else None,
    }
