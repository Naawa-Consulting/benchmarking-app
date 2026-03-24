from __future__ import annotations

import json
from pathlib import Path

import duckdb
from fastapi import APIRouter, Query

from app.data.demographics import load_demographics_config, normalize_demographics_config, respondents_path, value_labels_path
from app.data.market_lens import market_taxonomy_items_from_standard, resolve_classification
from app.data.warehouse import get_repo_root

router = APIRouter()


def _discover_curated_studies(root: Path) -> list[str]:
    curated_root = root / "data" / "warehouse" / "curated"
    discovered = []
    if curated_root.exists():
        for path in curated_root.glob("study_id=*"):
            if (path / "fact_journey.parquet").exists():
                discovered.append(path.name.replace("study_id=", "", 1))
    return discovered


def _classification_for_study(root: Path, study_id: str) -> dict[str, str | None]:
    classification_path = (
        root
        / "data"
        / "warehouse"
        / "taxonomy"
        / "study_classification"
        / f"study_id={study_id}.json"
    )
    if not classification_path.exists():
        return {
            "sector": None,
            "subsector": None,
            "category": None,
            "market_sector": None,
            "market_subsector": None,
            "market_category": None,
            "market_source": None,
        }
    try:
        payload = json.loads(classification_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        payload = json.loads(classification_path.read_text(encoding="utf-8-sig"))
    return resolve_classification(payload, root=root)


@router.get("/filters/options/studies")
def filter_study_options() -> dict:
    root = get_repo_root()
    study_ids = _discover_curated_studies(root)
    items = []
    for study_id in study_ids:
        classification = _classification_for_study(root, study_id)
        config = normalize_demographics_config(load_demographics_config(study_id))
        respondents_exists = respondents_path(study_id).exists()
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
        items = market_taxonomy_items_from_standard(get_repo_root())
        sectors = sorted({item.get("sector") for item in items if item.get("sector")})
        subsectors = sorted({item.get("subsector") for item in items if item.get("subsector")})
        categories = sorted({item.get("category") for item in items if item.get("category")})
        return {"items": items, "sectors": sectors, "subsectors": subsectors, "categories": categories}

    root = get_repo_root()
    taxonomy_path = (
        root
        / "data"
        / "warehouse"
        / "taxonomy"
        / "sector_subsector_category_v1.json"
    )
    if not taxonomy_path.exists():
        return {"items": [], "sectors": [], "subsectors": [], "categories": []}
    try:
        payload = json.loads(taxonomy_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        payload = json.loads(taxonomy_path.read_text(encoding="utf-8-sig"))
    items = payload.get("items", [])
    sectors = sorted({item.get("sector") for item in items if item.get("sector")})
    subsectors = sorted({item.get("subsector") for item in items if item.get("subsector")})
    categories = sorted({item.get("category") for item in items if item.get("category")})
    return {"items": items, "sectors": sectors, "subsectors": subsectors, "categories": categories}


def _parse_study_ids(raw: str | None, root: Path) -> list[str]:
    discovered = _discover_curated_studies(root)
    if not raw:
        return discovered
    requested = [item.strip() for item in raw.split(",") if item.strip()]
    return [study_id for study_id in requested if study_id in discovered]


@router.get("/filters/options/demographics")
def filter_demographic_options(
    study_ids: str | None = Query(None, description="Comma-separated study ids"),
) -> dict:
    root = get_repo_root()
    selected = _parse_study_ids(study_ids, root)

    gender_values: set[str] = set()
    nse_values: set[str] = set()
    state_values: set[str] = set()
    age_min = None
    age_max = None

    for study_id in selected:
        resp_path = respondents_path(study_id)
        if not resp_path.exists():
            continue
        conn = duckdb.connect()
        try:
            config = normalize_demographics_config(load_demographics_config(study_id))
            conn.execute(f"CREATE OR REPLACE VIEW respondents AS SELECT * FROM read_parquet('{resp_path}')")

            if config.get("age_var"):
                row = conn.execute(
                    "SELECT MIN(age) AS min_age, MAX(age) AS max_age FROM respondents WHERE age IS NOT NULL"
                ).fetchone()
                if row:
                    if row[0] is not None:
                        age_min = row[0] if age_min is None else min(age_min, row[0])
                    if row[1] is not None:
                        age_max = row[1] if age_max is None else max(age_max, row[1])

            labels_path = value_labels_path(study_id)
            if not labels_path.exists():
                continue
            conn.execute(f"CREATE OR REPLACE VIEW labels AS SELECT * FROM read_parquet('{labels_path}')")

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
                gender_values.update({str(row[0]).strip() for row in rows if row[0] is not None and str(row[0]).strip()})

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

    return {
        "gender": sorted(gender_values),
        "nse": sorted(nse_values),
        "state": sorted(state_values),
        "age": {"min": age_min, "max": age_max},
    }


@router.get("/filters/options/date")
def filter_date_options(
    study_ids: str | None = Query(None, description="Comma-separated study ids"),
) -> dict:
    root = get_repo_root()
    selected = _parse_study_ids(study_ids, root)
    quarters: set[int] = set()

    for study_id in selected:
        resp_path = respondents_path(study_id)
        if not resp_path.exists():
            continue
        conn = duckdb.connect()
        try:
            conn.execute(f"CREATE OR REPLACE VIEW respondents AS SELECT * FROM read_parquet('{resp_path}')")
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
