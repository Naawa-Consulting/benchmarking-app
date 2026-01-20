import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request

from app.data.demographics import load_demographics_config, normalize_demographics_config
from app.data.warehouse import get_duckdb_connection, get_repo_root, load_parquet_as_view
from app.models.schemas import JourneyPoint, JourneyResponse

router = APIRouter()

TABLE_STAGE_MAP = {
    "awareness": "brand_awareness",
    "ad_awareness": "ad_awareness",
    "consideration": "brand_consideration",
    "purchase": "brand_purchase",
    "satisfaction": "brand_satisfaction",
    "recommendation": "brand_recommendation",
}

SORTABLE_METRICS = {
    "brand_awareness",
    "ad_awareness",
    "brand_consideration",
    "brand_purchase",
    "brand_satisfaction",
    "brand_recommendation",
}

TOUCHPOINT_STAGE_KEY = "touchpoints"


def _discover_curated_studies(root: Path) -> list[str]:
    curated_root = root / "data" / "warehouse" / "curated"
    discovered = []
    if curated_root.exists():
        for path in curated_root.glob("study_id=*"):
            if (path / "fact_journey.parquet").exists():
                discovered.append(path.name.replace("study_id=", "", 1))
    return discovered


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")


def _quarter_key(value: str | None) -> int | None:
    if not value or "-Q" not in value:
        return None
    try:
        year_str, quarter_str = value.split("-Q", 1)
        year = int(year_str)
        quarter = int(quarter_str)
    except ValueError:
        return None
    if quarter < 1 or quarter > 4:
        return None
    return year * 10 + quarter


def _parse_filters(payload: dict | None) -> dict:
    if not payload:
        payload = {}
    study_ids = payload.get("study_ids") or payload.get("study_id") or []
    if isinstance(study_ids, str):
        study_ids = [item.strip() for item in study_ids.split(",") if item.strip()]
    return {
        "study_ids": study_ids,
        "sector": payload.get("sector"),
        "subsector": payload.get("subsector"),
        "category": payload.get("category"),
        "gender": payload.get("gender"),
        "nse": payload.get("nse"),
        "state": payload.get("state"),
        "age_min": payload.get("age_min"),
        "age_max": payload.get("age_max"),
        "date_grain": payload.get("date_grain") or "Q",
        "date_from": payload.get("date_from"),
        "date_to": payload.get("date_to"),
        "quarter_from": payload.get("quarter_from"),
        "quarter_to": payload.get("quarter_to"),
    }


def _study_matches_taxonomy(filters: dict, classification: dict[str, str | None]) -> bool:
    for key in ("sector", "subsector", "category"):
        value = filters.get(key)
        if not value:
            continue
        if not classification.get(key):
            return False
        if classification.get(key) != value:
            return False
    return True


def _needs_respondent_filter(filters: dict) -> bool:
    return any(
        [
            filters.get("gender"),
            filters.get("nse"),
            filters.get("state"),
            filters.get("age_min") is not None,
            filters.get("age_max") is not None,
            filters.get("quarter_from"),
            filters.get("quarter_to"),
            filters.get("date_from"),
            filters.get("date_to"),
        ]
    )


def _respondent_filter_cte(study_id: str, filters: dict) -> tuple[str | None, list, bool]:
    root = get_repo_root()
    respondents_path = (
        root
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "respondents.parquet"
    )
    if not _needs_respondent_filter(filters):
        return None, [], True
    if not respondents_path.exists():
        return None, [], False

    config = normalize_demographics_config(load_demographics_config(study_id))
    gender_var = config.get("gender_var")
    nse_var = config.get("nse_var")
    state_var = config.get("state_var")
    age_var = config.get("age_var")
    date_mode = (config.get("date") or {}).get("mode", "none")

    if filters.get("gender") and not gender_var:
        return None, [], False
    if filters.get("nse") and not nse_var:
        return None, [], False
    if filters.get("state") and not state_var:
        return None, [], False
    if (filters.get("age_min") is not None or filters.get("age_max") is not None) and not age_var:
        return None, [], False
    if (filters.get("quarter_from") or filters.get("quarter_to") or filters.get("date_from") or filters.get("date_to")) and date_mode == "none":
        return None, [], False

    labels_path = (
        root
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_value_labels.parquet"
    )
    labels_source = (
        f"SELECT * FROM read_parquet('{labels_path}')"
        if labels_path.exists()
        else "SELECT NULL::VARCHAR AS var_code, NULL::VARCHAR AS value_code, NULL::VARCHAR AS value_label WHERE FALSE"
    )

    gender_var_sql = _sql_literal(str(gender_var)) if gender_var else ""
    nse_var_sql = _sql_literal(str(nse_var)) if nse_var else ""
    state_var_sql = _sql_literal(str(state_var)) if state_var else ""

    conditions = []
    params: list = []
    if filters.get("gender"):
        conditions.append("gender_label = ?")
        params.append(filters["gender"])
    if filters.get("nse"):
        conditions.append("nse_label = ?")
        params.append(filters["nse"])
    if filters.get("state"):
        conditions.append("state_label = ?")
        params.append(filters["state"])
    if filters.get("age_min") is not None:
        conditions.append("age >= ?")
        params.append(filters["age_min"])
    if filters.get("age_max") is not None:
        conditions.append("age <= ?")
        params.append(filters["age_max"])

    quarter_from = _quarter_key(filters.get("quarter_from"))
    quarter_to = _quarter_key(filters.get("quarter_to"))
    if quarter_from is not None and quarter_to is not None:
        conditions.append("q_key BETWEEN ? AND ?")
        params.extend([quarter_from, quarter_to])
    elif quarter_from is not None:
        conditions.append("q_key >= ?")
        params.append(quarter_from)
    elif quarter_to is not None:
        conditions.append("q_key <= ?")
        params.append(quarter_to)

    if filters.get("date_from"):
        conditions.append("date_dt >= TRY_CAST(? AS DATE)")
        params.append(filters["date_from"])
    if filters.get("date_to"):
        conditions.append("date_dt <= TRY_CAST(? AS DATE)")
        params.append(filters["date_to"])

    where_clause = " AND ".join(["1=1"] + conditions)

    cte = f"""
        respondents AS (
            SELECT respondent_id, gender_code, nse_code, state_code, age, date
            FROM read_parquet('{respondents_path}')
        ),
        labels AS (
            {labels_source}
        ),
        respondents_labeled AS (
            SELECT
                r.respondent_id,
                r.age,
                r.date,
                TRY_CAST(r.date AS DATE) AS date_dt,
                EXTRACT(year FROM TRY_CAST(r.date AS DATE)) * 10
                    + EXTRACT(quarter FROM TRY_CAST(r.date AS DATE)) AS q_key,
                g.value_label AS gender_label,
                n.value_label AS nse_label,
                s.value_label AS state_label
            FROM respondents r
            LEFT JOIN labels g
                ON g.var_code = '{gender_var_sql}'
               AND g.value_code = CAST(r.gender_code AS VARCHAR)
            LEFT JOIN labels n
                ON n.var_code = '{nse_var_sql}'
               AND n.value_code = CAST(r.nse_code AS VARCHAR)
            LEFT JOIN labels s
                ON s.var_code = '{state_var_sql}'
               AND s.value_code = CAST(r.state_code AS VARCHAR)
        ),
        filtered_respondents AS (
            SELECT respondent_id
            FROM respondents_labeled
            WHERE {where_clause}
        )
    """
    return cte, params, True

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
        return {"sector": None, "subsector": None, "category": None}
    try:
        return json.loads(classification_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return json.loads(classification_path.read_text(encoding="utf-8-sig"))


def _compute_table_rows(study_id: str) -> list[dict]:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if not curated_path.exists():
        raise HTTPException(status_code=404, detail="Curated mart not found for study.")

    conn = get_duckdb_connection()
    load_parquet_as_view(conn, "journey_table", str(curated_path))
    has_value_raw = (
        conn.execute(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'journey_table' AND column_name = 'value_raw'
            """
        ).fetchone()[0]
        > 0
    )
    value_expr = (
        "COALESCE(TRY_CAST(value_raw AS INTEGER), TRY_CAST(value AS INTEGER))"
        if has_value_raw
        else "TRY_CAST(value AS INTEGER)"
    )
    query = f"""
        WITH base AS (
            SELECT
                LOWER(stage) AS stage,
                brand,
                respondent_id,
                {value_expr} AS v_int
            FROM journey_table
            WHERE study_id = ?
              AND brand IS NOT NULL
              AND TRIM(CAST(brand AS VARCHAR)) <> ''
        ),
        stage_stats AS (
            SELECT stage, MAX(v_int) AS max_v
            FROM base
            WHERE v_int IS NOT NULL
            GROUP BY stage
        ),
        denoms AS (
            SELECT stage, brand, COUNT(DISTINCT respondent_id) AS denom
            FROM base
            WHERE v_int IS NOT NULL
            GROUP BY stage, brand
        ),
        nums AS (
            SELECT b.stage, b.brand, COUNT(DISTINCT b.respondent_id) AS num
            FROM base b
            LEFT JOIN stage_stats s ON s.stage = b.stage
            WHERE b.v_int IS NOT NULL
              AND (
                (b.stage IN ('awareness', 'ad_awareness', 'consideration', 'purchase') AND b.v_int = 1)
                OR (
                    b.stage = 'satisfaction'
                    AND (
                        (s.max_v IS NOT NULL AND s.max_v >= 5 AND b.v_int IN (4, 5))
                        OR (s.max_v IS NOT NULL AND s.max_v < 5 AND b.v_int = 1)
                    )
                )
                OR (
                    b.stage = 'recommendation'
                    AND (
                        (s.max_v IS NOT NULL AND s.max_v >= 9 AND b.v_int IN (9, 10))
                        OR (s.max_v IS NOT NULL AND s.max_v < 9 AND b.v_int = 1)
                    )
                )
              )
            GROUP BY b.stage, b.brand
        )
        SELECT
            n.stage,
            n.brand,
            n.num,
            d.denom
        FROM nums n
        LEFT JOIN denoms d ON d.stage = n.stage AND d.brand = n.brand
    """
    rows = conn.execute(query, [study_id]).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No curated data for {study_id}.")

    table: dict[str, dict[str, float | None]] = {}
    for stage, brand, num, denom in rows:
        if stage not in TABLE_STAGE_MAP:
            continue
        metric_key = TABLE_STAGE_MAP[stage]
        if brand not in table:
            table[brand] = {value: None for value in TABLE_STAGE_MAP.values()}
        pct = None
        if denom and denom > 0:
            pct = round((num / denom) * 100, 1)
        table[brand][metric_key] = pct

    result_rows = []
    for brand, values in table.items():
        result_rows.append(
            {
                "brand": brand,
                "brand_awareness": values.get("brand_awareness"),
                "ad_awareness": values.get("ad_awareness"),
                "brand_consideration": values.get("brand_consideration"),
                "brand_purchase": values.get("brand_purchase"),
                "brand_satisfaction": values.get("brand_satisfaction"),
                "brand_recommendation": values.get("brand_recommendation"),
            }
        )
    return result_rows


def _compute_table_rows_filtered(study_id: str, filters: dict) -> list[dict]:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if not curated_path.exists():
        return []

    respondent_cte, respondent_params, eligible = _respondent_filter_cte(study_id, filters)
    if not eligible:
        return []

    conn = get_duckdb_connection()
    load_parquet_as_view(conn, "journey_table", str(curated_path))
    has_value_raw = (
        conn.execute(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'journey_table' AND column_name = 'value_raw'
            """
        ).fetchone()[0]
        > 0
    )
    value_expr = (
        "COALESCE(TRY_CAST(value_raw AS INTEGER), TRY_CAST(value AS INTEGER))"
        if has_value_raw
        else "TRY_CAST(value AS INTEGER)"
    )
    respondent_filter = (
        "AND respondent_id IN (SELECT respondent_id FROM filtered_respondents)"
        if respondent_cte
        else ""
    )
    cte_prefix = f"{respondent_cte}," if respondent_cte else ""
    query = f"""
        WITH {cte_prefix}
        base AS (
            SELECT
                LOWER(stage) AS stage,
                brand,
                respondent_id,
                {value_expr} AS v_int
            FROM journey_table
            WHERE study_id = ?
              AND brand IS NOT NULL
              AND TRIM(CAST(brand AS VARCHAR)) <> ''
              {respondent_filter}
        ),
        stage_stats AS (
            SELECT stage, MAX(v_int) AS max_v
            FROM base
            WHERE v_int IS NOT NULL
            GROUP BY stage
        ),
        denoms AS (
            SELECT stage, brand, COUNT(DISTINCT respondent_id) AS denom
            FROM base
            WHERE v_int IS NOT NULL
            GROUP BY stage, brand
        ),
        nums AS (
            SELECT b.stage, b.brand, COUNT(DISTINCT b.respondent_id) AS num
            FROM base b
            LEFT JOIN stage_stats s ON s.stage = b.stage
            WHERE b.v_int IS NOT NULL
              AND (
                (b.stage IN ('awareness', 'ad_awareness', 'consideration', 'purchase') AND b.v_int = 1)
                OR (
                    b.stage = 'satisfaction'
                    AND (
                        (s.max_v IS NOT NULL AND s.max_v >= 5 AND b.v_int IN (4, 5))
                        OR (s.max_v IS NOT NULL AND s.max_v < 5 AND b.v_int = 1)
                    )
                )
                OR (
                    b.stage = 'recommendation'
                    AND (
                        (s.max_v IS NOT NULL AND s.max_v >= 9 AND b.v_int IN (9, 10))
                        OR (s.max_v IS NOT NULL AND s.max_v < 9 AND b.v_int = 1)
                    )
                )
              )
            GROUP BY b.stage, b.brand
        )
        SELECT
            n.stage,
            n.brand,
            n.num,
            d.denom
        FROM nums n
        LEFT JOIN denoms d ON d.stage = n.stage AND d.brand = n.brand
    """
    params = [*respondent_params, study_id]
    rows = conn.execute(query, params).fetchall()
    if not rows:
        return []

    table: dict[str, dict[str, float | None]] = {}
    for stage, brand, num, denom in rows:
        if stage not in TABLE_STAGE_MAP:
            continue
        metric_key = TABLE_STAGE_MAP[stage]
        if brand not in table:
            table[brand] = {value: None for value in TABLE_STAGE_MAP.values()}
        pct = None
        if denom and denom > 0:
            pct = round((num / denom) * 100, 1)
        table[brand][metric_key] = pct

    result_rows = []
    for brand, values in table.items():
        result_rows.append(
            {
                "brand": brand,
                "brand_awareness": values.get("brand_awareness"),
                "ad_awareness": values.get("ad_awareness"),
                "brand_consideration": values.get("brand_consideration"),
                "brand_purchase": values.get("brand_purchase"),
                "brand_satisfaction": values.get("brand_satisfaction"),
                "brand_recommendation": values.get("brand_recommendation"),
            }
        )
    return result_rows


def _compute_touchpoint_rows(study_id: str) -> list[dict]:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if not curated_path.exists():
        return []

    conn = get_duckdb_connection()
    load_parquet_as_view(conn, "touchpoints_table", str(curated_path))
    has_touchpoint = (
        conn.execute(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'touchpoints_table' AND column_name = 'touchpoint'
            """
        ).fetchone()[0]
        > 0
    )
    if not has_touchpoint:
        return []

    query = f"""
        WITH base AS (
            SELECT
                brand,
                touchpoint,
                respondent_id,
                TRY_CAST(value AS INTEGER) AS v_int
            FROM touchpoints_table
            WHERE study_id = ?
              AND LOWER(stage) = '{TOUCHPOINT_STAGE_KEY}'
              AND touchpoint IS NOT NULL
              AND TRIM(CAST(touchpoint AS VARCHAR)) <> ''
              AND brand IS NOT NULL
              AND TRIM(CAST(brand AS VARCHAR)) <> ''
              AND TRY_CAST(value AS INTEGER) IS NOT NULL
        ),
        nums AS (
            SELECT brand, touchpoint, COUNT(DISTINCT respondent_id) AS num
            FROM base
            WHERE v_int = 1
            GROUP BY brand, touchpoint
        ),
        denoms AS (
            SELECT brand, touchpoint, COUNT(DISTINCT respondent_id) AS denom
            FROM base
            GROUP BY brand, touchpoint
        )
        SELECT
            d.brand,
            d.touchpoint,
            n.num,
            d.denom
        FROM denoms d
        LEFT JOIN nums n
            ON n.brand = d.brand AND n.touchpoint = d.touchpoint
    """
    rows = conn.execute(query, [study_id]).fetchall()
    if not rows:
        return []

    result_rows = []
    for brand, touchpoint, num, denom in rows:
        recall = None
        if denom and denom > 0:
            recall = round((float(num or 0) / denom) * 100, 1)
        result_rows.append(
            {
                "brand": brand,
                "touchpoint": touchpoint,
                "recall": recall,
            }
        )
    return result_rows


def _compute_touchpoint_rows_filtered(study_id: str, filters: dict) -> list[dict]:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if not curated_path.exists():
        return []

    respondent_cte, respondent_params, eligible = _respondent_filter_cte(study_id, filters)
    if not eligible:
        return []

    conn = get_duckdb_connection()
    load_parquet_as_view(conn, "touchpoints_table", str(curated_path))
    has_touchpoint = (
        conn.execute(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'touchpoints_table' AND column_name = 'touchpoint'
            """
        ).fetchone()[0]
        > 0
    )
    if not has_touchpoint:
        return []

    has_value_raw = (
        conn.execute(
            """
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'touchpoints_table' AND column_name = 'value_raw'
            """
        ).fetchone()[0]
        > 0
    )
    value_expr = (
        "COALESCE(TRY_CAST(value_raw AS INTEGER), TRY_CAST(value AS INTEGER))"
        if has_value_raw
        else "TRY_CAST(value AS INTEGER)"
    )

    respondent_filter = (
        "AND respondent_id IN (SELECT respondent_id FROM filtered_respondents)"
        if respondent_cte
        else ""
    )
    cte_prefix = f"{respondent_cte}," if respondent_cte else ""
    query = f"""
        WITH {cte_prefix}
        base AS (
            SELECT
                brand,
                touchpoint,
                respondent_id,
                {value_expr} AS v_int
            FROM touchpoints_table
            WHERE study_id = ?
              AND LOWER(stage) = '{TOUCHPOINT_STAGE_KEY}'
              AND touchpoint IS NOT NULL
              AND TRIM(CAST(touchpoint AS VARCHAR)) <> ''
              AND brand IS NOT NULL
              AND TRIM(CAST(brand AS VARCHAR)) <> ''
              AND {value_expr} IS NOT NULL
              {respondent_filter}
        ),
        nums AS (
            SELECT brand, touchpoint, COUNT(DISTINCT respondent_id) AS num
            FROM base
            WHERE v_int = 1
            GROUP BY brand, touchpoint
        ),
        denoms AS (
            SELECT brand, touchpoint, COUNT(DISTINCT respondent_id) AS denom
            FROM base
            GROUP BY brand, touchpoint
        )
        SELECT
            d.brand,
            d.touchpoint,
            n.num,
            d.denom
        FROM denoms d
        LEFT JOIN nums n
            ON n.brand = d.brand AND n.touchpoint = d.touchpoint
    """
    params = [*respondent_params, study_id]
    rows = conn.execute(query, params).fetchall()
    if not rows:
        return []

    result_rows = []
    for brand, touchpoint, num, denom in rows:
        recall = None
        if denom and denom > 0:
            recall = round((float(num or 0) / denom) * 100, 1)
        result_rows.append(
            {
                "brand": brand,
                "touchpoint": touchpoint,
                "recall": recall,
            }
        )
    return result_rows


@router.get("/journey", response_model=JourneyResponse)
def journey_analytics(study_id: str = Query(..., description="Study id")) -> JourneyResponse:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if curated_path.exists():
        parquet_path = curated_path
        source = "curated"
    else:
        raise HTTPException(status_code=404, detail="Curated mart not found for study.")

    try:
        conn = get_duckdb_connection()
        load_parquet_as_view(conn, "journey", str(parquet_path))
        query = """
            SELECT stage, brand, AVG(value) * 100 AS percentage
            FROM journey
            WHERE study_id = ?
              AND brand IS NOT NULL
              AND TRIM(CAST(brand AS VARCHAR)) <> ''
            GROUP BY stage, brand
            ORDER BY stage, brand
        """
        rows = conn.execute(query, [study_id]).fetchall()
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"No data available for {study_id}") from exc

    if not rows:
        raise HTTPException(status_code=404, detail=f"No data available for {study_id}")

    data = [JourneyPoint(stage=row[0], brand=row[1], percentage=float(row[2])) for row in rows]
    return JourneyResponse(study_id=study_id, points=data, source=source)


@router.get("/journey/table")
def journey_table(study_id: str = Query(..., description="Study id")) -> dict:
    classification = _classification_for_study(get_repo_root(), study_id)
    result_rows = _compute_table_rows(study_id)

    has_awareness = any(row.get("brand_awareness") is not None for row in result_rows)
    if has_awareness:
        result_rows.sort(key=lambda row: row.get("brand_awareness") or -1, reverse=True)
    else:
        result_rows.sort(key=lambda row: row.get("brand") or "")

    return {
        "study_id": study_id,
        "classification": classification,
        "source": "curated",
        "rows": [
            {
                "sector": classification.get("sector"),
                "subsector": classification.get("subsector"),
                "category": classification.get("category"),
                **row,
            }
            for row in result_rows
        ],
        "notes": {
            "brand_satisfaction_definition": "% of values 4 or 5",
            "brand_recommendation_definition": "% of values 9 or 10",
        },
    }


def _journey_table_multi_filtered(
    filters: dict,
    limit_mode: str,
    sort_by: str,
    sort_dir: str,
) -> dict:
    root = get_repo_root()
    discovered = _discover_curated_studies(root)
    requested = filters.get("study_ids") or []
    if requested:
        study_ids = [study_id for study_id in requested if study_id in discovered]
    else:
        study_ids = discovered

    if not study_ids:
        return {
            "rows": [],
            "meta": {
                "studies_included": [],
                "limit_mode": limit_mode,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
                "row_count": 0,
            },
        }

    rows: list[dict] = []
    for study_id in study_ids:
        classification = _classification_for_study(root, study_id)
        if not _study_matches_taxonomy(filters, classification):
            continue
        safe_classification = {
            "sector": classification.get("sector") or "Unassigned",
            "subsector": classification.get("subsector") or "Unassigned",
            "category": classification.get("category") or "Unassigned",
        }
        filtered_rows = _compute_table_rows_filtered(study_id, filters)
        for row in filtered_rows:
            rows.append(
                {
                    "study_id": study_id,
                    "study_name": study_id,
                    **safe_classification,
                    **row,
                }
            )

    if sort_by not in SORTABLE_METRICS:
        sort_by = "brand_awareness"
    reverse = sort_dir.lower() != "asc"
    rows.sort(key=lambda row: row.get(sort_by) or -1, reverse=reverse)

    limit_mode = limit_mode.lower()
    if limit_mode == "top25":
        rows = rows[:25]
    elif limit_mode == "top10":
        rows = rows[:10]

    return {
        "rows": rows,
        "meta": {
            "studies_included": study_ids,
            "limit_mode": limit_mode,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "row_count": len(rows),
        },
    }


@router.get("/journey/table_multi")
def journey_table_multi(
    studies: str | None = Query(None, description="Comma-separated study ids"),
    limit_mode: str = Query("top10", description="top10|top25|all"),
    sort_by: str = Query("brand_awareness", description="Metric to sort by"),
    sort_dir: str = Query("desc", description="asc|desc"),
) -> dict:
    filters = _parse_filters({})
    if studies:
        filters["study_ids"] = [study.strip() for study in studies.split(",") if study.strip()]
    return _journey_table_multi_filtered(filters, limit_mode, sort_by, sort_dir)


@router.post("/journey/table_multi")
async def journey_table_multi_post(
    request: Request,
    limit_mode: str = Query("top10", description="top10|top25|all"),
    sort_by: str = Query("brand_awareness", description="Metric to sort by"),
    sort_dir: str = Query("desc", description="asc|desc"),
) -> dict:
    payload = await request.json()
    filters = _parse_filters(payload)
    return _journey_table_multi_filtered(filters, limit_mode, sort_by, sort_dir)


def _touchpoints_table_multi_filtered(
    filters: dict,
    limit_mode: str,
    sort_by: str,
    sort_dir: str,
) -> dict:
    root = get_repo_root()
    discovered = _discover_curated_studies(root)
    requested = filters.get("study_ids") or []
    if requested:
        study_ids = [study_id for study_id in requested if study_id in discovered]
    else:
        study_ids = discovered

    if not study_ids:
        return {
            "rows": [],
            "meta": {
                "studies_included": [],
                "limit_mode": limit_mode,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
                "row_count": 0,
            },
        }

    rows: list[dict] = []
    for study_id in study_ids:
        classification = _classification_for_study(root, study_id)
        if not _study_matches_taxonomy(filters, classification):
            continue
        safe_classification = {
            "sector": classification.get("sector") or "Unassigned",
            "subsector": classification.get("subsector") or "Unassigned",
            "category": classification.get("category") or "Unassigned",
        }
        filtered_rows = _compute_touchpoint_rows_filtered(study_id, filters)
        for row in filtered_rows:
            rows.append(
                {
                    "study_id": study_id,
                    "study_name": study_id,
                    **safe_classification,
                    **row,
                }
            )

    reverse = sort_dir.lower() != "asc"
    rows.sort(key=lambda row: row.get("recall") or -1, reverse=reverse)

    limit_mode = limit_mode.lower()
    if limit_mode == "top10":
        rows = rows[:10]
    elif limit_mode == "top25":
        rows = rows[:25]

    return {
        "rows": rows,
        "meta": {
            "studies_included": study_ids,
            "limit_mode": limit_mode,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "row_count": len(rows),
        },
    }


@router.get("/touchpoints/table_multi")
def touchpoints_table_multi(
    studies: str | None = Query(None, description="Comma-separated study ids"),
    limit_mode: str = Query("top25", description="top10|top25|all"),
    sort_by: str = Query("recall", description="Metric to sort by"),
    sort_dir: str = Query("desc", description="asc|desc"),
) -> dict:
    filters = _parse_filters({})
    if studies:
        filters["study_ids"] = [study.strip() for study in studies.split(",") if study.strip()]
    return _touchpoints_table_multi_filtered(filters, limit_mode, sort_by, sort_dir)


@router.post("/touchpoints/table_multi")
async def touchpoints_table_multi_post(
    request: Request,
    limit_mode: str = Query("top25", description="top10|top25|all"),
    sort_by: str = Query("recall", description="Metric to sort by"),
    sort_dir: str = Query("desc", description="asc|desc"),
) -> dict:
    payload = await request.json()
    filters = _parse_filters(payload)
    return _touchpoints_table_multi_filtered(filters, limit_mode, sort_by, sort_dir)
