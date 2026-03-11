import json
import time
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request

from app.data.demographics import load_demographics_config, normalize_demographics_config
from app.data.warehouse import get_duckdb_connection, get_repo_root, load_parquet_as_view
from app.models.schemas import JourneyPoint, JourneyResponse

router = APIRouter()

JOURNEY_TABLE_MULTI_CACHE_TTL_SECONDS = 45
_JOURNEY_TABLE_MULTI_CACHE: dict[str, tuple[float, dict]] = {}

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
TOUCHPOINT_STAGE_METRICS = {
    "touchpoints": "recall",
    "awareness": "recall",
    "consideration": "consideration",
    "brand_consideration": "consideration",
    "purchase": "purchase",
    "brand_purchase": "purchase",
}

CORE_AWARENESS_BOUNDED_METRICS = (
    "brand_consideration",
    "brand_purchase",
    "brand_satisfaction",
    "brand_recommendation",
)


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


def _parquet_columns(path: Path) -> set[str]:
    if not path.exists():
        return set()
    conn = get_duckdb_connection()
    try:
        cursor = conn.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0")
        return {column[0] for column in cursor.description}
    finally:
        conn.close()


def _column_or_null(columns: set[str], column_name: str) -> str:
    return column_name if column_name in columns else f"NULL AS {column_name}"


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


def _journey_table_multi_cache_key(
    filters: dict,
    limit_mode: str,
    sort_by: str,
    sort_dir: str,
    include_global_benchmark: bool,
    response_mode: str,
) -> str:
    payload = {
        "filters": filters,
        "limit_mode": limit_mode.lower(),
        "sort_by": sort_by,
        "sort_dir": sort_dir.lower(),
        "include_global_benchmark": include_global_benchmark,
        "response_mode": response_mode,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _journey_table_multi_get_cached(key: str) -> dict | None:
    entry = _JOURNEY_TABLE_MULTI_CACHE.get(key)
    if not entry:
        return None
    created_at, payload = entry
    if time.time() - created_at > JOURNEY_TABLE_MULTI_CACHE_TTL_SECONDS:
        _JOURNEY_TABLE_MULTI_CACHE.pop(key, None)
        return None
    return payload


def _journey_table_multi_set_cached(key: str, payload: dict) -> None:
    _JOURNEY_TABLE_MULTI_CACHE[key] = (time.time(), payload)


def _resolve_study_ids(filters: dict) -> tuple[Path, list[str]]:
    root = get_repo_root()
    discovered = _discover_curated_studies(root)
    requested = filters.get("study_ids") or []
    if requested:
        study_ids = [study_id for study_id in requested if study_id in discovered]
    else:
        study_ids = discovered
    return root, study_ids


def _collect_journey_rows(
    root: Path,
    study_ids: list[str],
    filters: dict,
    classification_cache: dict[str, dict[str, str | None]] | None = None,
) -> tuple[list[dict], list[str], int]:
    rows: list[dict] = []
    matched_studies: list[str] = []
    local_classification_cache = classification_cache if classification_cache is not None else {}
    for study_id in study_ids:
        classification = local_classification_cache.get(study_id)
        if classification is None:
            classification = _classification_for_study(root, study_id)
            local_classification_cache[study_id] = classification
        if not _study_matches_taxonomy(filters, classification):
            continue
        safe_classification = {
            "sector": classification.get("sector") or "Unassigned",
            "subsector": classification.get("subsector") or "Unassigned",
            "category": classification.get("category") or "Unassigned",
        }
        filtered_rows = _compute_table_rows_filtered(study_id, filters)
        if filtered_rows:
            matched_studies.append(study_id)
        for row in filtered_rows:
            rows.append(
                {
                    "study_id": study_id,
                    "study_name": study_id,
                    **safe_classification,
                    **row,
                    "quality_flags": {
                        "population_denominator": bool(row.get("base_n_population")),
                        "awareness_ceiling_applied": bool(row.get("awareness_ceiling_applied")),
                    },
                }
            )
    return rows, matched_studies, len(study_ids)


def _sort_and_limit_journey_rows(rows: list[dict], sort_by: str, sort_dir: str, limit_mode: str) -> list[dict]:
    sort_metric = sort_by if sort_by in SORTABLE_METRICS else "brand_awareness"
    reverse = sort_dir.lower() != "asc"
    sorted_rows = rows[:]
    sorted_rows.sort(key=lambda row: row.get(sort_metric) or -1, reverse=reverse)
    normalized_limit = limit_mode.lower()
    if normalized_limit == "top25":
        return sorted_rows[:25]
    if normalized_limit == "top10":
        return sorted_rows[:10]
    return sorted_rows


def _weighted_metric_average(rows: list[dict], metric: str) -> float | None:
    points: list[tuple[float, float]] = []
    for row in rows:
        value = row.get(metric)
        weight = row.get("base_n_population") or row.get("aggregation_weight_n")
        if not isinstance(value, (int, float)):
            continue
        if not isinstance(weight, (int, float)) or weight <= 0:
            weight = 1.0
        points.append((float(value), float(weight)))
    if not points:
        return None
    total_weight = sum(weight for _, weight in points)
    if total_weight <= 0:
        return None
    return round(sum(value * weight for value, weight in points) / total_weight, 1)


def _build_benchmark_summary(rows: list[dict], label: str) -> dict:
    stage_keys = [
        "brand_awareness",
        "ad_awareness",
        "brand_consideration",
        "brand_purchase",
        "brand_satisfaction",
        "brand_recommendation",
    ]
    stage_values = {key: _weighted_metric_average(rows, key) for key in stage_keys}
    link_pairs = [
        ("brand_awareness", "brand_consideration"),
        ("brand_consideration", "brand_purchase"),
        ("brand_purchase", "brand_satisfaction"),
        ("brand_satisfaction", "brand_recommendation"),
    ]
    links = []
    for from_key, to_key in link_pairs:
        from_value = stage_values.get(from_key)
        to_value = stage_values.get(to_key)
        if isinstance(from_value, (int, float)) and isinstance(to_value, (int, float)) and from_value > 0:
            conversion = round((to_value / from_value) * 100, 1)
            drop_abs = round(from_value - to_value, 1)
        else:
            conversion = None
            drop_abs = None
        links.append({"from": from_key, "to": to_key, "conversion_pct": conversion, "drop_abs_pts": drop_abs})
    return {
        "label": label,
        "stages": stage_values,
        "links": links,
        "csat": _weighted_metric_average(rows, "csat"),
        "nps": _weighted_metric_average(rows, "nps"),
        "journey_index": None,
        "funnel_health": None,
    }


def _apply_awareness_ceiling(values: dict[str, float | None]) -> bool:
    """
    Enforce questionnaire hierarchy consistency per study-brand row:
    downstream brand stages cannot exceed Brand Awareness when both exist.
    """
    awareness = values.get("brand_awareness")
    if awareness is None:
        return False
    adjusted = False
    for metric in CORE_AWARENESS_BOUNDED_METRICS:
        stage_value = values.get(metric)
        if stage_value is None:
            continue
        if stage_value > awareness:
            values[metric] = awareness
            adjusted = True
    return adjusted


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

    respondent_columns = _parquet_columns(respondents_path)
    if "respondent_id" not in respondent_columns:
        return None, [], False

    config = normalize_demographics_config(load_demographics_config(study_id))
    gender_var = config.get("gender_var")
    nse_var = config.get("nse_var")
    state_var = config.get("state_var")
    age_var = config.get("age_var")
    date_mode = (config.get("date") or {}).get("mode", "none")

    if filters.get("gender") and (not gender_var or "gender_code" not in respondent_columns):
        return None, [], False
    if filters.get("nse") and (not nse_var or "nse_code" not in respondent_columns):
        return None, [], False
    if filters.get("state") and (not state_var or "state_code" not in respondent_columns):
        return None, [], False
    if (filters.get("age_min") is not None or filters.get("age_max") is not None) and (
        not age_var or "age" not in respondent_columns
    ):
        return None, [], False
    if (
        filters.get("quarter_from")
        or filters.get("quarter_to")
        or filters.get("date_from")
        or filters.get("date_to")
    ) and (date_mode == "none" or "date" not in respondent_columns):
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
            SELECT
                respondent_id,
                {_column_or_null(respondent_columns, "gender_code")},
                {_column_or_null(respondent_columns, "nse_code")},
                {_column_or_null(respondent_columns, "state_code")},
                {_column_or_null(respondent_columns, "age")},
                {_column_or_null(respondent_columns, "date")}
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


def _compute_table_rows_internal(
    study_id: str,
    respondent_cte: str | None,
    respondent_params: list,
    strict_missing: bool,
) -> list[dict]:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if not curated_path.exists():
        if strict_missing:
            raise HTTPException(status_code=404, detail="Curated mart not found for study.")
        return []

    conn = get_duckdb_connection()
    try:
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
            population AS (
                SELECT
                    COUNT(DISTINCT respondent_id) AS population_n
                FROM (
                    SELECT respondent_id
                    FROM journey_table
                    WHERE study_id = ?
                      AND respondent_id IS NOT NULL
                      {respondent_filter}
                ) pop
            ),
            stage_stats AS (
                SELECT stage, MAX(v_int) AS max_v
                FROM base
                WHERE v_int IS NOT NULL
                GROUP BY stage
            ),
            stage_nums AS (
                SELECT
                    b.brand,
                    COUNT(DISTINCT CASE WHEN b.stage = 'awareness' AND b.v_int = 1 THEN b.respondent_id END) AS awareness_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'ad_awareness' AND b.v_int = 1 THEN b.respondent_id END) AS ad_awareness_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'consideration' AND b.v_int = 1 THEN b.respondent_id END) AS consideration_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'purchase' AND b.v_int = 1 THEN b.respondent_id END) AS purchase_num,
                    COUNT(
                        DISTINCT CASE
                            WHEN b.stage = 'satisfaction'
                                 AND (
                                    (ss.max_v IS NOT NULL AND ss.max_v >= 5 AND b.v_int IN (4, 5))
                                    OR (ss.max_v IS NOT NULL AND ss.max_v < 5 AND b.v_int = 1)
                                 )
                            THEN b.respondent_id
                        END
                    ) AS satisfaction_num,
                    COUNT(
                        DISTINCT CASE
                            WHEN b.stage = 'recommendation'
                                 AND (
                                    (rs.max_v IS NOT NULL AND rs.max_v >= 9 AND b.v_int IN (9, 10))
                                    OR (rs.max_v IS NOT NULL AND rs.max_v < 9 AND b.v_int = 1)
                                 )
                            THEN b.respondent_id
                        END
                    ) AS recommendation_num
                FROM base b
                LEFT JOIN stage_stats ss ON ss.stage = 'satisfaction'
                LEFT JOIN stage_stats rs ON rs.stage = 'recommendation'
                WHERE b.v_int IS NOT NULL
                GROUP BY b.brand
            ),
            purchasers AS (
                SELECT brand, respondent_id
                FROM base
                WHERE stage = 'purchase' AND v_int = 1 AND respondent_id IS NOT NULL
                GROUP BY brand, respondent_id
            ),
            satisfaction_by_resp AS (
                SELECT brand, respondent_id, MAX(v_int) AS sat_v
                FROM base
                WHERE stage = 'satisfaction' AND v_int IS NOT NULL AND respondent_id IS NOT NULL
                GROUP BY brand, respondent_id
            ),
            recommendation_by_resp AS (
                SELECT brand, respondent_id, MAX(v_int) AS rec_v
                FROM base
                WHERE stage = 'recommendation' AND v_int IS NOT NULL AND respondent_id IS NOT NULL
                GROUP BY brand, respondent_id
            ),
            experience AS (
                SELECT
                    p.brand,
                    COUNT(DISTINCT p.respondent_id) AS purchase_n,
                    COUNT(
                        DISTINCT CASE
                            WHEN ss.max_v IS NOT NULL AND ss.max_v >= 5 AND s.sat_v IN (4, 5) THEN p.respondent_id
                            WHEN ss.max_v IS NOT NULL AND ss.max_v < 5 AND s.sat_v = 1 THEN p.respondent_id
                            ELSE NULL
                        END
                    ) AS top2_n,
                    COUNT(
                        DISTINCT CASE
                            WHEN ss.max_v IS NOT NULL AND ss.max_v >= 5 AND s.sat_v IN (1, 2) THEN p.respondent_id
                            ELSE NULL
                        END
                    ) AS bottom2_n,
                    COUNT(
                        DISTINCT CASE
                            WHEN rs.max_v IS NOT NULL AND rs.max_v >= 9 AND r.rec_v IN (9, 10) THEN p.respondent_id
                            WHEN rs.max_v IS NOT NULL AND rs.max_v < 9 AND r.rec_v = 1 THEN p.respondent_id
                            ELSE NULL
                        END
                    ) AS promoters_n,
                    COUNT(
                        DISTINCT CASE
                            WHEN rs.max_v IS NOT NULL AND rs.max_v >= 9 AND r.rec_v BETWEEN 0 AND 6 THEN p.respondent_id
                            WHEN rs.max_v IS NOT NULL AND rs.max_v < 9 AND r.rec_v = 0 THEN p.respondent_id
                            ELSE NULL
                        END
                    ) AS detractors_n
                FROM purchasers p
                LEFT JOIN satisfaction_by_resp s
                  ON s.brand = p.brand AND s.respondent_id = p.respondent_id
                LEFT JOIN recommendation_by_resp r
                  ON r.brand = p.brand AND r.respondent_id = p.respondent_id
                LEFT JOIN stage_stats ss ON ss.stage = 'satisfaction'
                LEFT JOIN stage_stats rs ON rs.stage = 'recommendation'
                GROUP BY p.brand, ss.max_v, rs.max_v
            ),
            brands AS (
                SELECT brand FROM stage_nums
                UNION
                SELECT brand FROM experience
            )
            SELECT
                b.brand,
                p.population_n,
                s.awareness_num,
                s.ad_awareness_num,
                s.consideration_num,
                s.purchase_num,
                s.satisfaction_num,
                s.recommendation_num,
                e.purchase_n,
                e.top2_n,
                e.bottom2_n,
                e.promoters_n,
                e.detractors_n
            FROM brands b
            CROSS JOIN population p
            LEFT JOIN stage_nums s ON s.brand = b.brand
            LEFT JOIN experience e ON e.brand = b.brand
        """
        params = [*respondent_params, study_id, study_id]
        rows = conn.execute(query, params).fetchall()
        if not rows:
            if strict_missing:
                raise HTTPException(status_code=404, detail=f"No curated data for {study_id}.")
            return []

        result_rows: list[dict] = []
        for (
            brand,
            population_n,
            awareness_num,
            ad_awareness_num,
            consideration_num,
            purchase_num,
            satisfaction_num,
            recommendation_num,
            purchaser_n,
            top2_n,
            bottom2_n,
            promoters_n,
            detractors_n,
        ) in rows:
            if not brand:
                continue
            values: dict[str, float | None] = {value: None for value in TABLE_STAGE_MAP.values()}
            values["base_n_population"] = population_n if population_n and population_n > 0 else None
            values["aggregation_weight_n"] = population_n if population_n and population_n > 0 else None

            if population_n and population_n > 0:
                numerator_map = {
                    "brand_awareness": awareness_num,
                    "ad_awareness": ad_awareness_num,
                    "brand_consideration": consideration_num,
                    "brand_purchase": purchase_num,
                    "brand_satisfaction": satisfaction_num,
                    "brand_recommendation": recommendation_num,
                }
                for metric, numerator in numerator_map.items():
                    if numerator is None:
                        continue
                    values[metric] = round((float(numerator) / float(population_n)) * 100, 1)

            if purchaser_n and purchaser_n > 0:
                values["csat"] = round(((float(top2_n or 0) - float(bottom2_n or 0)) / float(purchaser_n)) * 100, 1)
                values["nps"] = round(((float(promoters_n or 0) - float(detractors_n or 0)) / float(purchaser_n)) * 100, 1)
            else:
                values["csat"] = None
                values["nps"] = None

            awareness_ceiling_applied = _apply_awareness_ceiling(values)
            result_rows.append(
                {
                    "brand": brand,
                    "brand_awareness": values.get("brand_awareness"),
                    "ad_awareness": values.get("ad_awareness"),
                    "brand_consideration": values.get("brand_consideration"),
                    "brand_purchase": values.get("brand_purchase"),
                    "brand_satisfaction": values.get("brand_satisfaction"),
                    "brand_recommendation": values.get("brand_recommendation"),
                    "csat": values.get("csat"),
                    "nps": values.get("nps"),
                    "base_n_population": values.get("base_n_population"),
                    "aggregation_weight_n": values.get("aggregation_weight_n"),
                    "awareness_ceiling_applied": awareness_ceiling_applied,
                }
            )

        return result_rows
    finally:
        conn.close()


def _compute_table_rows(study_id: str) -> list[dict]:
    return _compute_table_rows_internal(study_id, None, [], True)


def _compute_table_rows_filtered(study_id: str, filters: dict) -> list[dict]:
    respondent_cte, respondent_params, eligible = _respondent_filter_cte(study_id, filters)
    if not eligible:
        return []
    return _compute_table_rows_internal(study_id, respondent_cte, respondent_params, False)


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
                LOWER(stage) AS stage,
                brand,
                touchpoint,
                respondent_id,
                TRY_CAST(value AS INTEGER) AS v_int
            FROM touchpoints_table
            WHERE study_id = ?
              AND LOWER(stage) IN ('touchpoints', 'awareness', 'consideration', 'brand_consideration', 'purchase', 'brand_purchase')
              AND touchpoint IS NOT NULL
              AND TRIM(CAST(touchpoint AS VARCHAR)) <> ''
              AND brand IS NOT NULL
              AND TRIM(CAST(brand AS VARCHAR)) <> ''
              AND TRY_CAST(value AS INTEGER) IS NOT NULL
        ),
        nums AS (
            SELECT stage, brand, touchpoint, COUNT(DISTINCT respondent_id) AS num
            FROM base
            WHERE v_int = 1
            GROUP BY stage, brand, touchpoint
        ),
        denoms AS (
            SELECT stage, brand, touchpoint, COUNT(DISTINCT respondent_id) AS denom
            FROM base
            GROUP BY stage, brand, touchpoint
        )
        SELECT
            d.stage,
            d.brand,
            d.touchpoint,
            n.num,
            d.denom
        FROM denoms d
        LEFT JOIN nums n
            ON n.stage = d.stage AND n.brand = d.brand AND n.touchpoint = d.touchpoint
    """
    rows = conn.execute(query, [study_id]).fetchall()
    if not rows:
        return []

    values_by_pair: dict[tuple[str, str], dict[str, float | None]] = {}
    for stage, brand, touchpoint, num, denom in rows:
        metric = TOUCHPOINT_STAGE_METRICS.get(str(stage).lower())
        if not metric:
            continue
        key = (brand, touchpoint)
        if key not in values_by_pair:
            values_by_pair[key] = {"recall": None, "consideration": None, "purchase": None}
        value = None
        if denom and denom > 0:
            value = round((float(num or 0) / denom) * 100, 1)
        values_by_pair[key][metric] = value

    result_rows = []
    for (brand, touchpoint), values in values_by_pair.items():
        result_rows.append(
            {
                "brand": brand,
                "touchpoint": touchpoint,
                "recall": values.get("recall"),
                "consideration": values.get("consideration"),
                "purchase": values.get("purchase"),
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
                LOWER(stage) AS stage,
                brand,
                touchpoint,
                respondent_id,
                {value_expr} AS v_int
            FROM touchpoints_table
            WHERE study_id = ?
              AND LOWER(stage) IN ('touchpoints', 'awareness', 'consideration', 'brand_consideration', 'purchase', 'brand_purchase')
              AND touchpoint IS NOT NULL
              AND TRIM(CAST(touchpoint AS VARCHAR)) <> ''
              AND brand IS NOT NULL
              AND TRIM(CAST(brand AS VARCHAR)) <> ''
              AND {value_expr} IS NOT NULL
              {respondent_filter}
        ),
        nums AS (
            SELECT stage, brand, touchpoint, COUNT(DISTINCT respondent_id) AS num
            FROM base
            WHERE v_int = 1
            GROUP BY stage, brand, touchpoint
        ),
        denoms AS (
            SELECT stage, brand, touchpoint, COUNT(DISTINCT respondent_id) AS denom
            FROM base
            GROUP BY stage, brand, touchpoint
        )
        SELECT
            d.stage,
            d.brand,
            d.touchpoint,
            n.num,
            d.denom
        FROM denoms d
        LEFT JOIN nums n
            ON n.stage = d.stage AND n.brand = d.brand AND n.touchpoint = d.touchpoint
    """
    params = [*respondent_params, study_id]
    rows = conn.execute(query, params).fetchall()
    if not rows:
        return []

    values_by_pair: dict[tuple[str, str], dict[str, float | None]] = {}
    for stage, brand, touchpoint, num, denom in rows:
        metric = TOUCHPOINT_STAGE_METRICS.get(str(stage).lower())
        if not metric:
            continue
        key = (brand, touchpoint)
        if key not in values_by_pair:
            values_by_pair[key] = {"recall": None, "consideration": None, "purchase": None}
        value = None
        if denom and denom > 0:
            value = round((float(num or 0) / denom) * 100, 1)
        values_by_pair[key][metric] = value

    result_rows = []
    for (brand, touchpoint), values in values_by_pair.items():
        result_rows.append(
            {
                "brand": brand,
                "touchpoint": touchpoint,
                "recall": values.get("recall"),
                "consideration": values.get("consideration"),
                "purchase": values.get("purchase"),
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
            "csat_definition": "(% values 4-5 - % values 1-2) among purchasers",
            "brand_recommendation_definition": "% of values 9 or 10",
            "nps_definition": "(% values 9-10 - % values 0-6) among purchasers",
            "quality_guardrail": "Brand Consideration/Purchase/Satisfaction/Recommendation are capped at Brand Awareness per study-brand.",
        },
    }


def _journey_table_multi_filtered(
    filters: dict,
    limit_mode: str,
    sort_by: str,
    sort_dir: str,
    include_global_benchmark: bool = False,
    response_mode: str = "full",
) -> dict:
    started_at = time.perf_counter()
    normalized_limit = limit_mode.lower()
    sort_metric = sort_by if sort_by in SORTABLE_METRICS else "brand_awareness"
    normalized_mode = response_mode.lower()
    if normalized_mode not in {"benchmark_global", "benchmark_selection", "full"}:
        normalized_mode = "full"
    if normalized_mode != "full":
        include_global_benchmark = False
    cache_key = _journey_table_multi_cache_key(
        filters, normalized_limit, sort_metric, sort_dir, include_global_benchmark, normalized_mode
    )
    cached = _journey_table_multi_get_cached(cache_key)
    if cached:
        cached_payload = dict(cached)
        meta = dict(cached_payload.get("meta") or {})
        meta["cache_hit"] = True
        cached_payload["meta"] = meta
        return cached_payload

    root, study_ids = _resolve_study_ids(filters)

    if not study_ids:
        payload = {
            "rows": [],
            "meta": {
                "studies_included": [],
                "limit_mode": normalized_limit,
                "sort_by": sort_metric,
                "sort_dir": sort_dir,
                "row_count": 0,
                "cache_hit": False,
                "studies_processed": 0,
                "query_ms": 0,
                "collect_ms": 0,
                "aggregate_ms": 0,
                "total_ms": round((time.perf_counter() - started_at) * 1000, 2),
            },
        }
        _journey_table_multi_set_cached(cache_key, payload)
        return payload

    classification_cache: dict[str, dict[str, str | None]] = {}
    collect_started = time.perf_counter()
    selection_rows_all: list[dict] = []
    selection_rows: list[dict] = []
    selection_studies: list[str] = []
    global_rows_all: list[dict] = []
    global_rows: list[dict] = []
    global_studies: list[str] = []
    processed = len(study_ids)

    if normalized_mode in {"benchmark_selection", "full"}:
        selection_rows_all, selection_studies, processed = _collect_journey_rows(
            root, study_ids, filters, classification_cache
        )

    if normalized_mode in {"benchmark_global"} or include_global_benchmark:
        global_filters = dict(filters)
        global_filters["sector"] = None
        global_filters["subsector"] = None
        global_filters["category"] = None
        global_rows_all, global_studies, _ = _collect_journey_rows(
            root, study_ids, global_filters, classification_cache
        )
    collect_ms = round((time.perf_counter() - collect_started) * 1000, 2)

    aggregate_started = time.perf_counter()
    if normalized_mode in {"benchmark_selection", "full"}:
        selection_rows = _sort_and_limit_journey_rows(selection_rows_all, sort_metric, sort_dir, normalized_limit)
    if normalized_mode in {"benchmark_global"} or include_global_benchmark:
        global_rows = _sort_and_limit_journey_rows(global_rows_all, sort_metric, sort_dir, normalized_limit)
    aggregate_ms = round((time.perf_counter() - aggregate_started) * 1000, 2)
    query_ms = round(collect_ms + aggregate_ms, 2)

    payload: dict = {
        "rows": selection_rows,
        "meta": {
            "studies_included": selection_studies,
            "limit_mode": normalized_limit,
            "sort_by": sort_metric,
            "sort_dir": sort_dir,
            "row_count": len(selection_rows),
            "cache_hit": False,
            "studies_processed": processed,
            "query_ms": query_ms,
            "collect_ms": collect_ms,
            "aggregate_ms": aggregate_ms,
            "total_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "response_mode": normalized_mode,
        },
    }
    if normalized_mode == "benchmark_global":
        payload["rows"] = []
        payload["summary_global"] = _build_benchmark_summary(global_rows_all, "Global Benchmark")
        payload["global_rows"] = global_rows
        payload["meta"]["global_row_count"] = len(global_rows)
        payload["meta"]["global_studies_included"] = global_studies
    elif normalized_mode == "benchmark_selection":
        payload["summary_selection"] = _build_benchmark_summary(selection_rows_all, "Selection Benchmark")
        payload["meta"]["selection_row_count"] = len(selection_rows)
        payload["meta"]["selection_studies_included"] = selection_studies
    elif include_global_benchmark:
        payload["summary_selection"] = _build_benchmark_summary(selection_rows_all, "Selection Benchmark")
        payload["summary_global"] = _build_benchmark_summary(global_rows_all, "Global Benchmark")
        payload["selection_rows"] = selection_rows
        payload["global_rows"] = global_rows
        payload["meta"]["selection_row_count"] = len(selection_rows)
        payload["meta"]["global_row_count"] = len(global_rows)
        payload["meta"]["selection_studies_included"] = selection_studies
        payload["meta"]["global_studies_included"] = global_studies

    _journey_table_multi_set_cached(cache_key, payload)
    return payload


@router.get("/journey/table_multi")
def journey_table_multi(
    studies: str | None = Query(None, description="Comma-separated study ids"),
    limit_mode: str = Query("top10", description="top10|top25|all"),
    sort_by: str = Query("brand_awareness", description="Metric to sort by"),
    sort_dir: str = Query("desc", description="asc|desc"),
    include_global_benchmark: bool = Query(False, description="Include selection and global rows in one response"),
    response_mode: str = Query("full", description="full|benchmark_global|benchmark_selection"),
) -> dict:
    filters = _parse_filters({})
    if studies:
        filters["study_ids"] = [study.strip() for study in studies.split(",") if study.strip()]
    return _journey_table_multi_filtered(
        filters, limit_mode, sort_by, sort_dir, include_global_benchmark, response_mode
    )


@router.post("/journey/table_multi")
async def journey_table_multi_post(
    request: Request,
    limit_mode: str = Query("top10", description="top10|top25|all"),
    sort_by: str = Query("brand_awareness", description="Metric to sort by"),
    sort_dir: str = Query("desc", description="asc|desc"),
    include_global_benchmark: bool = Query(False, description="Include selection and global rows in one response"),
    response_mode: str = Query("full", description="full|benchmark_global|benchmark_selection"),
) -> dict:
    payload = await request.json()
    filters = _parse_filters(payload)
    return _journey_table_multi_filtered(
        filters, limit_mode, sort_by, sort_dir, include_global_benchmark, response_mode
    )


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
    metric_sort_key = sort_by if sort_by in {"recall", "consideration", "purchase"} else "recall"
    rows.sort(key=lambda row: row.get(metric_sort_key) or -1, reverse=reverse)

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
