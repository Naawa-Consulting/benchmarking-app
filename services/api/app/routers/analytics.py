import json
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

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
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if not curated_path.exists():
        raise HTTPException(
            status_code=404,
            detail="Curated mart not found. Run /marts/journey/build first.",
        )

    classification_path = (
        root
        / "data"
        / "warehouse"
        / "taxonomy"
        / "study_classification"
        / f"study_id={study_id}.json"
    )
    classification = {"sector": None, "subsector": None, "category": None}
    if classification_path.exists():
        try:
            classification = json.loads(classification_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            classification = json.loads(classification_path.read_text(encoding="utf-8-sig"))

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
    value_expr = "COALESCE(TRY_CAST(value_raw AS INTEGER), TRY_CAST(value AS INTEGER))" if has_value_raw else "TRY_CAST(value AS INTEGER)"
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
                "sector": classification.get("sector"),
                "subsector": classification.get("subsector"),
                "category": classification.get("category"),
                "brand": brand,
                "brand_awareness": values.get("brand_awareness"),
                "ad_awareness": values.get("ad_awareness"),
                "brand_consideration": values.get("brand_consideration"),
                "brand_purchase": values.get("brand_purchase"),
                "brand_satisfaction": values.get("brand_satisfaction"),
                "brand_recommendation": values.get("brand_recommendation"),
            }
        )

    has_awareness = any(row.get("brand_awareness") is not None for row in result_rows)
    if has_awareness:
        result_rows.sort(key=lambda row: row.get("brand_awareness") or -1, reverse=True)
    else:
        result_rows.sort(key=lambda row: row.get("brand") or "")

    return {
        "study_id": study_id,
        "classification": classification,
        "source": "curated",
        "rows": result_rows,
        "notes": {
            "brand_satisfaction_definition": "% of values 4 or 5",
            "brand_recommendation_definition": "% of values 9 or 10",
        },
    }
