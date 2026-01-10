from pathlib import Path

from fastapi import APIRouter, HTTPException, Query

from app.data.warehouse import get_duckdb_connection, get_repo_root, load_parquet_as_view
from app.models.schemas import JourneyPoint, JourneyResponse

router = APIRouter()

TABLE_STAGES = ["awareness", "consideration", "purchase"]


@router.get("/journey", response_model=JourneyResponse)
def journey_analytics(study_id: str = Query(..., description="Study id")) -> JourneyResponse:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    demo_path = root / "data" / "warehouse" / "curated" / "fact_journey_demo.parquet"

    if curated_path.exists():
        parquet_path = curated_path
        source = "curated"
    else:
        parquet_path = demo_path
        source = "demo"
        if not parquet_path.exists():
            raise HTTPException(status_code=404, detail="Demo data not seeded yet.")

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

    conn = get_duckdb_connection()
    load_parquet_as_view(conn, "journey_table", str(curated_path))
    query = """
        SELECT
            LOWER(stage) AS stage,
            brand,
            COUNT(DISTINCT CASE WHEN value = 1 THEN respondent_id END) AS num
        FROM journey_table
        WHERE study_id = ?
          AND brand IS NOT NULL
          AND TRIM(CAST(brand AS VARCHAR)) <> ''
        GROUP BY LOWER(stage), brand
    """
    rows = conn.execute(query, [study_id]).fetchall()
    denom_row = conn.execute(
        "SELECT COUNT(DISTINCT respondent_id) AS denom FROM journey_table WHERE study_id = ?",
        [study_id],
    ).fetchone()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No curated data for {study_id}.")

    denom = denom_row[0] if denom_row else 0

    table: dict[str, dict[str, float | None]] = {}
    for stage, brand, num in rows:
        if stage not in TABLE_STAGES:
            continue
        if brand not in table:
            table[brand] = {key: None for key in TABLE_STAGES}
        pct = None
        if denom and denom > 0:
            pct = round((num / denom) * 100, 1)
        table[brand][stage] = pct

    result_rows = []
    for brand, values in table.items():
        result_rows.append(
            {
                "brand": brand,
                "awareness": values.get("awareness"),
                "consideration": values.get("consideration"),
                "purchase": values.get("purchase"),
            }
        )

    has_awareness = any(row.get("awareness") is not None for row in result_rows)
    if has_awareness:
        result_rows.sort(key=lambda row: row.get("awareness") or -1, reverse=True)
    else:
        result_rows.sort(key=lambda row: row.get("brand") or "")

    return {"study_id": study_id, "source": "curated", "rows": result_rows}
