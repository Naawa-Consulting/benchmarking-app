from pathlib import Path

import re

from fastapi import APIRouter, HTTPException, Query

from app.data.ingest_from_landing import ensure_raw_from_landing
from app.data.warehouse import get_duckdb_connection, get_repo_root, load_parquet_as_view
from app.models.schemas import PreviewVariable, Study, StudyPreviewResponse

router = APIRouter()


def _slugify_landing(stem: str) -> str:
    value = stem.strip().lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_]", "", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_") or "study"


@router.get("/")
def list_studies(sync: bool = Query(False, description="Sync from landing")):
    root = get_repo_root()
    base_data_dir = root / "data"

    sync_summary = None
    if sync:
        sync_summary = ensure_raw_from_landing(base_data_dir)

    landing_dir = base_data_dir / "landing"
    landing_files = (
        {_slugify_landing(path.stem): path.name for path in landing_dir.glob("*.sav")}
        if landing_dir.exists()
        else {}
    )

    studies: list[Study] = []
    seen: set[str] = set()

    raw_root = root / "data" / "warehouse" / "raw"
    if raw_root.exists():
        for path in sorted(raw_root.glob("study_id=*")):
            study_id = path.name.replace("study_id=", "", 1)
            curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
            if study_id and study_id not in seen:
                studies.append(
                    Study(
                        id=study_id,
                        name=study_id,
                        source="raw",
                        raw_ready=True,
                        curated_ready=curated_path.exists(),
                        landing_file=landing_files.get(study_id),
                        status="ready",
                    )
                )
                seen.add(study_id)

    for study_id, filename in landing_files.items():
        if study_id in seen:
            continue
        studies.append(
            Study(
                id=study_id,
                name=study_id,
                source="landing",
                raw_ready=False,
                curated_ready=False,
                landing_file=filename,
                status="missing_raw",
            )
        )
        seen.add(study_id)

    if sync_summary:
        for error in sync_summary.get("errors", []):
            study_id = error.get("study_id")
            if not study_id:
                continue
            studies.append(
                Study(
                    id=study_id,
                    name=study_id,
                    source="landing",
                    raw_ready=False,
                    curated_ready=False,
                    landing_file=error.get("file"),
                    status="error",
                    error=error.get("error"),
                )
            )

    demo_path = root / "data" / "warehouse" / "curated" / "fact_journey_demo.parquet"
    if demo_path.exists() and "demo_001" not in seen:
        studies.append(
            Study(
                id="demo_001",
                name="Demo Study",
                source="demo",
                raw_ready=False,
                curated_ready=True,
            )
        )

    if sync:
        return {"sync": sync_summary or {}, "studies": studies}
    return studies


@router.get("/{study_id}/preview", response_model=StudyPreviewResponse)
def study_preview(study_id: str) -> StudyPreviewResponse:
    raw_root = get_repo_root() / "data" / "warehouse" / "raw"
    study_dir = raw_root / f"study_id={study_id}"
    responses_path = study_dir / "raw_responses.parquet"
    variables_path = study_dir / "raw_variables.parquet"

    if not study_dir.exists() or not responses_path.exists():
        raise HTTPException(status_code=404, detail="Study not found in raw warehouse.")

    conn = get_duckdb_connection()
    load_parquet_as_view(conn, "responses", str(responses_path))
    rows = int(conn.execute("SELECT COUNT(*) FROM responses").fetchone()[0])
    variables = int(conn.execute("SELECT COUNT(DISTINCT var_code) FROM responses").fetchone()[0])

    variables_sample: list[PreviewVariable] = []
    if variables_path.exists():
        load_parquet_as_view(conn, "var_meta", str(variables_path))
        sample_rows = conn.execute(
            "SELECT var_code, question_text FROM var_meta LIMIT 50"
        ).fetchall()
        variables_sample = [
            PreviewVariable(var_code=str(row[0]), question_text=row[1]) for row in sample_rows
        ]
    else:
        sample_rows = conn.execute(
            "SELECT DISTINCT var_code FROM responses LIMIT 50"
        ).fetchall()
        variables_sample = [PreviewVariable(var_code=str(row[0])) for row in sample_rows]

    return StudyPreviewResponse(
        study_id=study_id,
        raw_path=str(study_dir),
        rows=rows,
        variables=variables,
        variables_sample=variables_sample,
    )
