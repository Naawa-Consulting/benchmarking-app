import re
from concurrent.futures import ThreadPoolExecutor

import duckdb
from fastapi import APIRouter, HTTPException, Query

from app.data.market_lens import resolve_classification
from app.data.ingest_from_landing import ensure_raw_from_landing
from app.data.warehouse import (
    blob_exists,
    list_blob_names,
    list_study_ids,
    load_parquet_as_view,
    read_json_blob,
)
from app.models.schemas import PreviewVariable, Study, StudyPreviewResponse

router = APIRouter()


def _slugify_landing(stem: str) -> str:
    value = stem.strip().lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-z0-9_]", "", value)
    value = re.sub(r"_+", "_", value)
    return value.strip("_") or "study"


def _classification_for_study(study_id: str) -> dict[str, str | None]:
    empty = {
        "sector": None,
        "subsector": None,
        "category": None,
        "market_sector": None,
        "market_subsector": None,
        "market_category": None,
        "market_source": None,
    }
    key = f"warehouse/taxonomy/study_classification/study_id={study_id}.json"
    try:
        payload = read_json_blob(key, default=None)
    except Exception:
        return empty
    if payload is None:
        return empty
    try:
        return resolve_classification(payload)
    except Exception:
        return empty


@router.get("/")
def list_studies(sync: bool = Query(False, description="Sync from landing")):
    sync_summary = None
    if sync:
        sync_summary = ensure_raw_from_landing()

    landing_files = {
        _slugify_landing(name[: -len(".sav")]): name
        for name in list_blob_names("landing")
        if name.lower().endswith(".sav")
    }

    studies: list[Study] = []
    seen: set[str] = set()

    study_ids = list_study_ids("warehouse/raw", "raw_responses.parquet")

    def _lookup(study_id: str) -> tuple[str, dict[str, str | None], bool]:
        curated_key = f"warehouse/curated/study_id={study_id}/fact_journey.parquet"
        return study_id, _classification_for_study(study_id), blob_exists(curated_key)

    # Each lookup is two Storage round-trips per study — running them concurrently
    # cuts wall-clock time roughly by the pool size instead of paying per-study
    # latency sequentially across dozens of studies.
    lookups: dict[str, tuple[dict[str, str | None], bool]] = {}
    if study_ids:
        with ThreadPoolExecutor(max_workers=min(4, len(study_ids))) as executor:
            for study_id, classification, curated_ready in executor.map(_lookup, study_ids):
                lookups[study_id] = (classification, curated_ready)

    for study_id in study_ids:
        classification, curated_ready = lookups[study_id]
        if study_id and study_id not in seen:
            studies.append(
                Study(
                    id=study_id,
                    name=study_id,
                    source="raw",
                    raw_ready=True,
                    curated_ready=curated_ready,
                    landing_file=landing_files.get(study_id),
                    status="ready",
                    sector=classification["sector"],
                    subsector=classification["subsector"],
                    category=classification["category"],
                    market_sector=classification["market_sector"],
                    market_subsector=classification["market_subsector"],
                    market_category=classification["market_category"],
                    market_source=classification["market_source"],
                )
            )
            seen.add(study_id)

    for study_id, filename in landing_files.items():
        if study_id in seen:
            continue
        classification = _classification_for_study(study_id)
        studies.append(
            Study(
                id=study_id,
                name=study_id,
                source="landing",
                raw_ready=False,
                curated_ready=False,
                landing_file=filename,
                status="missing_raw",
                sector=classification["sector"],
                subsector=classification["subsector"],
                category=classification["category"],
                market_sector=classification["market_sector"],
                market_subsector=classification["market_subsector"],
                market_category=classification["market_category"],
                market_source=classification["market_source"],
            )
        )
        seen.add(study_id)

    if sync_summary:
        for error in sync_summary.get("errors", []):
            study_id = error.get("study_id")
            if not study_id:
                continue
            classification = _classification_for_study(study_id)
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
                    sector=classification["sector"],
                    subsector=classification["subsector"],
                    category=classification["category"],
                    market_sector=classification["market_sector"],
                    market_subsector=classification["market_subsector"],
                    market_category=classification["market_category"],
                    market_source=classification["market_source"],
                )
            )

    if sync:
        return {"sync": sync_summary or {}, "studies": studies}
    return studies


@router.get("/{study_id}/preview", response_model=StudyPreviewResponse)
def study_preview(study_id: str) -> StudyPreviewResponse:
    study_dir = f"warehouse/raw/study_id={study_id}"
    responses_key = f"{study_dir}/raw_responses.parquet"
    variables_key = f"{study_dir}/raw_variables.parquet"

    if not blob_exists(responses_key):
        raise HTTPException(status_code=404, detail="Study not found in raw warehouse.")

    conn = duckdb.connect()
    load_parquet_as_view(conn, "responses", responses_key)
    rows = int(conn.execute("SELECT COUNT(*) FROM responses").fetchone()[0])
    variables = int(conn.execute("SELECT COUNT(DISTINCT var_code) FROM responses").fetchone()[0])

    variables_sample: list[PreviewVariable] = []
    if blob_exists(variables_key):
        load_parquet_as_view(conn, "var_meta", variables_key)
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
        raw_path=study_dir,
        rows=rows,
        variables=variables,
        variables_sample=variables_sample,
    )
