from fastapi import APIRouter, File, HTTPException, Query, UploadFile

from app.data.ingest_from_landing import ensure_raw_from_landing
from app.data.warehouse import blob_exists, delete_blob, delete_prefix, read_csv_blob, write_csv_blob
from app.models.schemas import IngestRunResponse

router = APIRouter()


@router.post("/ingest/run", response_model=IngestRunResponse)
def run_ingest() -> IngestRunResponse:
    summary = ensure_raw_from_landing()
    return IngestRunResponse(
        status="completed",
        processed=summary["processed"],
        skipped=summary["skipped"],
        errors=summary["errors"],
    )


def _sanitize_study_id(value: str) -> str:
    clean = "".join(ch for ch in value.strip().lower() if ch.isalnum() or ch == "_")
    clean = clean.replace(" ", "_")
    while "__" in clean:
        clean = clean.replace("__", "_")
    return clean.strip("_")


@router.post("/ingest/upload")
async def upload_sav_to_landing(
    study_id: str = Query(..., description="Study id slug"),
    file: UploadFile = File(...),
) -> dict:
    normalized_study_id = _sanitize_study_id(study_id)
    if not normalized_study_id:
        raise HTTPException(status_code=400, detail="Invalid study_id.")

    filename = file.filename or ""
    if not filename.lower().endswith(".sav"):
        raise HTTPException(status_code=400, detail="Only .sav files are allowed.")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file.")

    landing_key = f"landing/{normalized_study_id}.sav"
    from app.storage.blob import get_storage

    get_storage().write_bytes(landing_key, content)

    return {
        "ok": True,
        "study_id": normalized_study_id,
        "landing_file": f"{normalized_study_id}.sav",
        "bytes": len(content),
    }


@router.post("/ingest/study/delete")
def delete_study_artifacts(
    study_id: str = Query(..., description="Study id slug"),
) -> dict:
    normalized_study_id = _sanitize_study_id(study_id)
    if not normalized_study_id:
        raise HTTPException(status_code=400, detail="Invalid study_id.")

    removed: list[str] = []
    missing: list[str] = []

    directory_prefixes = [
        f"warehouse/raw/study_id={normalized_study_id}",
        f"warehouse/curated/study_id={normalized_study_id}",
    ]
    file_keys = [
        f"landing/{normalized_study_id}.sav",
        f"warehouse/taxonomy/study_classification/study_id={normalized_study_id}.json",
        f"warehouse/study_config/study_id={normalized_study_id}.json",
        f"warehouse/demographics/study_id={normalized_study_id}.json",
        f"warehouse/mapping/study_rules/study_id={normalized_study_id}.json",
    ]

    for prefix in directory_prefixes:
        deleted = delete_prefix(prefix)
        if deleted:
            removed.extend(deleted)
        else:
            missing.append(prefix)

    for key in file_keys:
        if blob_exists(key):
            delete_blob(key)
            removed.append(key)
        else:
            missing.append(key)

    mapping_key = "warehouse/mapping/question_map_v0.csv"
    df = read_csv_blob(mapping_key)
    if df is not None and "study_id" in df.columns:
        before = len(df)
        filtered = df[df["study_id"].astype(str) != normalized_study_id]
        if len(filtered) != before:
            write_csv_blob(mapping_key, filtered)
            removed.append(mapping_key)

    return {
        "ok": True,
        "study_id": normalized_study_id,
        "removed": removed,
        "missing": missing,
    }
