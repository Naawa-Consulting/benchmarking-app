from pathlib import Path
import shutil

from fastapi import APIRouter, File, HTTPException, Query, UploadFile

from app.data.ingest_from_landing import ensure_raw_from_landing
from app.data.warehouse import get_repo_root
from app.models.schemas import IngestRunResponse

router = APIRouter()


@router.post("/ingest/run", response_model=IngestRunResponse)
def run_ingest() -> IngestRunResponse:
    base_data_dir = get_repo_root() / "data"
    summary = ensure_raw_from_landing(base_data_dir)
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

    base_data_dir = get_repo_root() / "data"
    landing_dir = base_data_dir / "landing"
    landing_dir.mkdir(parents=True, exist_ok=True)

    target_path = landing_dir / f"{normalized_study_id}.sav"
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file.")

    Path(target_path).write_bytes(content)

    return {
        "ok": True,
        "study_id": normalized_study_id,
        "landing_file": target_path.name,
        "bytes": len(content),
    }


@router.post("/ingest/study/delete")
def delete_study_artifacts(
    study_id: str = Query(..., description="Study id slug"),
) -> dict:
    normalized_study_id = _sanitize_study_id(study_id)
    if not normalized_study_id:
        raise HTTPException(status_code=400, detail="Invalid study_id.")

    root = get_repo_root()
    data_root = root / "data"
    removed: list[str] = []
    missing: list[str] = []

    candidate_paths = [
        data_root / "landing" / f"{normalized_study_id}.sav",
        data_root / "warehouse" / "raw" / f"study_id={normalized_study_id}",
        data_root / "warehouse" / "curated" / f"study_id={normalized_study_id}",
        data_root / "warehouse" / "taxonomy" / "study_classification" / f"study_id={normalized_study_id}.json",
        data_root / "warehouse" / "study_config" / f"study_id={normalized_study_id}.json",
        data_root / "warehouse" / "demographics" / f"study_id={normalized_study_id}.json",
        data_root / "warehouse" / "mapping" / "study_rules" / f"study_id={normalized_study_id}.json",
    ]

    for path in candidate_paths:
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
            removed.append(str(path))
        elif path.exists():
            path.unlink(missing_ok=True)
            removed.append(str(path))
        else:
            missing.append(str(path))

    mapping_csv = data_root / "warehouse" / "mapping" / "question_map_v0.csv"
    if mapping_csv.exists():
        import pandas as pd

        df = pd.read_csv(mapping_csv)
        if "study_id" in df.columns:
            before = len(df)
            filtered = df[df["study_id"].astype(str) != normalized_study_id]
            if len(filtered) != before:
                filtered.to_csv(mapping_csv, index=False)
                removed.append(str(mapping_csv))

    return {
        "ok": True,
        "study_id": normalized_study_id,
        "removed": removed,
        "missing": missing,
    }
