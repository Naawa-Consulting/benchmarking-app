from fastapi import APIRouter

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
