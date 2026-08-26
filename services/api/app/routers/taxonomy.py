from fastapi import APIRouter, HTTPException, Query, Request

from app.data.market_lens import (
    market_taxonomy_items_from_standard,
    resolve_classification,
)
from app.data.warehouse import blob_exists, delete_blob, read_json_blob, write_json_blob

router = APIRouter()

_TAXONOMY_KEY = "warehouse/taxonomy/sector_subsector_category_v1.json"


def _study_classification_key(study_id: str) -> str:
    return f"warehouse/taxonomy/study_classification/study_id={study_id}.json"


def _load_taxonomy() -> dict:
    payload = read_json_blob(_TAXONOMY_KEY, default=None)
    if payload is None:
        raise HTTPException(status_code=404, detail="Taxonomy file not found.")
    return payload


@router.get("/taxonomy")
def get_taxonomy() -> dict:
    return _load_taxonomy()


@router.get("/taxonomy/market")
def get_market_taxonomy() -> dict:
    items = market_taxonomy_items_from_standard()
    sectors = sorted({item.get("sector") for item in items if item.get("sector")})
    subsectors = sorted({item.get("subsector") for item in items if item.get("subsector")})
    categories = sorted({item.get("category") for item in items if item.get("category")})
    return {"items": items, "sectors": sectors, "subsectors": subsectors, "categories": categories}


@router.get("/taxonomy/study")
def get_study_taxonomy(study_id: str = Query(..., description="Study id")) -> dict:
    key = _study_classification_key(study_id)
    payload = read_json_blob(key, default=None)
    if payload is None:
        return {
            "study_id": study_id,
            "sector": None,
            "subsector": None,
            "category": None,
            "market_sector": None,
            "market_subsector": None,
            "market_category": None,
            "market_source": None,
        }
    resolved = resolve_classification(payload)
    return {"study_id": study_id, **resolved}


@router.post("/taxonomy/study")
async def save_study_taxonomy(
    study_id: str = Query(..., description="Study id"),
    request: Request = ...,
) -> dict:
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object.")

    resolved = resolve_classification(payload)
    sector = resolved.get("sector")
    subsector = resolved.get("subsector")
    category = resolved.get("category")

    key = _study_classification_key(study_id)

    if not sector or not subsector or not category:
        if blob_exists(key):
            delete_blob(key)
        return {
            "study_id": study_id,
            "sector": None,
            "subsector": None,
            "category": None,
            "market_sector": None,
            "market_subsector": None,
            "market_category": None,
            "market_source": None,
        }

    taxonomy = _load_taxonomy()
    items = taxonomy.get("items", [])
    valid = any(
        item.get("sector") == sector
        and item.get("subsector") == subsector
        and item.get("category") == category
        for item in items
    )
    if not valid:
        raise HTTPException(status_code=400, detail="Invalid sector/subsector/category.")

    data = {
        "study_id": study_id,
        "sector": sector,
        "subsector": subsector,
        "category": category,
        "market_sector": resolved.get("market_sector"),
        "market_subsector": resolved.get("market_subsector"),
        "market_category": resolved.get("market_category"),
        "market_source": resolved.get("market_source") or "rule",
    }
    write_json_blob(key, data)
    return data
