from pathlib import Path

import json
from fastapi import APIRouter, HTTPException, Query, Request

from app.data.market_lens import (
    market_taxonomy_items_from_standard,
    resolve_classification,
)
from app.data.warehouse import get_repo_root

router = APIRouter()


def _taxonomy_path() -> Path:
    return (
        get_repo_root()
        / "data"
        / "warehouse"
        / "taxonomy"
        / "sector_subsector_category_v1.json"
    )


def _study_classification_path(study_id: str) -> Path:
    return (
        get_repo_root()
        / "data"
        / "warehouse"
        / "taxonomy"
        / "study_classification"
        / f"study_id={study_id}.json"
    )


def _load_taxonomy() -> dict:
    path = _taxonomy_path()
    if not path.exists():
        raise HTTPException(status_code=404, detail="Taxonomy file not found.")
    return json.loads(path.read_text(encoding="utf-8"))


@router.get("/taxonomy")
def get_taxonomy() -> dict:
    return _load_taxonomy()


@router.get("/taxonomy/market")
def get_market_taxonomy() -> dict:
    items = market_taxonomy_items_from_standard(get_repo_root())
    sectors = sorted({item.get("sector") for item in items if item.get("sector")})
    subsectors = sorted({item.get("subsector") for item in items if item.get("subsector")})
    categories = sorted({item.get("category") for item in items if item.get("category")})
    return {"items": items, "sectors": sectors, "subsectors": subsectors, "categories": categories}


@router.get("/taxonomy/study")
def get_study_taxonomy(study_id: str = Query(..., description="Study id")) -> dict:
    path = _study_classification_path(study_id)
    if not path.exists():
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
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    resolved = resolve_classification(payload, root=get_repo_root())
    return {"study_id": study_id, **resolved}


@router.post("/taxonomy/study")
async def save_study_taxonomy(
    study_id: str = Query(..., description="Study id"),
    request: Request = ...,
) -> dict:
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Payload must be a JSON object.")

    resolved = resolve_classification(payload, root=get_repo_root())
    sector = resolved.get("sector")
    subsector = resolved.get("subsector")
    category = resolved.get("category")

    path = _study_classification_path(study_id)
    path.parent.mkdir(parents=True, exist_ok=True)

    if not sector or not subsector or not category:
        if path.exists():
            path.unlink()
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
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return data
