from __future__ import annotations

import json
import unicodedata
from pathlib import Path

from app.data.warehouse import get_repo_root


def _taxonomy_dir(root: Path | None = None) -> Path:
    repo_root = root or get_repo_root()
    return repo_root / "data" / "warehouse" / "taxonomy"


def standard_taxonomy_path(root: Path | None = None) -> Path:
    return _taxonomy_dir(root) / "sector_subsector_category_v1.json"


def market_lens_rules_path(root: Path | None = None) -> Path:
    return _taxonomy_dir(root) / "market_lens_rules_v1.json"


def _normalize(value: str | None) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(text.strip().lower().split())


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return json.loads(path.read_text(encoding="utf-8-sig"))


def load_standard_taxonomy_items(root: Path | None = None) -> list[dict[str, str]]:
    payload = _read_json(standard_taxonomy_path(root))
    items = payload.get("items", []) if isinstance(payload, dict) else []
    normalized: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        sector = str(item.get("sector") or "").strip()
        subsector = str(item.get("subsector") or "").strip()
        category = str(item.get("category") or "").strip()
        if sector and subsector and category:
            normalized.append(
                {
                    "sector": sector,
                    "subsector": subsector,
                    "category": category,
                }
            )
    return normalized


def load_market_lens_rules(root: Path | None = None) -> dict:
    payload = _read_json(market_lens_rules_path(root))
    if not isinstance(payload, dict):
        return {"category_rules": [], "subsector_rules": [], "sector_rules": []}
    return {
        "category_rules": payload.get("category_rules", []) or [],
        "subsector_rules": payload.get("subsector_rules", []) or [],
        "sector_rules": payload.get("sector_rules", []) or [],
    }


def _market_value(value: str | None, fallback: str) -> str:
    cleaned = str(value or "").strip()
    return cleaned or fallback


def derive_market_lens(
    sector: str | None,
    subsector: str | None,
    category: str | None,
    root: Path | None = None,
) -> dict[str, str]:
    rules = load_market_lens_rules(root)
    normalized_sector = _normalize(sector)
    normalized_subsector = _normalize(subsector)
    normalized_category = _normalize(category)

    for rule in rules.get("category_rules", []):
        if not isinstance(rule, dict):
            continue
        if _normalize(rule.get("category")) != normalized_category:
            continue
        return {
            "market_sector": _market_value(rule.get("market_sector"), "General Market"),
            "market_subsector": _market_value(rule.get("market_subsector"), "General Segment"),
            "market_category": _market_value(rule.get("market_category"), "General Category"),
            "market_source": "rule",
        }

    for rule in rules.get("subsector_rules", []):
        if not isinstance(rule, dict):
            continue
        if _normalize(rule.get("subsector")) != normalized_subsector:
            continue
        if rule.get("sector") and _normalize(rule.get("sector")) != normalized_sector:
            continue
        return {
            "market_sector": _market_value(rule.get("market_sector"), "General Market"),
            "market_subsector": _market_value(rule.get("market_subsector"), "General Segment"),
            "market_category": _market_value(rule.get("market_category"), "General Category"),
            "market_source": "rule",
        }

    for rule in rules.get("sector_rules", []):
        if not isinstance(rule, dict):
            continue
        if _normalize(rule.get("sector")) != normalized_sector:
            continue
        return {
            "market_sector": _market_value(rule.get("market_sector"), "General Market"),
            "market_subsector": _market_value(rule.get("market_subsector"), "General Segment"),
            "market_category": _market_value(rule.get("market_category"), "General Category"),
            "market_source": "rule",
        }

    fallback_sector = str(sector or "").strip() or "Unassigned"
    fallback_subsector = str(subsector or "").strip() or fallback_sector
    fallback_category = str(category or "").strip() or fallback_subsector
    return {
        "market_sector": fallback_sector,
        "market_subsector": fallback_subsector,
        "market_category": fallback_category,
        "market_source": "rule",
    }


def resolve_classification(data: dict | None, root: Path | None = None) -> dict[str, str | None]:
    payload = data if isinstance(data, dict) else {}
    sector = str(payload.get("sector") or "").strip() or None
    subsector = str(payload.get("subsector") or "").strip() or None
    category = str(payload.get("category") or "").strip() or None
    market_sector = str(payload.get("market_sector") or "").strip() or None
    market_subsector = str(payload.get("market_subsector") or "").strip() or None
    market_category = str(payload.get("market_category") or "").strip() or None
    market_source = str(payload.get("market_source") or "").strip().lower() or None

    if market_sector and market_subsector and market_category:
        return {
            "sector": sector,
            "subsector": subsector,
            "category": category,
            "market_sector": market_sector,
            "market_subsector": market_subsector,
            "market_category": market_category,
            "market_source": "manual" if market_source == "manual" else "rule",
        }

    derived = derive_market_lens(sector, subsector, category, root=root)
    return {
        "sector": sector,
        "subsector": subsector,
        "category": category,
        "market_sector": derived["market_sector"],
        "market_subsector": derived["market_subsector"],
        "market_category": derived["market_category"],
        "market_source": "rule",
    }


def market_taxonomy_items_from_standard(root: Path | None = None) -> list[dict[str, str]]:
    rows = load_standard_taxonomy_items(root)
    dedup: set[tuple[str, str, str]] = set()
    items: list[dict[str, str]] = []
    for row in rows:
        derived = derive_market_lens(row["sector"], row["subsector"], row["category"], root=root)
        key = (
            derived["market_sector"],
            derived["market_subsector"],
            derived["market_category"],
        )
        if key in dedup:
            continue
        dedup.add(key)
        items.append(
            {
                "sector": derived["market_sector"],
                "subsector": derived["market_subsector"],
                "category": derived["market_category"],
            }
        )
    return sorted(items, key=lambda item: (item["sector"], item["subsector"], item["category"]))
