from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from itertools import combinations
from pathlib import Path

import duckdb
from fastapi import APIRouter, Query

from app.data.warehouse import get_repo_root
from app.routers import analytics

router = APIRouter()

CACHE_TTL_SECONDS = 60
MAX_ITEMS_PER_RESPONDENT = 50

_NETWORK_CACHE: dict[str, tuple[float, dict]] = {}


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _cache_key(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _get_cached(key: str) -> dict | None:
    entry = _NETWORK_CACHE.get(key)
    if not entry:
        return None
    created_at, payload = entry
    if time.time() - created_at > CACHE_TTL_SECONDS:
        _NETWORK_CACHE.pop(key, None)
        return None
    return payload


def _set_cached(key: str, payload: dict) -> None:
    _NETWORK_CACHE[key] = (time.time(), payload)


def _normalize_percentiles(values: list[float]) -> list[float]:
    if not values:
        return []
    sorted_vals = sorted(values)
    if len(sorted_vals) == 1:
        return [0.5]
    p05_idx = max(0, int(round((len(sorted_vals) - 1) * 0.05)))
    p95_idx = max(0, int(round((len(sorted_vals) - 1) * 0.95)))
    p05 = sorted_vals[p05_idx]
    p95 = sorted_vals[p95_idx]
    if p95 <= p05:
        return [0.5 for _ in values]
    return [max(0.0, min(1.0, (value - p05) / (p95 - p05))) for value in values]


def _scale_values(values: list[float], min_size: float, max_size: float) -> list[float]:
    if not values:
        return []
    min_val = min(values)
    max_val = max(values)
    if max_val == min_val:
        return [round((min_size + max_size) / 2, 2) for _ in values]
    return [round(min_size + (value - min_val) / (max_val - min_val) * (max_size - min_size), 2) for value in values]


def _pick_mode(weighted_counts: dict[str, float]) -> tuple[str | None, bool]:
    if not weighted_counts:
        return None, False
    sorted_items = sorted(weighted_counts.items(), key=lambda item: (-item[1], item[0]))
    top_value = sorted_items[0][1]
    if top_value <= 0:
        return None, False
    tied = [item for item in sorted_items if item[1] == top_value]
    return tied[0][0], len(tied) > 1


def _parse_filters(
    study_ids: str | None,
    brands: str | None,
    sector: str | None,
    subsector: str | None,
    category: str | None,
    gender: str | None,
    nse: str | None,
    state: str | None,
    age_min: int | None,
    age_max: int | None,
    date_from: str | None,
    date_to: str | None,
    quarter_from: str | None,
    quarter_to: str | None,
) -> dict:
    payload = {
        "study_ids": study_ids,
        "sector": sector,
        "subsector": subsector,
        "category": category,
        "gender": gender,
        "nse": nse,
        "state": state,
        "age_min": age_min,
        "age_max": age_max,
        "date_from": date_from,
        "date_to": date_to,
        "quarter_from": quarter_from,
        "quarter_to": quarter_to,
    }
    return analytics._parse_filters(payload)


def _parse_csv(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item and item.strip()]


def _filter_studies(filters: dict) -> list[str]:
    root = get_repo_root()
    discovered = analytics._discover_curated_studies(root)
    requested = filters.get("study_ids") or []
    if requested:
        study_ids = [study_id for study_id in requested if study_id in discovered]
    else:
        study_ids = discovered

    if not (filters.get("sector") or filters.get("subsector") or filters.get("category")):
        return study_ids

    filtered: list[str] = []
    for study_id in study_ids:
        classification = analytics._classification_for_study(root, study_id)
        if analytics._study_matches_taxonomy(filters, classification):
            filtered.append(study_id)
    return filtered


def _parquet_columns(path: Path) -> set[str]:
    if not path.exists():
        return set()
    conn = duckdb.connect()
    try:
        cursor = conn.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0")
        return {col[0] for col in cursor.description}
    finally:
        conn.close()


def _value_expr(columns: set[str]) -> str:
    if "value_raw" in columns:
        return "COALESCE(TRY_CAST(value_raw AS INTEGER), TRY_CAST(value AS INTEGER))"
    return "TRY_CAST(value AS INTEGER)"


def _build_primary_links(
    filters: dict,
    study_ids: list[str],
    top_links: int | None,
) -> tuple[list[dict], list[dict], dict]:
    if not study_ids:
        return [], [], {"warning": None, "link_metric_counts": {"recall": 0, "consideration": 0, "purchase": 0}}

    touchpoints_payload = analytics._touchpoints_table_multi_filtered(filters, "all", "recall", "desc")
    rows = touchpoints_payload.get("rows", [])
    if not rows:
        return [], [], {"warning": None, "link_metric_counts": {"recall": 0, "consideration": 0, "purchase": 0}}

    links = []
    selected_brands = set(filters.get("brands") or [])
    consideration_by_study: dict[str, dict[tuple[str, str], float]] = {}
    purchase_by_study: dict[str, dict[tuple[str, str], float]] = {}
    for study_id in study_ids:
        consideration_by_study[study_id] = _build_conditional_metric_by_touchpoint(
            study_id,
            filters,
            ("consideration", "brand_consideration"),
            selected_brands or None,
        )
        purchase_by_study[study_id] = _build_conditional_metric_by_touchpoint(
            study_id,
            filters,
            ("purchase", "brand_purchase"),
            selected_brands or None,
        )
    brand_contexts: dict[str, dict[str, dict[str, float] | list[dict]]] = {}
    for row in rows:
        recall = row.get("recall")
        consideration = row.get("consideration")
        purchase = row.get("purchase")
        if recall is None and consideration is None and purchase is None:
            continue
        brand = row.get("brand")
        touchpoint = row.get("touchpoint")
        study_id = row.get("study_id")
        sector = row.get("sector")
        subsector = row.get("subsector")
        category = row.get("category")
        if not brand or not touchpoint:
            continue
        if selected_brands and brand not in selected_brands:
            continue
        conditional_key = (str(brand), str(touchpoint))
        consideration_conditional = (
            consideration_by_study.get(str(study_id), {}).get(conditional_key)
            if study_id is not None
            else None
        )
        purchase_conditional = (
            purchase_by_study.get(str(study_id), {}).get(conditional_key)
            if study_id is not None
            else None
        )
        context = brand_contexts.setdefault(
            brand,
            {
                "sector": defaultdict(float),
                "subsector": defaultdict(float),
                "category": defaultdict(float),
                "sources": [],
            },
        )
        context_metric = recall if recall is not None else consideration if consideration is not None else purchase
        weight = float(context_metric or 0)
        if sector:
            context["sector"][sector] += weight
        if subsector:
            context["subsector"][subsector] += weight
        if category:
            context["category"][category] += weight
        if study_id and len(context["sources"]) < 5:
            context["sources"].append(
                {
                    "study_id": study_id,
                    "sector": sector,
                    "subsector": subsector,
                    "category": category,
                }
            )
        links.append(
            {
                "source": f"tp:{touchpoint}",
                "target": f"brand:{brand}",
                "weight": None,
                "type": "primary_tp_brand",
                "w_recall_raw": round(float(recall) / 100, 4) if recall is not None else None,
                "w_consideration_raw": round(float(consideration_conditional), 4)
                if consideration_conditional is not None
                else (round(float(consideration) / 100, 4) if consideration is not None else None),
                "w_purchase_raw": round(float(purchase_conditional), 4)
                if purchase_conditional is not None
                else (round(float(purchase) / 100, 4) if purchase is not None else None),
                "n_base": None,
                "colorMeta": {
                    "metric": "recall",
                    "study_id": study_id,
                    "consideration_given_recall": consideration_conditional,
                    "purchase_given_recall": purchase_conditional,
                },
            }
        )

    links.sort(
        key=lambda link: max(
            link.get("w_recall_raw") or 0,
            link.get("w_consideration_raw") or 0,
            link.get("w_purchase_raw") or 0,
        ),
        reverse=True,
    )
    # Top-links truncation is deprecated. Links are already aggregated (one weighted link per pair),
    # so we always keep the full set.

    journey_payload = analytics._journey_table_multi_filtered(filters, "all", "brand_awareness", "desc")
    awareness_map: dict[str, float] = {}
    for row in journey_payload.get("rows", []):
        brand = row.get("brand")
        awareness = row.get("brand_awareness")
        if brand and awareness is not None:
            awareness_map[brand] = float(awareness)

    touchpoint_values: dict[str, list[float]] = defaultdict(list)
    for link in links:
        touchpoint = link["source"].replace("tp:", "", 1)
        recall_raw = link.get("w_recall_raw")
        if recall_raw is not None:
            touchpoint_values[touchpoint].append(recall_raw)

    touchpoint_avg = {
        touchpoint: sum(values) / len(values) if values else 0.1
        for touchpoint, values in touchpoint_values.items()
    }

    brand_values: dict[str, float] = {}
    for link in links:
        brand = link["target"].replace("brand:", "", 1)
        brand_values[brand] = awareness_map.get(brand, 0.6)

    brand_sizes = _scale_values(list(brand_values.values()), 12, 32)
    touchpoint_sizes = _scale_values(list(touchpoint_avg.values()), 10, 28)

    nodes = []
    for (brand, size) in zip(brand_values.keys(), brand_sizes):
        context = brand_contexts.get(brand, {})
        sector_value, sector_mixed = _pick_mode(context.get("sector", {}))
        subsector_value, subsector_mixed = _pick_mode(context.get("subsector", {}))
        category_value, category_mixed = _pick_mode(context.get("category", {}))
        context_key = sector_value or category_value or "Unknown"
        halo_key = subsector_value or "Unknown"
        context_mixed = sector_mixed or subsector_mixed or category_mixed
        nodes.append(
            {
                "id": f"brand:{brand}",
                "type": "brand",
                "label": brand,
                "size": size,
                "group": "brand",
                "sector": sector_value,
                "subsector": subsector_value,
                "category": category_value,
                "context_key": context_key,
                "halo_key": halo_key,
                "context_sources": context.get("sources", []),
                "colorMeta": {
                    "kpi_awareness": awareness_map.get(brand),
                    "base_n_awareness": None,
                    "paletteKey": context_key,
                    "haloKey": halo_key,
                    "context_mixed": context_mixed,
                },
            }
        )

    for (touchpoint, size) in zip(touchpoint_avg.keys(), touchpoint_sizes):
        nodes.append(
            {
                "id": f"tp:{touchpoint}",
                "type": "touchpoint",
                "label": touchpoint,
                "size": size,
                "group": "touchpoint",
                "colorMeta": {
                    "kpi_recall": round(touchpoint_avg.get(touchpoint, 0) * 100, 1),
                    "base_n_recall": None,
                },
            }
        )

    metric_counts = {
        "recall": sum(1 for link in links if link.get("w_recall_raw") is not None),
        "consideration": sum(1 for link in links if link.get("w_consideration_raw") is not None),
        "purchase": sum(1 for link in links if link.get("w_purchase_raw") is not None),
    }

    return nodes, links, {"warning": None, "link_metric_counts": metric_counts}


def _collect_positive_items(
    study_id: str,
    stage: str,
    column: str,
    filters: dict,
    columns: set[str],
    allowed_items: set[str] | None = None,
) -> tuple[dict[str, set[str]], bool]:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if not curated_path.exists():
        return {}, False
    if column not in columns:
        return {}, False

    respondent_cte, respondent_params, eligible = analytics._respondent_filter_cte(study_id, filters)
    if not eligible:
        return {}, False

    value_expr = _value_expr(columns)
    respondent_filter = (
        "AND respondent_id IN (SELECT respondent_id FROM filtered_respondents)"
        if respondent_cte
        else ""
    )
    cte_prefix = f"{respondent_cte}," if respondent_cte else ""

    query = f"""
        WITH {cte_prefix}
        base AS (
            SELECT respondent_id, {column} AS item, {value_expr} AS v_int
            FROM read_parquet('{curated_path}')
            WHERE study_id = ?
              AND LOWER(stage) = '{stage}'
              AND {column} IS NOT NULL
              AND TRIM(CAST({column} AS VARCHAR)) <> ''
              AND {value_expr} IS NOT NULL
              {respondent_filter}
        )
        SELECT respondent_id, item
        FROM base
        WHERE v_int = 1
    """

    conn = duckdb.connect()
    try:
        rows = conn.execute(query, [*respondent_params, study_id]).fetchall()
    finally:
        conn.close()

    items_by_resp: dict[str, set[str]] = defaultdict(set)
    for respondent_id, item in rows:
        if respondent_id is None or item is None:
            continue
        item_value = str(item)
        if allowed_items and item_value not in allowed_items:
            continue
        bucket = items_by_resp[str(respondent_id)]
        if len(bucket) >= MAX_ITEMS_PER_RESPONDENT:
            continue
        bucket.add(item_value)

    return items_by_resp, bool(rows)


def _build_conditional_metric_by_touchpoint(
    study_id: str,
    filters: dict,
    positive_stages: tuple[str, ...],
    selected_brands: set[str] | None = None,
) -> dict[tuple[str, str], float]:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    columns = _parquet_columns(curated_path)
    if not curated_path.exists() or "touchpoint" not in columns or "brand" not in columns or "respondent_id" not in columns:
        return {}

    respondent_cte, respondent_params, eligible = analytics._respondent_filter_cte(study_id, filters)
    if not eligible:
        return {}

    value_expr = _value_expr(columns)
    respondent_filter = (
        "AND respondent_id IN (SELECT respondent_id FROM filtered_respondents)"
        if respondent_cte
        else ""
    )
    cte_prefix = f"{respondent_cte}," if respondent_cte else ""
    stages_sql = ", ".join(f"'{stage}'" for stage in positive_stages)
    selected_brands = selected_brands or set()
    brand_filter_sql = ""
    if selected_brands:
        escaped = ", ".join("'" + brand.replace("'", "''") + "'" for brand in sorted(selected_brands))
        brand_filter_sql = f" AND brand IN ({escaped})"

    query = f"""
        WITH {cte_prefix}
        recall_tp AS (
            SELECT respondent_id, brand, touchpoint
            FROM read_parquet('{curated_path}')
            WHERE study_id = ?
              AND LOWER(stage) IN ('touchpoints', 'awareness')
              AND touchpoint IS NOT NULL
              AND TRIM(CAST(touchpoint AS VARCHAR)) <> ''
              AND brand IS NOT NULL
              AND TRIM(CAST(brand AS VARCHAR)) <> ''
              AND {value_expr} = 1
              {respondent_filter}
              {brand_filter_sql}
            GROUP BY respondent_id, brand, touchpoint
        ),
        metric_brand AS (
            SELECT respondent_id, brand
            FROM read_parquet('{curated_path}')
            WHERE study_id = ?
              AND LOWER(stage) IN ({stages_sql})
              AND brand IS NOT NULL
              AND TRIM(CAST(brand AS VARCHAR)) <> ''
              AND {value_expr} = 1
              {respondent_filter}
              {brand_filter_sql}
            GROUP BY respondent_id, brand
        )
        SELECT
            r.brand,
            r.touchpoint,
            COUNT(*) AS denom_recall,
            SUM(CASE WHEN m.respondent_id IS NOT NULL THEN 1 ELSE 0 END) AS numer_joint
        FROM recall_tp r
        LEFT JOIN metric_brand m
          ON m.respondent_id = r.respondent_id
         AND m.brand = r.brand
        GROUP BY r.brand, r.touchpoint
    """

    conn = duckdb.connect()
    try:
        rows = conn.execute(query, [*respondent_params, study_id, study_id]).fetchall()
    finally:
        conn.close()

    values: dict[tuple[str, str], float] = {}
    for brand, touchpoint, denom, numer in rows:
        if not denom:
            continue
        value = float(numer or 0) / float(denom)
        values[(str(brand), str(touchpoint))] = max(0.0, min(1.0, value))
    return values


def _aggregate_pairs(
    items_by_resp: dict[str, set[str]],
    base_counts: dict[str, int],
    co_counts: dict[tuple[str, str], int],
) -> None:
    for items in items_by_resp.values():
        if not items:
            continue
        for item in items:
            base_counts[item] += 1
        if len(items) < 2:
            continue
        for a, b in combinations(sorted(items), 2):
            co_counts[(a, b)] += 1


def _prune_pairs(
    co_counts: dict[tuple[str, str], int],
    base_counts: dict[str, int],
    top_k: int,
) -> list[dict]:
    adjacency: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for (a, b), count in co_counts.items():
        base_a = base_counts.get(a, 0)
        base_b = base_counts.get(b, 0)
        if not base_a or not base_b:
            continue
        weight = count / min(base_a, base_b)
        adjacency[a].append((b, weight))
        adjacency[b].append((a, weight))

    allowed_pairs: set[tuple[str, str]] = set()
    for node, neighbors in adjacency.items():
        neighbors.sort(key=lambda item: item[1], reverse=True)
        for other, _ in neighbors[:top_k]:
            allowed_pairs.add(tuple(sorted((node, other))))

    results = []
    for (a, b) in allowed_pairs:
        count = co_counts.get((a, b)) or co_counts.get((b, a)) or 0
        base_a = base_counts.get(a, 0)
        base_b = base_counts.get(b, 0)
        if not base_a or not base_b:
            continue
        weight = count / min(base_a, base_b)
        results.append(
            {
                "a": a,
                "b": b,
                "weight": weight,
                "co_count": count,
                "base_a": base_a,
                "base_b": base_b,
            }
        )
    return results


def _build_secondary_brand_links(
    study_ids: list[str],
    filters: dict,
    stage: str,
    link_type: str,
    top_k: int,
) -> list[dict]:
    base_counts: dict[str, int] = defaultdict(int)
    co_counts: dict[tuple[str, str], int] = defaultdict(int)
    any_rows = False
    selected_brands = set(filters.get("brands") or [])

    for study_id in study_ids:
        root = get_repo_root()
        curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
        columns = _parquet_columns(curated_path)
        items_by_resp, has_rows = _collect_positive_items(
            study_id,
            stage,
            "brand",
            filters,
            columns,
            selected_brands or None,
        )
        if has_rows:
            any_rows = True
        _aggregate_pairs(items_by_resp, base_counts, co_counts)

    if not any_rows:
        return []

    pruned = _prune_pairs(co_counts, base_counts, top_k)
    links = []
    for pair in pruned:
        links.append(
            {
                "source": f"brand:{pair['a']}",
                "target": f"brand:{pair['b']}",
                "weight": pair["weight"],
                "type": link_type,
                "w_recall_raw": pair["weight"] if "recall" in link_type else None,
                "w_consideration_raw": pair["weight"] if "consideration" in link_type else None,
                "w_purchase_raw": pair["weight"] if "purchase" in link_type else None,
                "n_base": None,
                "colorMeta": {
                    "co_count": pair["co_count"],
                    "base_a": pair["base_a"],
                    "base_b": pair["base_b"],
                    "metric": link_type,
                },
            }
        )
    return links


def _build_secondary_touchpoint_links(
    study_ids: list[str],
    filters: dict,
    top_k: int,
) -> list[dict]:
    base_counts: dict[str, int] = defaultdict(int)
    co_counts: dict[tuple[str, str], int] = defaultdict(int)
    any_rows = False

    for study_id in study_ids:
        root = get_repo_root()
        curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
        columns = _parquet_columns(curated_path)
        if "touchpoint" not in columns:
            continue
        items_by_resp, has_rows = _collect_positive_items(
            study_id,
            analytics.TOUCHPOINT_STAGE_KEY,
            "touchpoint",
            filters,
            columns,
        )
        if has_rows:
            any_rows = True
        _aggregate_pairs(items_by_resp, base_counts, co_counts)

    if not any_rows:
        return []

    pruned = _prune_pairs(co_counts, base_counts, top_k)
    links = []
    for pair in pruned:
        links.append(
            {
                "source": f"tp:{pair['a']}",
                "target": f"tp:{pair['b']}",
                "weight": pair["weight"],
                "type": "secondary_touchpoint_touchpoint_recall",
                "w_recall_raw": pair["weight"],
                "w_consideration_raw": None,
                "w_purchase_raw": None,
                "n_base": None,
                "colorMeta": {
                    "co_count": pair["co_count"],
                    "base_a": pair["base_a"],
                    "base_b": pair["base_b"],
                    "metric": "recall",
                },
            }
        )
    return links


def _normalize_links(links: list[dict]) -> None:
    recall_values = [link["w_recall_raw"] for link in links if link.get("w_recall_raw") is not None]
    consideration_values = [
        link["w_consideration_raw"] for link in links if link.get("w_consideration_raw") is not None
    ]
    purchase_values = [link["w_purchase_raw"] for link in links if link.get("w_purchase_raw") is not None]

    recall_norm = _normalize_percentiles(recall_values)
    consideration_norm = _normalize_percentiles(consideration_values)
    purchase_norm = _normalize_percentiles(purchase_values)

    recall_iter = iter(recall_norm)
    consideration_iter = iter(consideration_norm)
    purchase_iter = iter(purchase_norm)

    for link in links:
        if link.get("w_recall_raw") is not None:
            link["w_recall_norm"] = next(recall_iter)
        if link.get("w_consideration_raw") is not None:
            link["w_consideration_norm"] = next(consideration_iter)
        if link.get("w_purchase_raw") is not None:
            link["w_purchase_norm"] = next(purchase_iter)


def _apply_metric_weights(links: list[dict], metric_mode: str) -> None:
    for link in links:
        if metric_mode == "purchase":
            link["weight"] = link.get("w_purchase_norm") or 0
        elif metric_mode == "consideration":
            link["weight"] = link.get("w_consideration_norm") or 0
        elif metric_mode == "both":
            link["weight"] = max(
                link.get("w_consideration_norm") or 0,
                link.get("w_purchase_norm") or 0,
                link.get("w_recall_norm") or 0,
            )
        else:
            link["weight"] = link.get("w_recall_norm") or 0


def _synthetic_graph(metric: str) -> dict:
    nodes = [
        {"id": "brand:Walmart", "type": "brand", "label": "Walmart", "size": 24, "group": "brand"},
        {"id": "brand:Oxxo", "type": "brand", "label": "Oxxo", "size": 22, "group": "brand"},
        {"id": "tp:TV", "type": "touchpoint", "label": "TV", "size": 20, "group": "touchpoint"},
        {"id": "tp:Facebook", "type": "touchpoint", "label": "Facebook", "size": 18, "group": "touchpoint"},
    ]
    links = [
        {
            "source": "tp:TV",
            "target": "brand:Walmart",
            "weight": 0.7,
            "type": "primary_tp_brand",
            "w_recall_raw": 0.7,
            "w_recall_norm": 0.7,
            "w_consideration_raw": None,
            "w_purchase_raw": None,
            "n_base": None,
        },
        {
            "source": "tp:Facebook",
            "target": "brand:Oxxo",
            "weight": 0.5,
            "type": "primary_tp_brand",
            "w_recall_raw": 0.5,
            "w_recall_norm": 0.5,
            "w_consideration_raw": None,
            "w_purchase_raw": None,
            "n_base": None,
        },
    ]
    return {
        "ok": True,
        "metric": metric,
        "filters": {},
        "nodes": nodes,
        "links": links,
        "meta": {
            "cache_hit": False,
            "generated_at": _now_iso(),
            "synthetic": True,
            "note": "Synthetic graph until curated data is available.",
        },
    }


def _build_tp_brand_graph(
    metric_mode: str,
    filters: dict,
    top_links: int | None,
    secondary_links: str,
    secondary_top_k: int,
    tp_secondary_top_k: int,
) -> tuple[dict, str | None]:
    study_ids = _filter_studies(filters)
    if not study_ids:
        return {
            "nodes": [],
            "links": [],
            "meta": {"empty_reason": "No curated studies available for the selected filters."},
        }, None

    nodes, links, meta = _build_primary_links(filters, study_ids, top_links)
    warning = None

    has_primary = bool(links)
    if not has_primary:
        return {
            "nodes": [],
            "links": [],
            "meta": {"empty_reason": "No touchpoint data available for the selected filters."},
        }, None

    consideration_available = any(link.get("w_consideration_raw") is not None for link in links)
    purchase_available = any(link.get("w_purchase_raw") is not None for link in links)
    if metric_mode == "consideration" and not consideration_available:
        warning = "Consideration links are unavailable for this scope (computed as P(consideration|touchpoint recall))."
    elif metric_mode == "purchase" and not purchase_available:
        warning = "Purchase links are unavailable for this scope (computed as P(purchase|touchpoint recall))."

    _normalize_links(links)
    _apply_metric_weights(links, metric_mode)

    secondary_links = secondary_links.lower()
    secondary: list[dict] = []
    if secondary_links in {"brands", "both"}:
        if metric_mode == "purchase":
            brand_links = _build_secondary_brand_links(
                study_ids,
                filters,
                "purchase",
                "secondary_brand_brand_purchase",
                secondary_top_k,
            )
            if not brand_links:
                brand_links = _build_secondary_brand_links(
                    study_ids,
                    filters,
                    "awareness",
                    "secondary_brand_brand_recall",
                    secondary_top_k,
                )
                warning = warning or "Purchase secondary links unavailable. Showing Recall instead."
            secondary.extend(brand_links)
        elif metric_mode == "consideration":
            brand_links = _build_secondary_brand_links(
                study_ids,
                filters,
                "consideration",
                "secondary_brand_brand_consideration",
                secondary_top_k,
            )
            if not brand_links:
                brand_links = _build_secondary_brand_links(
                    study_ids,
                    filters,
                    "awareness",
                    "secondary_brand_brand_recall",
                    secondary_top_k,
                )
                warning = warning or "Consideration secondary links unavailable. Showing Recall instead."
            secondary.extend(brand_links)
        elif metric_mode == "both":
            brand_consider = _build_secondary_brand_links(
                study_ids,
                filters,
                "consideration",
                "secondary_brand_brand_consideration",
                secondary_top_k,
            )
            if not brand_consider:
                brand_consider = _build_secondary_brand_links(
                    study_ids,
                    filters,
                    "awareness",
                    "secondary_brand_brand_recall",
                    secondary_top_k,
                )
                warning = warning or "Consideration secondary links unavailable. Showing Recall instead."
            brand_purchase = _build_secondary_brand_links(
                study_ids,
                filters,
                "purchase",
                "secondary_brand_brand_purchase",
                secondary_top_k,
            )
            if not brand_purchase and not warning:
                warning = "Purchase secondary links unavailable."
            secondary.extend(brand_consider)
            secondary.extend(brand_purchase)
        else:
            secondary.extend(
                _build_secondary_brand_links(
                    study_ids,
                    filters,
                    "awareness",
                    "secondary_brand_brand_recall",
                    secondary_top_k,
                )
            )

    if secondary_links in {"touchpoints", "both"}:
        secondary.extend(
            _build_secondary_touchpoint_links(
                study_ids,
                filters,
                tp_secondary_top_k,
            )
        )

    if secondary:
        _normalize_links(secondary)
        _apply_metric_weights(secondary, metric_mode)

    links = links + secondary

    selected_brands = set(filters.get("brands") or [])
    if selected_brands:
        allowed_brand_node_ids = {f"brand:{brand}" for brand in selected_brands}

        filtered_links = []
        for link in links:
            source = str(link.get("source", ""))
            target = str(link.get("target", ""))
            if source.startswith("brand:") and source not in allowed_brand_node_ids:
                continue
            if target.startswith("brand:") and target not in allowed_brand_node_ids:
                continue
            filtered_links.append(link)
        links = filtered_links

    node_ids = {node["id"] for node in nodes}
    for link in links:
        for node_id in (link["source"], link["target"]):
            if node_id in node_ids:
                continue
            label = node_id.split(":", 1)[1]
            group = "brand" if node_id.startswith("brand:") else "touchpoint"
            nodes.append(
                {
                    "id": node_id,
                    "type": group,
                    "label": label,
                    "size": 14,
                    "group": group,
                    "colorMeta": {},
                }
            )
            node_ids.add(node_id)

    referenced_ids = {node_id for link in links for node_id in (str(link["source"]), str(link["target"]))}
    nodes = [node for node in nodes if str(node.get("id")) in referenced_ids]

    node_counts = {
        "brand": sum(1 for node in nodes if node.get("group") == "brand"),
        "touchpoint": sum(1 for node in nodes if node.get("group") == "touchpoint"),
    }

    meta.update(
        {
            "node_counts": node_counts,
            "link_count": len(links),
        }
    )

    return {"nodes": nodes, "links": links, "meta": meta}, warning


@router.get("/network")
def demand_network(
    metric: str = Query("recall", description="recall|consideration|purchase|both"),
    metric_mode: str | None = Query(None, description="recall|consideration|purchase|both"),
    study_ids: str | None = Query(None, description="Comma-separated study ids"),
    brands: str | None = Query(None, description="Comma-separated brands"),
    sector: str | None = Query(None),
    subsector: str | None = Query(None),
    category: str | None = Query(None),
    gender: str | None = Query(None),
    nse: str | None = Query(None),
    state: str | None = Query(None),
    age_min: int | None = Query(None),
    age_max: int | None = Query(None),
    date_from: str | None = Query(None),
    date_to: str | None = Query(None),
    quarter_from: str | None = Query(None),
    quarter_to: str | None = Query(None),
    top_links: int | None = Query(None, description="Deprecated legacy link cap; omit for full links"),
    secondary_links: str = Query("off", description="off|brands|touchpoints|both"),
    secondary_top_k_per_node: int = Query(3, description="3-5"),
    tp_secondary_top_k_per_node: int = Query(3, description="3-5"),
) -> dict:
    metric_mode = metric_mode or metric or "recall"
    metric_mode = metric_mode.lower()
    if metric_mode not in {"recall", "consideration", "purchase", "both"}:
        metric_mode = "recall"

    # Legacy query param retained for backward compatibility; ignored by the pipeline.
    secondary_top_k_per_node = min(max(secondary_top_k_per_node, 3), 5)
    tp_secondary_top_k_per_node = min(max(tp_secondary_top_k_per_node, 3), 5)

    filters = _parse_filters(
        study_ids,
        brands,
        sector,
        subsector,
        category,
        gender,
        nse,
        state,
        age_min,
        age_max,
        date_from,
        date_to,
        quarter_from,
        quarter_to,
    )
    filters["brands"] = _parse_csv(brands)

    cache_payload = {
        "calc_v": 2,
        "metric_mode": metric_mode,
        "filters": filters,
        "secondary_links": secondary_links,
        "secondary_top_k": secondary_top_k_per_node,
        "tp_secondary_top_k": tp_secondary_top_k_per_node,
    }
    key = _cache_key(cache_payload)
    cached = _get_cached(key)
    if cached:
        cached["meta"]["cache_hit"] = True
        return cached

    has_curated = bool(analytics._discover_curated_studies(get_repo_root()))
    if not has_curated:
        payload = _synthetic_graph(metric_mode)
        _set_cached(key, payload)
        return payload

    graph, warning = _build_tp_brand_graph(
        metric_mode,
        filters,
        top_links,
        secondary_links,
        secondary_top_k_per_node,
        tp_secondary_top_k_per_node,
    )

    payload = {
        "ok": True,
        "metric": metric_mode,
        "filters": filters,
        "nodes": graph["nodes"],
        "links": graph["links"],
        "meta": {
            "cache_hit": False,
            "generated_at": _now_iso(),
            "synthetic": False,
            "warning": warning,
            **graph["meta"],
        },
    }

    _set_cached(key, payload)
    return payload
