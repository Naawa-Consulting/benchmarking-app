import json
import re
import time
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request

from app.data.demographics import load_demographics_config, normalize_demographics_config
from app.data.market_lens import resolve_classification
from app.data.warehouse import get_duckdb_connection, get_repo_root, load_parquet_as_view
from app.models.schemas import JourneyPoint, JourneyResponse

router = APIRouter()

JOURNEY_TABLE_MULTI_CACHE_TTL_SECONDS = 45
_JOURNEY_TABLE_MULTI_CACHE: dict[str, tuple[float, dict]] = {}

TABLE_STAGE_MAP = {
    "awareness": "brand_awareness",
    "ad_awareness": "ad_awareness",
    "consideration": "brand_consideration",
    "purchase": "brand_purchase",
    "satisfaction": "brand_satisfaction",
    "recommendation": "brand_recommendation",
}

SORTABLE_METRICS = {
    "brand_awareness",
    "ad_awareness",
    "brand_consideration",
    "brand_purchase",
    "brand_satisfaction",
    "brand_recommendation",
}

TOUCHPOINT_STAGE_KEY = "touchpoints"
TOUCHPOINT_STAGE_METRICS = {
    "touchpoints": "recall",
    "awareness": "recall",
    "consideration": "consideration",
    "brand_consideration": "consideration",
    "purchase": "purchase",
    "brand_purchase": "purchase",
}

TRACKING_SERIES_CACHE_TTL_SECONDS = 30
_TRACKING_SERIES_CACHE: dict[str, tuple[float, dict]] = {}

CORE_AWARENESS_BOUNDED_METRICS = (
    "brand_consideration",
    "brand_purchase",
    "brand_satisfaction",
    "brand_recommendation",
)
MIN_EXPERIENCE_BASE_N = 10
CONSIDERATION_IMPUTE_VERSION = "v1.0"
CONSIDERATION_IMPUTE_MIN_N = 15
CONSIDERATION_IMPUTE_CACHE_TTL_SECONDS = 120
CONSIDERATION_IMPUTE_WARN_THRESHOLD = 0.40
CONSIDERATION_INVALID_YEAR = 20
CONSIDERATION_TRAIN_MIN_YEAR = 1900
CONSIDERATION_EXCLUDED_MARKET_CATEGORIES = {"specialty stores", "speciality stores"}
_CONSIDERATION_IMPUTE_CACHE: dict[str, tuple[float, dict]] = {}
SATISFACTION_IMPUTE_VERSION = "v1.0"
SATISFACTION_IMPUTE_MIN_N = 15
SATISFACTION_IMPUTE_CACHE_TTL_SECONDS = 120
SATISFACTION_IMPUTE_WARN_THRESHOLD = 0.40
_SATISFACTION_IMPUTE_CACHE: dict[str, tuple[float, dict]] = {}
CSAT_IMPUTE_VERSION = "v1.0"
CSAT_IMPUTE_MIN_N = 15
CSAT_IMPUTE_CACHE_TTL_SECONDS = 120
CSAT_IMPUTE_WARN_THRESHOLD = 0.40
_CSAT_IMPUTE_CACHE: dict[str, tuple[float, dict]] = {}


def _discover_curated_studies(root: Path) -> list[str]:
    curated_root = root / "data" / "warehouse" / "curated"
    discovered = []
    if curated_root.exists():
        for path in curated_root.glob("study_id=*"):
            if (path / "fact_journey.parquet").exists():
                discovered.append(path.name.replace("study_id=", "", 1))
    return discovered


def _study_year_from_id(study_id: str) -> int | None:
    match = re.search(r"(19|20)\d{2}", study_id or "")
    if not match:
        return None
    try:
        return int(match.group(0))
    except ValueError:
        return None


def _is_training_year_valid(year: int | None) -> bool:
    if year is None:
        return False
    if year == CONSIDERATION_INVALID_YEAR:
        return False
    max_allowed = datetime.utcnow().year + 1
    return CONSIDERATION_TRAIN_MIN_YEAR <= year <= max_allowed


def _as_non_empty_text(value: object) -> str:
    if not isinstance(value, str):
        return "Unassigned"
    trimmed = value.strip()
    return trimmed or "Unassigned"


def _normalize_for_match(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", " ", value.strip().lower())


def _normalize_market_category(value: object) -> str:
    text = _as_non_empty_text(value)
    normalized = _normalize_for_match(text)
    if normalized == "speciality stores":
        return "Specialty Stores"
    return text


def _quantile(sorted_values: list[float], q: float) -> float:
    if len(sorted_values) == 1:
        return sorted_values[0]
    q = max(0.0, min(1.0, q))
    pos = (len(sorted_values) - 1) * q
    low = int(pos)
    high = min(low + 1, len(sorted_values) - 1)
    if low == high:
        return sorted_values[low]
    frac = pos - low
    return sorted_values[low] * (1.0 - frac) + sorted_values[high] * frac


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    size = len(ordered)
    mid = size // 2
    if size % 2 == 1:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _winsorized_median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    p5 = _quantile(ordered, 0.05)
    p95 = _quantile(ordered, 0.95)
    clipped = [min(max(value, p5), p95) for value in ordered]
    return _median(clipped)


def _consideration_cache_key(root: Path, study_ids: list[str]) -> str:
    return f"consideration:{root}:{','.join(sorted(study_ids))}"


def _satisfaction_cache_key(root: Path, study_ids: list[str]) -> str:
    return f"satisfaction:{root}:{','.join(sorted(study_ids))}"


def _csat_cache_key(root: Path, study_ids: list[str]) -> str:
    return f"csat:{root}:{','.join(sorted(study_ids))}"


def _build_consideration_rate_model(root: Path) -> dict:
    study_ids = _discover_curated_studies(root)
    buckets: dict[tuple[str, ...], dict[str, object]] = {}

    def _get_bucket(level_key: tuple[str, ...]) -> dict[str, object]:
        bucket = buckets.get(level_key)
        if bucket is None:
            bucket = {
                "c_over_a": [],
                "p_over_c": [],
                "comparisons_total": 0,
                "comparisons_p_gt_c": 0,
            }
            buckets[level_key] = bucket
        return bucket

    for study_id in study_ids:
        year = _study_year_from_id(study_id)
        if not _is_training_year_valid(year):
            continue
        classification = _classification_for_study(root, study_id)
        market_sector = _as_non_empty_text(classification.get("market_sector"))
        market_subsector = _as_non_empty_text(classification.get("market_subsector"))
        market_category = _normalize_market_category(classification.get("market_category"))
        if _normalize_for_match(market_category) in CONSIDERATION_EXCLUDED_MARKET_CATEGORIES:
            continue

        rows = _compute_table_rows_internal(
            study_id=study_id,
            respondent_cte=None,
            respondent_params=[],
            strict_missing=False,
            apply_consideration_imputation=False,
            apply_satisfaction_imputation=False,
            apply_csat_imputation=False,
        )
        for row in rows:
            awareness = row.get("brand_awareness")
            consideration = row.get("brand_consideration")
            purchase = row.get("brand_purchase")
            for key in (
                ("category", market_sector, market_subsector, market_category),
                ("subsector", market_sector, market_subsector),
                ("sector", market_sector),
                ("global",),
            ):
                bucket = _get_bucket(key)
                if isinstance(consideration, (int, float)) and isinstance(purchase, (int, float)):
                    bucket["comparisons_total"] = int(bucket["comparisons_total"]) + 1
                    if float(purchase) > float(consideration):
                        bucket["comparisons_p_gt_c"] = int(bucket["comparisons_p_gt_c"]) + 1
                if isinstance(awareness, (int, float)) and float(awareness) > 0 and isinstance(consideration, (int, float)):
                    bucket["c_over_a"].append(float(consideration) / float(awareness))
                if isinstance(consideration, (int, float)) and float(consideration) > 0 and isinstance(purchase, (int, float)):
                    bucket["p_over_c"].append(float(purchase) / float(consideration))

    rates: dict[tuple[str, ...], dict[str, float | int | None]] = {}
    warnings: list[dict[str, object]] = []
    for key, bucket in buckets.items():
        c_over_a = [float(value) for value in bucket["c_over_a"] if isinstance(value, (int, float))]
        p_over_c = [float(value) for value in bucket["p_over_c"] if isinstance(value, (int, float))]
        comparisons_total = int(bucket["comparisons_total"])
        comparisons_p_gt_c = int(bucket["comparisons_p_gt_c"])
        anomaly_pct = (
            (comparisons_p_gt_c / comparisons_total)
            if comparisons_total > 0
            else None
        )
        rates[key] = {
            "n_ca": len(c_over_a),
            "r_ca": _winsorized_median(c_over_a),
            "n_pc": len(p_over_c),
            "r_pc": _winsorized_median(p_over_c),
            "comparisons_total": comparisons_total,
            "comparisons_p_gt_c": comparisons_p_gt_c,
            "p_gt_c_pct": anomaly_pct,
        }
        if key and key[0] == "category" and anomaly_pct is not None and anomaly_pct > CONSIDERATION_IMPUTE_WARN_THRESHOLD:
            warnings.append(
                {
                    "level": "category",
                    "market_sector": key[1] if len(key) > 1 else None,
                    "market_subsector": key[2] if len(key) > 2 else None,
                    "market_category": key[3] if len(key) > 3 else None,
                    "purchase_gt_consideration_pct": round(anomaly_pct * 100, 1),
                    "training_n": comparisons_total,
                }
            )

    return {
        "version": CONSIDERATION_IMPUTE_VERSION,
        "study_ids": study_ids,
        "rates": rates,
        "warnings": warnings,
    }


def _get_consideration_rate_model(root: Path) -> dict:
    studies = _discover_curated_studies(root)
    cache_key = _consideration_cache_key(root, studies)
    entry = _CONSIDERATION_IMPUTE_CACHE.get(cache_key)
    if entry and time.time() - entry[0] <= CONSIDERATION_IMPUTE_CACHE_TTL_SECONDS:
        return entry[1]
    model = _build_consideration_rate_model(root)
    _CONSIDERATION_IMPUTE_CACHE.clear()
    _CONSIDERATION_IMPUTE_CACHE[cache_key] = (time.time(), model)
    return model


def _build_satisfaction_rate_model(root: Path) -> dict:
    study_ids = _discover_curated_studies(root)
    buckets: dict[tuple[str, ...], dict[str, object]] = {}

    def _get_bucket(level_key: tuple[str, ...]) -> dict[str, object]:
        bucket = buckets.get(level_key)
        if bucket is None:
            bucket = {
                "s_over_p": [],
                "s_over_r": [],
                "comparisons_total": 0,
                "comparisons_r_gt_s": 0,
            }
            buckets[level_key] = bucket
        return bucket

    for study_id in study_ids:
        year = _study_year_from_id(study_id)
        if not _is_training_year_valid(year):
            continue
        classification = _classification_for_study(root, study_id)
        market_sector = _as_non_empty_text(classification.get("market_sector"))
        market_subsector = _as_non_empty_text(classification.get("market_subsector"))
        market_category = _normalize_market_category(classification.get("market_category"))
        if _normalize_for_match(market_category) in CONSIDERATION_EXCLUDED_MARKET_CATEGORIES:
            continue

        rows = _compute_table_rows_internal(
            study_id=study_id,
            respondent_cte=None,
            respondent_params=[],
            strict_missing=False,
            apply_consideration_imputation=False,
            apply_satisfaction_imputation=False,
            apply_csat_imputation=False,
        )
        for row in rows:
            purchase = row.get("brand_purchase")
            recommendation = row.get("brand_recommendation")
            satisfaction = row.get("brand_satisfaction")
            for key in (
                ("category", market_sector, market_subsector, market_category),
                ("subsector", market_sector, market_subsector),
                ("sector", market_sector),
                ("global",),
            ):
                bucket = _get_bucket(key)
                if isinstance(recommendation, (int, float)) and isinstance(satisfaction, (int, float)):
                    bucket["comparisons_total"] = int(bucket["comparisons_total"]) + 1
                    if float(recommendation) > float(satisfaction):
                        bucket["comparisons_r_gt_s"] = int(bucket["comparisons_r_gt_s"]) + 1
                if isinstance(purchase, (int, float)) and float(purchase) > 0 and isinstance(satisfaction, (int, float)):
                    bucket["s_over_p"].append(float(satisfaction) / float(purchase))
                if (
                    isinstance(recommendation, (int, float))
                    and float(recommendation) > 0
                    and isinstance(satisfaction, (int, float))
                ):
                    bucket["s_over_r"].append(float(satisfaction) / float(recommendation))

    rates: dict[tuple[str, ...], dict[str, float | int | None]] = {}
    warnings: list[dict[str, object]] = []
    for key, bucket in buckets.items():
        s_over_p = [float(value) for value in bucket["s_over_p"] if isinstance(value, (int, float))]
        s_over_r = [float(value) for value in bucket["s_over_r"] if isinstance(value, (int, float))]
        comparisons_total = int(bucket["comparisons_total"])
        comparisons_r_gt_s = int(bucket["comparisons_r_gt_s"])
        anomaly_pct = (
            (comparisons_r_gt_s / comparisons_total)
            if comparisons_total > 0
            else None
        )
        rates[key] = {
            "n_sp": len(s_over_p),
            "r_sp": _winsorized_median(s_over_p),
            "n_sr": len(s_over_r),
            "r_sr": _winsorized_median(s_over_r),
            "comparisons_total": comparisons_total,
            "comparisons_r_gt_s": comparisons_r_gt_s,
            "r_gt_s_pct": anomaly_pct,
        }
        if key and key[0] == "category" and anomaly_pct is not None and anomaly_pct > SATISFACTION_IMPUTE_WARN_THRESHOLD:
            warnings.append(
                {
                    "level": "category",
                    "market_sector": key[1] if len(key) > 1 else None,
                    "market_subsector": key[2] if len(key) > 2 else None,
                    "market_category": key[3] if len(key) > 3 else None,
                    "recommendation_gt_satisfaction_pct": round(anomaly_pct * 100, 1),
                    "training_n": comparisons_total,
                }
            )

    return {
        "version": SATISFACTION_IMPUTE_VERSION,
        "study_ids": study_ids,
        "rates": rates,
        "warnings": warnings,
    }


def _get_satisfaction_rate_model(root: Path) -> dict:
    studies = _discover_curated_studies(root)
    cache_key = _satisfaction_cache_key(root, studies)
    entry = _SATISFACTION_IMPUTE_CACHE.get(cache_key)
    if entry and time.time() - entry[0] <= SATISFACTION_IMPUTE_CACHE_TTL_SECONDS:
        return entry[1]
    model = _build_satisfaction_rate_model(root)
    _SATISFACTION_IMPUTE_CACHE.clear()
    _SATISFACTION_IMPUTE_CACHE[cache_key] = (time.time(), model)
    return model


def _build_csat_gap_model(root: Path) -> dict:
    study_ids = _discover_curated_studies(root)
    buckets: dict[tuple[str, ...], dict[str, object]] = {}

    def _get_bucket(level_key: tuple[str, ...]) -> dict[str, object]:
        bucket = buckets.get(level_key)
        if bucket is None:
            bucket = {
                "sat_minus_csat": [],
                "csat_minus_rec": [],
                "comparisons_total": 0,
                "comparisons_csat_gt_sat": 0,
            }
            buckets[level_key] = bucket
        return bucket

    for study_id in study_ids:
        year = _study_year_from_id(study_id)
        if not _is_training_year_valid(year):
            continue
        classification = _classification_for_study(root, study_id)
        market_sector = _as_non_empty_text(classification.get("market_sector"))
        market_subsector = _as_non_empty_text(classification.get("market_subsector"))
        market_category = _normalize_market_category(classification.get("market_category"))
        if _normalize_for_match(market_category) in CONSIDERATION_EXCLUDED_MARKET_CATEGORIES:
            continue

        rows = _compute_table_rows_internal(
            study_id=study_id,
            respondent_cte=None,
            respondent_params=[],
            strict_missing=False,
            apply_consideration_imputation=False,
            apply_satisfaction_imputation=False,
            apply_csat_imputation=False,
        )
        for row in rows:
            satisfaction = row.get("brand_satisfaction")
            csat = row.get("csat")
            recommendation = row.get("brand_recommendation")
            if not isinstance(satisfaction, (int, float)) or not isinstance(csat, (int, float)):
                continue
            delta = float(satisfaction) - float(csat)
            for key in (
                ("category", market_sector, market_subsector, market_category),
                ("subsector", market_sector, market_subsector),
                ("sector", market_sector),
                ("global",),
            ):
                bucket = _get_bucket(key)
                bucket["sat_minus_csat"].append(delta)
                if isinstance(recommendation, (int, float)):
                    bucket["csat_minus_rec"].append(float(csat) - float(recommendation))
                bucket["comparisons_total"] = int(bucket["comparisons_total"]) + 1
                if float(csat) > float(satisfaction):
                    bucket["comparisons_csat_gt_sat"] = int(bucket["comparisons_csat_gt_sat"]) + 1

    rates: dict[tuple[str, ...], dict[str, float | int | None]] = {}
    warnings: list[dict[str, object]] = []
    for key, bucket in buckets.items():
        deltas = [float(value) for value in bucket["sat_minus_csat"] if isinstance(value, (int, float))]
        csat_rec_deltas = [float(value) for value in bucket["csat_minus_rec"] if isinstance(value, (int, float))]
        comparisons_total = int(bucket["comparisons_total"])
        comparisons_csat_gt_sat = int(bucket["comparisons_csat_gt_sat"])
        anomaly_pct = (
            (comparisons_csat_gt_sat / comparisons_total)
            if comparisons_total > 0
            else None
        )
        rates[key] = {
            "n_delta_sc": len(deltas),
            "delta_sc": _winsorized_median(deltas),
            "n_delta_cr": len(csat_rec_deltas),
            "delta_cr": _winsorized_median(csat_rec_deltas),
            "comparisons_total": comparisons_total,
            "comparisons_csat_gt_sat": comparisons_csat_gt_sat,
            "csat_gt_sat_pct": anomaly_pct,
        }
        if key and key[0] == "category" and anomaly_pct is not None and anomaly_pct > CSAT_IMPUTE_WARN_THRESHOLD:
            warnings.append(
                {
                    "level": "category",
                    "market_sector": key[1] if len(key) > 1 else None,
                    "market_subsector": key[2] if len(key) > 2 else None,
                    "market_category": key[3] if len(key) > 3 else None,
                    "csat_gt_satisfaction_pct": round(anomaly_pct * 100, 1),
                    "training_n": comparisons_total,
                }
            )

    return {
        "version": CSAT_IMPUTE_VERSION,
        "study_ids": study_ids,
        "rates": rates,
        "warnings": warnings,
    }


def _get_csat_gap_model(root: Path) -> dict:
    studies = _discover_curated_studies(root)
    cache_key = _csat_cache_key(root, studies)
    entry = _CSAT_IMPUTE_CACHE.get(cache_key)
    if entry and time.time() - entry[0] <= CSAT_IMPUTE_CACHE_TTL_SECONDS:
        return entry[1]
    model = _build_csat_gap_model(root)
    _CSAT_IMPUTE_CACHE.clear()
    _CSAT_IMPUTE_CACHE[cache_key] = (time.time(), model)
    return model


def _resolve_rate_for_level(
    model: dict,
    metric: str,
    market_sector: str,
    market_subsector: str,
    market_category: str,
) -> tuple[float | None, str]:
    rates = model.get("rates") or {}
    lookup = [
        ("category", ("category", market_sector, market_subsector, market_category)),
        ("subsector", ("subsector", market_sector, market_subsector)),
        ("sector", ("sector", market_sector)),
        ("global", ("global",)),
    ]
    n_key = "n_ca" if metric == "ca" else "n_pc"
    r_key = "r_ca" if metric == "ca" else "r_pc"
    for level, key in lookup:
        payload = rates.get(key)
        if not isinstance(payload, dict):
            continue
        n_value = payload.get(n_key)
        rate_value = payload.get(r_key)
        if not isinstance(n_value, int) or n_value < CONSIDERATION_IMPUTE_MIN_N:
            continue
        if not isinstance(rate_value, (int, float)) or float(rate_value) <= 0:
            continue
        return float(rate_value), level
    return None, "none"


def _resolve_satisfaction_rate_for_level(
    model: dict,
    metric: str,
    market_sector: str,
    market_subsector: str,
    market_category: str,
) -> tuple[float | None, str]:
    rates = model.get("rates") or {}
    lookup = [
        ("category", ("category", market_sector, market_subsector, market_category)),
        ("subsector", ("subsector", market_sector, market_subsector)),
        ("sector", ("sector", market_sector)),
        ("global", ("global",)),
    ]
    n_key = "n_sp" if metric == "sp" else "n_sr"
    r_key = "r_sp" if metric == "sp" else "r_sr"
    for level, key in lookup:
        payload = rates.get(key)
        if not isinstance(payload, dict):
            continue
        n_value = payload.get(n_key)
        rate_value = payload.get(r_key)
        if not isinstance(n_value, int) or n_value < SATISFACTION_IMPUTE_MIN_N:
            continue
        if not isinstance(rate_value, (int, float)) or float(rate_value) <= 0:
            continue
        return float(rate_value), level
    return None, "none"


def _resolve_csat_gap_for_level(
    model: dict,
    metric: str,
    market_sector: str,
    market_subsector: str,
    market_category: str,
) -> tuple[float | None, str]:
    rates = model.get("rates") or {}
    lookup = [
        ("category", ("category", market_sector, market_subsector, market_category)),
        ("subsector", ("subsector", market_sector, market_subsector)),
        ("sector", ("sector", market_sector)),
        ("global", ("global",)),
    ]
    for level, key in lookup:
        payload = rates.get(key)
        if not isinstance(payload, dict):
            continue
        n_key = "n_delta_sc" if metric == "sc" else "n_delta_cr"
        d_key = "delta_sc" if metric == "sc" else "delta_cr"
        n_value = payload.get(n_key)
        delta = payload.get(d_key)
        if not isinstance(n_value, int) or n_value < CSAT_IMPUTE_MIN_N:
            continue
        if not isinstance(delta, (int, float)):
            continue
        return float(delta), level
    return None, "none"


def _estimate_brand_consideration(
    model: dict,
    market_sector: str,
    market_subsector: str,
    market_category: str,
    awareness: float | None,
    purchase: float | None,
) -> tuple[float | None, str]:
    candidates: list[float] = []
    levels: list[str] = []

    r_ca, level_ca = _resolve_rate_for_level(model, "ca", market_sector, market_subsector, market_category)
    if isinstance(awareness, (int, float)) and float(awareness) > 0 and isinstance(r_ca, (int, float)):
        candidates.append(float(awareness) * float(r_ca))
        levels.append(level_ca)

    r_pc, level_pc = _resolve_rate_for_level(model, "pc", market_sector, market_subsector, market_category)
    if isinstance(purchase, (int, float)) and float(purchase) > 0 and isinstance(r_pc, (int, float)) and float(r_pc) > 0:
        candidates.append(float(purchase) / float(r_pc))
        levels.append(level_pc)

    if not candidates:
        return None, "none"

    estimate = _median(candidates)
    if estimate is None:
        return None, "none"

    if isinstance(awareness, (int, float)):
        estimate = min(max(estimate, 0.0), float(awareness))
    if isinstance(purchase, (int, float)):
        estimate = max(estimate, float(purchase))

    selected_level = "global"
    if "category" in levels:
        selected_level = "category"
    elif "subsector" in levels:
        selected_level = "subsector"
    elif "sector" in levels:
        selected_level = "sector"

    return round(float(estimate), 1), selected_level


def _estimate_brand_satisfaction(
    model: dict,
    market_sector: str,
    market_subsector: str,
    market_category: str,
    purchase: float | None,
    recommendation: float | None,
    awareness: float | None,
) -> tuple[float | None, str]:
    candidates: list[float] = []
    levels: list[str] = []

    r_sp, level_sp = _resolve_satisfaction_rate_for_level(model, "sp", market_sector, market_subsector, market_category)
    if isinstance(purchase, (int, float)) and float(purchase) > 0 and isinstance(r_sp, (int, float)):
        candidates.append(float(purchase) * float(r_sp))
        levels.append(level_sp)

    r_sr, level_sr = _resolve_satisfaction_rate_for_level(model, "sr", market_sector, market_subsector, market_category)
    if isinstance(recommendation, (int, float)) and float(recommendation) > 0 and isinstance(r_sr, (int, float)):
        candidates.append(float(recommendation) * float(r_sr))
        levels.append(level_sr)

    if not candidates:
        return None, "none"

    estimate = _median(candidates)
    if estimate is None:
        return None, "none"

    estimate = min(max(estimate, 0.0), 100.0)
    if isinstance(purchase, (int, float)):
        estimate = min(estimate, float(purchase))
    if isinstance(recommendation, (int, float)):
        estimate = max(estimate, float(recommendation))
    if isinstance(awareness, (int, float)):
        estimate = min(estimate, float(awareness))

    selected_level = "global"
    if "category" in levels:
        selected_level = "category"
    elif "subsector" in levels:
        selected_level = "subsector"
    elif "sector" in levels:
        selected_level = "sector"

    return round(float(estimate), 1), selected_level


def _estimate_csat_from_satisfaction(
    model: dict,
    market_sector: str,
    market_subsector: str,
    market_category: str,
    satisfaction: float | None,
    recommendation: float | None,
) -> tuple[float | None, str]:
    candidates: list[float] = []
    levels: list[str] = []

    if isinstance(satisfaction, (int, float)):
        gap_sc, level_sc = _resolve_csat_gap_for_level(
            model=model,
            metric="sc",
            market_sector=market_sector,
            market_subsector=market_subsector,
            market_category=market_category,
        )
        if isinstance(gap_sc, (int, float)):
            candidates.append(float(satisfaction) - float(gap_sc))
            levels.append(level_sc)

    if isinstance(recommendation, (int, float)):
        delta_cr, level_cr = _resolve_csat_gap_for_level(
            model=model,
            metric="cr",
            market_sector=market_sector,
            market_subsector=market_subsector,
            market_category=market_category,
        )
        if isinstance(delta_cr, (int, float)):
            candidates.append(float(recommendation) + float(delta_cr))
            levels.append(level_cr)

    if not candidates:
        return None, "none"

    estimate = _median(candidates)
    if estimate is None:
        return None, "none"
    level = "global"
    if "category" in levels:
        level = "category"
    elif "subsector" in levels:
        level = "subsector"
    elif "sector" in levels:
        level = "sector"

    estimate = min(100.0, max(-100.0, float(estimate)))
    return round(estimate, 1), level


def _apply_consideration_imputation_to_rows(
    rows: list[dict],
    market_sector: str,
    market_subsector: str,
    market_category: str,
) -> dict[str, object]:
    model = _get_consideration_rate_model(get_repo_root())
    metrics = {
        "total_rows": len(rows),
        "imputed_rows": 0,
        "levels": {"category": 0, "subsector": 0, "sector": 0, "global": 0, "none": 0},
        "post_purchase_gt_consideration_rows": 0,
        "post_comparable_rows": 0,
        "warnings": [],
        "version": CONSIDERATION_IMPUTE_VERSION,
    }

    training_warnings = model.get("warnings")
    if isinstance(training_warnings, list):
        target_norm = _normalize_for_match(market_category)
        scoped = [
            warning
            for warning in training_warnings
            if isinstance(warning, dict)
            and _normalize_for_match(warning.get("market_category")) == target_norm
        ]
        metrics["warnings"] = scoped[:3]

    for row in rows:
        consideration = row.get("brand_consideration")
        purchase = row.get("brand_purchase")
        awareness = row.get("brand_awareness")
        if isinstance(consideration, (int, float)):
            row["brand_consideration_source"] = "observed"
            row["brand_consideration_imputed"] = None
            row["brand_consideration_impute_level"] = "none"
            row["brand_consideration_impute_version"] = None
        else:
            estimated, level = _estimate_brand_consideration(
                model=model,
                market_sector=market_sector,
                market_subsector=market_subsector,
                market_category=market_category,
                awareness=float(awareness) if isinstance(awareness, (int, float)) else None,
                purchase=float(purchase) if isinstance(purchase, (int, float)) else None,
            )
            if isinstance(estimated, (int, float)):
                row["brand_consideration"] = estimated
                row["brand_consideration_imputed"] = estimated
                row["brand_consideration_source"] = "imputed"
                row["brand_consideration_impute_level"] = level
                row["brand_consideration_impute_version"] = CONSIDERATION_IMPUTE_VERSION
                metrics["imputed_rows"] = int(metrics["imputed_rows"]) + 1
                levels_map = metrics["levels"]
                if isinstance(levels_map, dict):
                    levels_map[level] = int(levels_map.get(level, 0)) + 1
            else:
                row["brand_consideration_imputed"] = None
                row["brand_consideration_source"] = "observed"
                row["brand_consideration_impute_level"] = "none"
                row["brand_consideration_impute_version"] = None

        final_consideration = row.get("brand_consideration")
        if isinstance(final_consideration, (int, float)) and isinstance(purchase, (int, float)):
            metrics["post_comparable_rows"] = int(metrics["post_comparable_rows"]) + 1
            if float(purchase) > float(final_consideration):
                metrics["post_purchase_gt_consideration_rows"] = int(metrics["post_purchase_gt_consideration_rows"]) + 1

    if metrics["total_rows"]:
        metrics["imputed_pct"] = round((int(metrics["imputed_rows"]) / int(metrics["total_rows"])) * 100, 1)
    else:
        metrics["imputed_pct"] = 0.0

    comparable = int(metrics["post_comparable_rows"])
    if comparable > 0:
        anomaly_pct = int(metrics["post_purchase_gt_consideration_rows"]) / comparable
        metrics["post_purchase_gt_consideration_pct"] = round(anomaly_pct * 100, 1)
        if anomaly_pct > CONSIDERATION_IMPUTE_WARN_THRESHOLD:
            warning = {
                "level": "category",
                "market_sector": market_sector,
                "market_subsector": market_subsector,
                "market_category": market_category,
                "purchase_gt_consideration_pct": round(anomaly_pct * 100, 1),
                "post_n": comparable,
            }
            warnings = metrics["warnings"]
            if isinstance(warnings, list):
                warnings.append(warning)
            else:
                metrics["warnings"] = [warning]
    else:
        metrics["post_purchase_gt_consideration_pct"] = None

    return metrics


def _apply_satisfaction_imputation_to_rows(
    rows: list[dict],
    market_sector: str,
    market_subsector: str,
    market_category: str,
) -> dict[str, object]:
    model = _get_satisfaction_rate_model(get_repo_root())
    metrics = {
        "total_rows": len(rows),
        "imputed_rows": 0,
        "levels": {"category": 0, "subsector": 0, "sector": 0, "global": 0, "none": 0},
        "post_recommendation_gt_satisfaction_rows": 0,
        "post_comparable_rows": 0,
        "warnings": [],
        "version": SATISFACTION_IMPUTE_VERSION,
    }

    training_warnings = model.get("warnings")
    if isinstance(training_warnings, list):
        target_norm = _normalize_for_match(market_category)
        scoped = [
            warning
            for warning in training_warnings
            if isinstance(warning, dict)
            and _normalize_for_match(warning.get("market_category")) == target_norm
        ]
        metrics["warnings"] = scoped[:3]

    for row in rows:
        satisfaction = row.get("brand_satisfaction")
        purchase = row.get("brand_purchase")
        recommendation = row.get("brand_recommendation")
        awareness = row.get("brand_awareness")
        if isinstance(satisfaction, (int, float)):
            row["brand_satisfaction_source"] = "observed"
            row["brand_satisfaction_imputed"] = None
            row["brand_satisfaction_impute_level"] = "none"
            row["brand_satisfaction_impute_version"] = None
        else:
            estimated, level = _estimate_brand_satisfaction(
                model=model,
                market_sector=market_sector,
                market_subsector=market_subsector,
                market_category=market_category,
                purchase=float(purchase) if isinstance(purchase, (int, float)) else None,
                recommendation=float(recommendation) if isinstance(recommendation, (int, float)) else None,
                awareness=float(awareness) if isinstance(awareness, (int, float)) else None,
            )
            if isinstance(estimated, (int, float)):
                row["brand_satisfaction"] = estimated
                row["brand_satisfaction_imputed"] = estimated
                row["brand_satisfaction_source"] = "imputed"
                row["brand_satisfaction_impute_level"] = level
                row["brand_satisfaction_impute_version"] = SATISFACTION_IMPUTE_VERSION
                metrics["imputed_rows"] = int(metrics["imputed_rows"]) + 1
                levels_map = metrics["levels"]
                if isinstance(levels_map, dict):
                    levels_map[level] = int(levels_map.get(level, 0)) + 1
            else:
                row["brand_satisfaction_imputed"] = None
                row["brand_satisfaction_source"] = "none"
                row["brand_satisfaction_impute_level"] = "none"
                row["brand_satisfaction_impute_version"] = None

        final_satisfaction = row.get("brand_satisfaction")
        if isinstance(final_satisfaction, (int, float)) and isinstance(recommendation, (int, float)):
            metrics["post_comparable_rows"] = int(metrics["post_comparable_rows"]) + 1
            if float(recommendation) > float(final_satisfaction):
                metrics["post_recommendation_gt_satisfaction_rows"] = int(
                    metrics["post_recommendation_gt_satisfaction_rows"]
                ) + 1

    if metrics["total_rows"]:
        metrics["imputed_pct"] = round((int(metrics["imputed_rows"]) / int(metrics["total_rows"])) * 100, 1)
    else:
        metrics["imputed_pct"] = 0.0

    comparable = int(metrics["post_comparable_rows"])
    if comparable > 0:
        anomaly_pct = int(metrics["post_recommendation_gt_satisfaction_rows"]) / comparable
        metrics["post_recommendation_gt_satisfaction_pct"] = round(anomaly_pct * 100, 1)
        if anomaly_pct > SATISFACTION_IMPUTE_WARN_THRESHOLD:
            warning = {
                "level": "category",
                "market_sector": market_sector,
                "market_subsector": market_subsector,
                "market_category": market_category,
                "recommendation_gt_satisfaction_pct": round(anomaly_pct * 100, 1),
                "post_n": comparable,
            }
            warnings = metrics["warnings"]
            if isinstance(warnings, list):
                warnings.append(warning)
            else:
                metrics["warnings"] = [warning]
    else:
        metrics["post_recommendation_gt_satisfaction_pct"] = None

    return metrics


def _apply_csat_imputation_to_rows(
    rows: list[dict],
    market_sector: str,
    market_subsector: str,
    market_category: str,
) -> dict[str, object]:
    model = _get_csat_gap_model(get_repo_root())
    metrics = {
        "total_rows": len(rows),
        "eligible_rows": 0,
        "imputed_rows": 0,
        "levels": {"category": 0, "subsector": 0, "sector": 0, "global": 0, "none": 0},
        "post_csat_gt_satisfaction_rows": 0,
        "post_comparable_rows": 0,
        "warnings": [],
        "version": CSAT_IMPUTE_VERSION,
    }

    training_warnings = model.get("warnings")
    if isinstance(training_warnings, list):
        target_norm = _normalize_for_match(market_category)
        scoped = [
            warning
            for warning in training_warnings
            if isinstance(warning, dict)
            and _normalize_for_match(warning.get("market_category")) == target_norm
        ]
        metrics["warnings"] = scoped[:3]

    for row in rows:
        csat = row.get("csat")
        satisfaction = row.get("brand_satisfaction")

        if isinstance(csat, (int, float)):
            row["csat_source"] = "observed"
            row["csat_imputed"] = None
            row["csat_impute_level"] = "none"
            row["csat_impute_version"] = None
        elif (
            isinstance(satisfaction, (int, float))
            or isinstance(row.get("brand_recommendation"), (int, float))
        ):
            metrics["eligible_rows"] = int(metrics["eligible_rows"]) + 1
            estimated, level = _estimate_csat_from_satisfaction(
                model=model,
                market_sector=market_sector,
                market_subsector=market_subsector,
                market_category=market_category,
                satisfaction=float(satisfaction) if isinstance(satisfaction, (int, float)) else None,
                recommendation=float(row.get("brand_recommendation"))
                if isinstance(row.get("brand_recommendation"), (int, float))
                else None,
            )
            if isinstance(estimated, (int, float)):
                row["csat"] = estimated
                row["csat_imputed"] = estimated
                row["csat_source"] = "imputed"
                row["csat_impute_level"] = level
                row["csat_impute_version"] = CSAT_IMPUTE_VERSION
                metrics["imputed_rows"] = int(metrics["imputed_rows"]) + 1
                levels_map = metrics["levels"]
                if isinstance(levels_map, dict):
                    levels_map[level] = int(levels_map.get(level, 0)) + 1
            else:
                row["csat_imputed"] = None
                row["csat_source"] = "none"
                row["csat_impute_level"] = "none"
                row["csat_impute_version"] = None
        else:
            row["csat_imputed"] = None
            row["csat_source"] = "none"
            row["csat_impute_level"] = "none"
            row["csat_impute_version"] = None

        final_csat = row.get("csat")
        final_satisfaction = row.get("brand_satisfaction")
        if isinstance(final_csat, (int, float)) and isinstance(final_satisfaction, (int, float)):
            metrics["post_comparable_rows"] = int(metrics["post_comparable_rows"]) + 1
            if float(final_csat) > float(final_satisfaction):
                metrics["post_csat_gt_satisfaction_rows"] = int(metrics["post_csat_gt_satisfaction_rows"]) + 1

    if metrics["eligible_rows"]:
        metrics["imputed_pct"] = round((int(metrics["imputed_rows"]) / int(metrics["eligible_rows"])) * 100, 1)
    else:
        metrics["imputed_pct"] = 0.0

    comparable = int(metrics["post_comparable_rows"])
    if comparable > 0:
        anomaly_pct = int(metrics["post_csat_gt_satisfaction_rows"]) / comparable
        metrics["post_csat_gt_satisfaction_pct"] = round(anomaly_pct * 100, 1)
        if anomaly_pct > CSAT_IMPUTE_WARN_THRESHOLD:
            warning = {
                "level": "category",
                "market_sector": market_sector,
                "market_subsector": market_subsector,
                "market_category": market_category,
                "csat_gt_satisfaction_pct": round(anomaly_pct * 100, 1),
                "post_n": comparable,
            }
            warnings = metrics["warnings"]
            if isinstance(warnings, list):
                warnings.append(warning)
            else:
                metrics["warnings"] = [warning]
    else:
        metrics["post_csat_gt_satisfaction_pct"] = None

    return metrics


def _sql_literal(value: str) -> str:
    return value.replace("'", "''")



def _with_clause(*ctes: str) -> str:
    cleaned: list[str] = []
    for cte in ctes:
        if not cte:
            continue
        candidate = cte.strip()
        if not candidate:
            continue
        candidate = re.sub(r"^\s*WITH\s+", "", candidate, flags=re.IGNORECASE).strip()
        candidate = candidate.rstrip(", \n\t")
        if candidate:
            cleaned.append(candidate)
    if not cleaned:
        return ""
    return "WITH " + ",\n".join(cleaned)


def _parquet_columns(path: Path) -> set[str]:
    if not path.exists():
        return set()
    conn = get_duckdb_connection()
    try:
        cursor = conn.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0")
        return {column[0] for column in cursor.description}
    finally:
        conn.close()


def _column_or_null(columns: set[str], column_name: str) -> str:
    return column_name if column_name in columns else f"NULL AS {column_name}"


def _parse_filters(payload: dict | None) -> dict:
    if not payload:
        payload = {}
    study_ids = payload.get("study_ids") or payload.get("study_id") or []
    if isinstance(study_ids, str):
        study_ids = [item.strip() for item in study_ids.split(",") if item.strip()]
    brands = payload.get("brands") or []
    if isinstance(brands, str):
        brands = [item.strip() for item in brands.split(",") if item.strip()]
    years = payload.get("years") or []
    if isinstance(years, str):
        years = [item.strip() for item in years.split(",") if item.strip()]
    normalized_years = [item for item in years if isinstance(item, str) and item.isdigit() and len(item) == 4]
    def _normalize_demo_values(raw: object) -> list[str]:
        if raw is None:
            return []
        if isinstance(raw, str):
            return [item.strip() for item in raw.split(",") if item.strip()]
        if isinstance(raw, list):
            values: list[str] = []
            for item in raw:
                if isinstance(item, str):
                    trimmed = item.strip()
                    if trimmed:
                        values.append(trimmed)
            return values
        return []

    genders = _normalize_demo_values(payload.get("gender"))
    nses_raw = _normalize_demo_values(payload.get("nse"))

    nse_group_map = {
        "AB": ["AB", "A", "B"],
        "C": ["C+", "C", "C-"],
        "DE": ["D+", "D", "DE", "E"],
    }
    nses: list[str] = []
    for value in nses_raw:
        token = value.strip().upper().replace(" ", "")
        if token in nse_group_map:
            nses.extend(nse_group_map[token])
        else:
            nses.append(value)
    nses = list(dict.fromkeys(nses))

    states = _normalize_demo_values(payload.get("state"))
    return {
        "study_ids": study_ids,
        "brands": brands,
        "taxonomy_view": "standard"
        if str(payload.get("taxonomy_view") or "").strip().lower() == "standard"
        else "market",
        "years": normalized_years,
        "sector": payload.get("sector"),
        "subsector": payload.get("subsector"),
        "category": payload.get("category"),
        "gender": genders,
        "nse": nses,
        "state": states,
        "age_min": payload.get("age_min"),
        "age_max": payload.get("age_max"),
        "date_grain": payload.get("date_grain") or "Q",
        "date_from": payload.get("date_from"),
        "date_to": payload.get("date_to"),
}


def _taxonomy_key(filters: dict, key: str) -> str:
    if filters.get("taxonomy_view") == "standard":
        return key
    return f"market_{key}"


def _effective_classification_values(
    classification: dict[str, str | None], filters: dict
) -> dict[str, str]:
    return {
        "sector": (classification.get(_taxonomy_key(filters, "sector")) or "").strip() or "Unassigned",
        "subsector": (classification.get(_taxonomy_key(filters, "subsector")) or "").strip() or "Unassigned",
        "category": (classification.get(_taxonomy_key(filters, "category")) or "").strip() or "Unassigned",
    }


def _tracking_series_cache_key(filters: dict) -> str:
    return json.dumps({"filters": filters}, sort_keys=True, separators=(",", ":"))


def _tracking_series_get_cached(key: str) -> dict | None:
    entry = _TRACKING_SERIES_CACHE.get(key)
    if not entry:
        return None
    created_at, payload = entry
    if time.time() - created_at > TRACKING_SERIES_CACHE_TTL_SECONDS:
        _TRACKING_SERIES_CACHE.pop(key, None)
        return None
    cloned = dict(payload)
    meta = dict(cloned.get("meta") or {})
    meta["cache_hit"] = True
    cloned["meta"] = meta
    return cloned


def _tracking_series_set_cached(key: str, payload: dict) -> None:
    _TRACKING_SERIES_CACHE[key] = (time.time(), payload)


def _quarter_label_from_key(q_key: int) -> str:
    return f"{q_key // 10}-Q{q_key % 10}"


def _collect_available_quarters_filtered(study_id: str, filters: dict) -> list[int]:
    root = get_repo_root()
    respondents_path = (
        root / "data" / "warehouse" / "raw" / f"study_id={study_id}" / "respondents.parquet"
    )
    if not respondents_path.exists():
        return []

    respondent_cte, respondent_params, eligible = _respondent_filter_cte(study_id, filters)
    if not eligible:
        return []

    conn = get_duckdb_connection()
    try:
        if respondent_cte:
            with_prefix = _with_clause(respondent_cte)
            query = f"""
                {with_prefix}
                SELECT DISTINCT q_key
                FROM filtered_respondents
                WHERE q_key IS NOT NULL
                ORDER BY q_key
            """
            rows = conn.execute(query, respondent_params).fetchall()
            return [int(row[0]) for row in rows if row and row[0] is not None]

        columns = _parquet_columns(respondents_path)
        if "date" not in columns:
            return []
        where = ["q_key IS NOT NULL"]
        params: list = []
        years = filters.get("years") or []
        if years:
            placeholders = ",".join("?" for _ in years)
            where.append(f"CAST(q_key / 10 AS INTEGER) IN ({placeholders})")
            params.extend([int(year) for year in years])
        rows = conn.execute(
            f"""
            SELECT DISTINCT q_key FROM (
                SELECT EXTRACT(year FROM TRY_CAST(date AS DATE)) * 10
                    + EXTRACT(quarter FROM TRY_CAST(date AS DATE)) AS q_key
                FROM read_parquet('{respondents_path}')
            ) q
            WHERE {' AND '.join(where)}
            ORDER BY q_key
            """,
            params,
        ).fetchall()
        return [int(row[0]) for row in rows if row and row[0] is not None]
    finally:
        conn.close()


def _build_tracking_periods(all_quarters: set[int]) -> tuple[str, list[dict]]:
    if not all_quarters:
        return "year", []
    years = sorted({quarter // 10 for quarter in all_quarters})
    if len(years) > 1:
        periods = [
            {
                "key": str(year),
                "label": str(year),
                "order": year,
            }
            for year in years
        ]
        return "year", periods

    quarters = sorted(all_quarters)
    periods = [
        {
            "key": _quarter_label_from_key(quarter),
            "label": _quarter_label_from_key(quarter),
            "order": quarter,
        }
        for quarter in quarters
    ]
    return "quarter", periods


def _tracking_metric_average(
    rows: list[dict], metric: str, weight_key: str | None = "base_n_population"
) -> float | None:
    points: list[tuple[float, float]] = []
    for row in rows:
        value = row.get(metric)
        if not isinstance(value, (int, float)):
            continue
        weight = row.get(weight_key) if weight_key else None
        if not isinstance(weight, (int, float)) or weight <= 0:
            weight = row.get("aggregation_weight_n")
        if not isinstance(weight, (int, float)) or weight <= 0:
            weight = 1.0
        points.append((float(value), float(weight)))
    if not points:
        return None
    total_weight = sum(weight for _, weight in points)
    if total_weight <= 0:
        return None
    return round(sum(value * weight for value, weight in points) / total_weight, 1)


def _build_series_metric_payload(period_keys: list[str], value_by_period: dict[str, float | None]) -> dict:
    values = {period_key: value_by_period.get(period_key) for period_key in period_keys}
    deltas: dict[str, float | None] = {}
    for index in range(1, len(period_keys)):
        previous_key = period_keys[index - 1]
        current_key = period_keys[index]
        previous = values.get(previous_key)
        current = values.get(current_key)
        delta_key = f"{previous_key}->{current_key}"
        if isinstance(previous, (int, float)) and isinstance(current, (int, float)):
            deltas[delta_key] = round(float(current) - float(previous), 1)
        else:
            deltas[delta_key] = None
    return {"values": values, "deltas": deltas}


def _resolve_tracking_breakdown(filters: dict) -> tuple[str, str]:
    labels = (
        ("Macrosector", "Segmento", "Categoría comercial")
        if filters.get("taxonomy_view") == "market"
        else ("Sector", "Subsector", "Category")
    )
    if not filters.get("sector"):
        return "sector", labels[0]
    if not filters.get("subsector"):
        return "subsector", labels[1]
    if not filters.get("category"):
        return "category", labels[2]
    return "brand", "Brand"


def _tracking_entity_name(
    breakdown: str,
    classification: dict[str, str | None],
    filters: dict,
    brand_name: str | None = None,
) -> str:
    if breakdown == "brand":
        return (brand_name or "").strip() or "Unassigned"
    key = _taxonomy_key(filters, breakdown)
    return (classification.get(key) or "").strip() or "Unassigned"


def _tracking_series_filtered(filters: dict) -> dict:
    started_at = time.perf_counter()
    cache_key = _tracking_series_cache_key(filters)
    cached = _tracking_series_get_cached(cache_key)
    if cached is not None:
        return cached

    root, study_ids = _resolve_study_ids(filters)
    classification_cache: dict[str, dict[str, str | None]] = {}
    matched_studies: list[str] = []
    study_quarters: dict[str, list[int]] = {}
    all_quarters: set[int] = set()

    collect_started = time.perf_counter()
    for study_id in study_ids:
        classification = classification_cache.get(study_id)
        if classification is None:
            classification = _classification_for_study(root, study_id)
            classification_cache[study_id] = classification
        if not _study_matches_taxonomy(filters, classification):
            continue
        quarters = _collect_available_quarters_filtered(study_id, filters)
        if not quarters:
            continue
        matched_studies.append(study_id)
        study_quarters[study_id] = quarters
        all_quarters.update(quarters)

    resolved_granularity, periods = _build_tracking_periods(all_quarters)
    collect_ms = round((time.perf_counter() - collect_started) * 1000, 2)
    resolved_breakdown, entity_label = _resolve_tracking_breakdown(filters)
    if not periods:
        payload = {
            "ok": True,
            "resolved_granularity": "year",
            "resolved_breakdown": resolved_breakdown,
            "entity_label": entity_label,
            "periods": [],
            "delta_columns": [],
            "entity_rows": [],
            "secondary_rows": [],
            "brand_rows": [],
            "touchpoint_rows": [],
            "metric_meta_brand": {},
            "metric_meta_touchpoint": {},
            "meta": {
                "warnings": ["No temporal data available for the selected scope."],
                "studies_considered": study_ids,
                "studies_used": matched_studies,
                "response_mode": "series",
                "cache_hit": False,
            },
        }
        _tracking_series_set_cached(cache_key, payload)
        return payload

    brand_metrics = [
        "brand_awareness",
        "ad_awareness",
        "brand_consideration",
        "brand_purchase",
        "brand_satisfaction",
        "brand_recommendation",
        "csat",
        "nps",
    ]
    touchpoint_metrics = ["recall", "consideration", "purchase"]
    metric_meta_brand = {
        "brand_awareness": {"label": "Brand Awareness", "unit": "%"},
        "ad_awareness": {"label": "Ad Awareness", "unit": "%"},
        "brand_consideration": {"label": "Brand Consideration", "unit": "%"},
        "brand_purchase": {"label": "Brand Purchase", "unit": "%"},
        "brand_satisfaction": {"label": "Brand Satisfaction", "unit": "%"},
        "brand_recommendation": {"label": "Brand Recommendation", "unit": "%"},
        "csat": {"label": "CSAT", "unit": "%"},
        "nps": {"label": "NPS", "unit": "%"},
    }
    metric_meta_touchpoint = {
        "recall": {"label": "Recall", "unit": "%"},
        "consideration": {"label": "Consideration", "unit": "%"},
        "purchase": {"label": "Purchase", "unit": "%"},
    }

    entity_period_buckets: dict[str, dict[str, list[dict]]] = {}
    secondary_period_buckets: dict[str, dict[str, list[dict]]] = {}
    studies_with_data: set[str] = set()
    rows_scanned = {"journey": 0, "touchpoint": 0}
    period_keys = [period["key"] for period in periods]
    valid_period_keys = set(period_keys)

    def period_key_from_quarter(q_key: int) -> str:
        if resolved_granularity == "year":
            return str(int(q_key) // 10)
        return _quarter_label_from_key(int(q_key))

    aggregate_started = time.perf_counter()
    for study_id in matched_studies:
        classification = classification_cache.get(study_id)
        if classification is None:
            classification = _classification_for_study(root, study_id)
            classification_cache[study_id] = classification

        journey_rows_by_quarter = _compute_table_rows_by_quarter_filtered(study_id, filters)
        touchpoint_rows_by_quarter = _compute_touchpoint_rows_by_quarter_filtered(study_id, filters)
        rows_scanned["journey"] += sum(len(items) for items in journey_rows_by_quarter.values())
        rows_scanned["touchpoint"] += sum(len(items) for items in touchpoint_rows_by_quarter.values())

        if journey_rows_by_quarter:
            studies_with_data.add(study_id)

        for q_key, journey_rows in journey_rows_by_quarter.items():
            period_key = period_key_from_quarter(int(q_key))
            if period_key not in valid_period_keys:
                continue
            for row in journey_rows:
                brand_name = row.get("brand")
                if not isinstance(brand_name, str) or not brand_name.strip():
                    continue
                if resolved_breakdown == "brand" and filters.get("brands") and brand_name not in filters.get("brands"):
                    continue
                entity_name = _tracking_entity_name(resolved_breakdown, classification, filters, brand_name)
                entity_period_buckets.setdefault(entity_name, {}).setdefault(period_key, []).append(row)

        for q_key, touchpoint_rows in touchpoint_rows_by_quarter.items():
            period_key = period_key_from_quarter(int(q_key))
            if period_key not in valid_period_keys:
                continue
            for row in touchpoint_rows:
                touchpoint = row.get("touchpoint")
                if not isinstance(touchpoint, str) or not touchpoint.strip():
                    continue
                row_brand = row.get("brand")
                if resolved_breakdown == "brand" and filters.get("brands") and row_brand not in filters.get("brands"):
                    continue
                secondary_period_buckets.setdefault(touchpoint, {}).setdefault(period_key, []).append(row)

    entity_rows: list[dict] = []
    for entity_name in sorted(entity_period_buckets.keys(), key=lambda item: item.lower()):
        metrics_payload: dict[str, dict] = {}
        for metric in brand_metrics:
            values_by_period = {
                period_key: _tracking_metric_average(entity_period_buckets[entity_name].get(period_key, []), metric)
                for period_key in period_keys
            }
            metrics_payload[metric] = _build_series_metric_payload(period_keys, values_by_period)
        entity_rows.append({"entity": entity_name, "metrics": metrics_payload})

    secondary_rows: list[dict] = []
    for entity_name in sorted(secondary_period_buckets.keys(), key=lambda item: item.lower()):
        metrics_payload: dict[str, dict] = {}
        for metric in touchpoint_metrics:
            values_by_period = {
                period_key: _tracking_metric_average(
                    secondary_period_buckets[entity_name].get(period_key, []), metric, None
                )
                for period_key in period_keys
            }
            metrics_payload[metric] = _build_series_metric_payload(period_keys, values_by_period)
        secondary_rows.append({"entity": entity_name, "metrics": metrics_payload})

    delta_columns = []
    for index in range(1, len(period_keys)):
        previous_key = period_keys[index - 1]
        current_key = period_keys[index]
        delta_columns.append(
            {
                "key": f"{previous_key}->{current_key}",
                "from": previous_key,
                "to": current_key,
                "label": f"Delta {current_key} vs {previous_key}",
            }
        )

    payload = {
        "ok": True,
        "resolved_granularity": resolved_granularity,
        "resolved_breakdown": resolved_breakdown,
        "entity_label": entity_label,
        "periods": [{"key": p["key"], "label": p["label"], "order": p["order"]} for p in periods],
        "delta_columns": delta_columns,
        "entity_rows": entity_rows,
        "secondary_rows": secondary_rows,
        "brand_rows": [
            {"brand": row["entity"], "metrics": row["metrics"]} for row in entity_rows
        ]
        if resolved_breakdown == "brand"
        else [],
        "touchpoint_rows": [{"touchpoint": row["entity"], "metrics": row["metrics"]} for row in secondary_rows],
        "metric_meta_brand": metric_meta_brand,
        "metric_meta_touchpoint": metric_meta_touchpoint,
        "meta": {
            "warnings": [],
            "studies_considered": study_ids,
            "studies_used": matched_studies,
            "studies_with_data": sorted(studies_with_data),
            "response_mode": "series",
            "cache_hit": False,
            "collect_ms": collect_ms,
            "aggregate_ms": round((time.perf_counter() - aggregate_started) * 1000, 2),
            "total_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "studies_processed": len(matched_studies),
            "rows_scanned": rows_scanned,
        },
    }
    _tracking_series_set_cached(cache_key, payload)
    return payload


def _journey_table_multi_cache_key(
    filters: dict,
    limit_mode: str,
    sort_by: str,
    sort_dir: str,
    include_global_benchmark: bool,
    response_mode: str,
) -> str:
    payload = {
        "filters": filters,
        "limit_mode": limit_mode.lower(),
        "sort_by": sort_by,
        "sort_dir": sort_dir.lower(),
        "include_global_benchmark": include_global_benchmark,
        "response_mode": response_mode,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _journey_table_multi_get_cached(key: str) -> dict | None:
    entry = _JOURNEY_TABLE_MULTI_CACHE.get(key)
    if not entry:
        return None
    created_at, payload = entry
    if time.time() - created_at > JOURNEY_TABLE_MULTI_CACHE_TTL_SECONDS:
        _JOURNEY_TABLE_MULTI_CACHE.pop(key, None)
        return None
    return payload


def _journey_table_multi_set_cached(key: str, payload: dict) -> None:
    _JOURNEY_TABLE_MULTI_CACHE[key] = (time.time(), payload)


def _resolve_study_ids(filters: dict) -> tuple[Path, list[str]]:
    root = get_repo_root()
    discovered = _discover_curated_studies(root)
    requested = filters.get("study_ids") or []
    if requested:
        study_ids = [study_id for study_id in requested if study_id in discovered]
    else:
        study_ids = discovered
    return root, study_ids


def _collect_journey_rows(
    root: Path,
    study_ids: list[str],
    filters: dict,
    classification_cache: dict[str, dict[str, str | None]] | None = None,
) -> tuple[list[dict], list[str], int]:
    rows: list[dict] = []
    matched_studies: list[str] = []
    local_classification_cache = classification_cache if classification_cache is not None else {}
    for study_id in study_ids:
        classification = local_classification_cache.get(study_id)
        if classification is None:
            classification = _classification_for_study(root, study_id)
            local_classification_cache[study_id] = classification
        if not _study_matches_taxonomy(filters, classification):
            continue
        safe_classification = _effective_classification_values(classification, filters)
        filtered_rows = _compute_table_rows_filtered(study_id, filters)
        if filtered_rows:
            matched_studies.append(study_id)
        for row in filtered_rows:
            rows.append(
                {
                    "study_id": study_id,
                    "study_name": study_id,
                    **safe_classification,
                    **row,
                    "quality_flags": {
                        "population_denominator": bool(row.get("base_n_population")),
                        "awareness_ceiling_applied": bool(row.get("awareness_ceiling_applied")),
                    },
                }
            )
    return rows, matched_studies, len(study_ids)


def _sort_and_limit_journey_rows(rows: list[dict], sort_by: str, sort_dir: str, limit_mode: str) -> list[dict]:
    sort_metric = sort_by if sort_by in SORTABLE_METRICS else "brand_awareness"
    reverse = sort_dir.lower() != "asc"
    sorted_rows = rows[:]
    sorted_rows.sort(key=lambda row: row.get(sort_metric) or -1, reverse=reverse)
    normalized_limit = limit_mode.lower()
    if normalized_limit == "top25":
        return sorted_rows[:25]
    if normalized_limit == "top10":
        return sorted_rows[:10]
    return sorted_rows


def _weighted_metric_average(rows: list[dict], metric: str) -> float | None:
    points: list[tuple[float, float]] = []
    for row in rows:
        value = row.get(metric)
        weight = row.get("base_n_population") or row.get("aggregation_weight_n")
        if not isinstance(value, (int, float)):
            continue
        if not isinstance(weight, (int, float)) or weight <= 0:
            weight = 1.0
        points.append((float(value), float(weight)))
    if not points:
        return None
    total_weight = sum(weight for _, weight in points)
    if total_weight <= 0:
        return None
    return round(sum(value * weight for value, weight in points) / total_weight, 1)


def _build_benchmark_summary(rows: list[dict], label: str) -> dict:
    stage_keys = [
        "brand_awareness",
        "ad_awareness",
        "brand_consideration",
        "brand_purchase",
        "brand_satisfaction",
        "brand_recommendation",
    ]
    stage_values = {key: _weighted_metric_average(rows, key) for key in stage_keys}
    link_pairs = [
        ("brand_awareness", "brand_consideration"),
        ("brand_consideration", "brand_purchase"),
        ("brand_purchase", "brand_satisfaction"),
        ("brand_satisfaction", "brand_recommendation"),
    ]
    links = []
    for from_key, to_key in link_pairs:
        from_value = stage_values.get(from_key)
        to_value = stage_values.get(to_key)
        if isinstance(from_value, (int, float)) and isinstance(to_value, (int, float)) and from_value > 0:
            conversion = round((to_value / from_value) * 100, 1)
            drop_abs = round(from_value - to_value, 1)
        else:
            conversion = None
            drop_abs = None
        links.append({"from": from_key, "to": to_key, "conversion_pct": conversion, "drop_abs_pts": drop_abs})
    return {
        "label": label,
        "stages": stage_values,
        "links": links,
        "csat": _weighted_metric_average(rows, "csat"),
        "nps": _weighted_metric_average(rows, "nps"),
        "journey_index": None,
        "funnel_health": None,
    }


def _apply_awareness_ceiling(values: dict[str, float | None]) -> bool:
    """
    Enforce questionnaire hierarchy consistency per study-brand row:
    downstream brand stages cannot exceed Brand Awareness when both exist.
    """
    awareness = values.get("brand_awareness")
    if awareness is None:
        return False
    adjusted = False
    for metric in CORE_AWARENESS_BOUNDED_METRICS:
        stage_value = values.get(metric)
        if stage_value is None:
            continue
        if stage_value > awareness:
            values[metric] = awareness
            adjusted = True
    return adjusted


def _has_brand_signal(values: dict[str, float | None]) -> bool:
    """
    Keep only rows with at least one meaningful journey signal.
    This removes synthetic brands that are all-zero/null across journey metrics.
    """
    signal_metrics = (
        "brand_awareness",
        "ad_awareness",
        "brand_consideration",
        "brand_purchase",
        "brand_satisfaction",
        "brand_recommendation",
        "csat",
        "nps",
    )
    for metric in signal_metrics:
        value = values.get(metric)
        if isinstance(value, (int, float)) and float(value) > 0:
            return True
    return False


def _study_matches_taxonomy(filters: dict, classification: dict[str, str | None]) -> bool:
    for key in ("sector", "subsector", "category"):
        value = filters.get(key)
        if not value:
            continue
        taxonomy_key = _taxonomy_key(filters, key)
        if not classification.get(taxonomy_key):
            return False
        if classification.get(taxonomy_key) != value:
            return False
    return True


def _needs_respondent_filter(filters: dict) -> bool:
    return any(
        [
            filters.get("years"),
            filters.get("gender"),
            filters.get("nse"),
            filters.get("state"),
            filters.get("age_min") is not None,
            filters.get("age_max") is not None,
            filters.get("date_from"),
            filters.get("date_to"),
        ]
    )


def _respondent_filter_cte(study_id: str, filters: dict) -> tuple[str | None, list, bool]:
    root = get_repo_root()
    respondents_path = (
        root
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "respondents.parquet"
    )
    if not _needs_respondent_filter(filters):
        return None, [], True
    if not respondents_path.exists():
        return None, [], False

    respondent_columns = _parquet_columns(respondents_path)
    if "respondent_id" not in respondent_columns:
        return None, [], False

    config = normalize_demographics_config(load_demographics_config(study_id))
    gender_var = config.get("gender_var")
    nse_var = config.get("nse_var")
    state_var = config.get("state_var")
    age_var = config.get("age_var")
    date_mode = (config.get("date") or {}).get("mode", "none")

    has_gender_standard = "gender" in respondent_columns
    if filters.get("gender") and (not has_gender_standard and (not gender_var or "gender_code" not in respondent_columns)):
        return None, [], False
    if filters.get("nse") and (not nse_var or "nse_code" not in respondent_columns):
        return None, [], False
    if filters.get("state") and (not state_var or "state_code" not in respondent_columns):
        return None, [], False
    if (filters.get("age_min") is not None or filters.get("age_max") is not None) and (
        not age_var or "age" not in respondent_columns
    ):
        return None, [], False
    if (filters.get("years") or filters.get("date_from") or filters.get("date_to")) and (
        date_mode == "none" or "date" not in respondent_columns
    ):
        return None, [], False

    labels_path = (
        root
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_value_labels.parquet"
    )
    labels_source = (
        f"SELECT * FROM read_parquet('{labels_path}')"
        if labels_path.exists()
        else "SELECT NULL::VARCHAR AS var_code, NULL::VARCHAR AS value_code, NULL::VARCHAR AS value_label WHERE FALSE"
    )

    gender_var_sql = _sql_literal(str(gender_var)) if gender_var else ""
    nse_var_sql = _sql_literal(str(nse_var)) if nse_var else ""
    state_var_sql = _sql_literal(str(state_var)) if state_var else ""

    conditions = []
    params: list = []
    genders = filters.get("gender") or []
    if genders:
        placeholders = ",".join("?" for _ in genders)
        if has_gender_standard:
            conditions.append(f"gender_standard IN ({placeholders})")
        else:
            conditions.append(f"gender_label IN ({placeholders})")
        params.extend(genders)
    nses = filters.get("nse") or []
    if nses:
        placeholders = ",".join("?" for _ in nses)
        conditions.append(f"nse_label IN ({placeholders})")
        params.extend(nses)
    states = filters.get("state") or []
    if states:
        placeholders = ",".join("?" for _ in states)
        conditions.append(f"state_label IN ({placeholders})")
        params.extend(states)
    if filters.get("age_min") is not None:
        conditions.append("age >= ?")
        params.append(filters["age_min"])
    if filters.get("age_max") is not None:
        conditions.append("age <= ?")
        params.append(filters["age_max"])

    years = filters.get("years") or []
    if years:
        placeholders = ",".join("?" for _ in years)
        conditions.append(f"CAST(EXTRACT(year FROM date_dt) AS INTEGER) IN ({placeholders})")
        params.extend([int(year) for year in years])

    if filters.get("date_from"):
        conditions.append("date_dt >= TRY_CAST(? AS DATE)")
        params.append(filters["date_from"])
    if filters.get("date_to"):
        conditions.append("date_dt <= TRY_CAST(? AS DATE)")
        params.append(filters["date_to"])

    where_clause = " AND ".join(["1=1"] + conditions)

    cte = f"""
        respondents AS (
            SELECT
                respondent_id,
                {_column_or_null(respondent_columns, "gender_code")},
                {_column_or_null(respondent_columns, "gender")},
                {_column_or_null(respondent_columns, "nse_code")},
                {_column_or_null(respondent_columns, "state_code")},
                {_column_or_null(respondent_columns, "age")},
                {_column_or_null(respondent_columns, "date")}
            FROM read_parquet('{respondents_path}')
        ),
        labels AS (
            {labels_source}
        ),
        respondents_labeled AS (
            SELECT
                r.respondent_id,
                r.age,
                r.date,
                TRY_CAST(r.date AS DATE) AS date_dt,
                EXTRACT(year FROM TRY_CAST(r.date AS DATE)) * 10
                    + EXTRACT(quarter FROM TRY_CAST(r.date AS DATE)) AS q_key,
                CASE
                    WHEN r.gender IS NOT NULL AND TRIM(CAST(r.gender AS VARCHAR)) <> '' THEN CAST(r.gender AS VARCHAR)
                    WHEN g.value_label IS NULL OR TRIM(CAST(g.value_label AS VARCHAR)) = '' THEN 'Unknown'
                    WHEN LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%prefiere no%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%prefer not%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%declina%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%no responde%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%refus%' THEN 'Prefer not to say'
                    WHEN LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%non-binary%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%non binary%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%no binario%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%no binaria%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%no binarie%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) IN ('nb', 'genderqueer') THEN 'Non-binary'
                    WHEN LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%female%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%femen%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%mujer%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) IN ('f', 'fem') THEN 'Female'
                    WHEN LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%male%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%mascul%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%hombre%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) LIKE '%varon%'
                        OR LOWER(CAST(g.value_label AS VARCHAR)) IN ('m', 'masc') THEN 'Male'
                    ELSE 'Unknown'
                END AS gender_standard,
                g.value_label AS gender_label,
                n.value_label AS nse_label,
                s.value_label AS state_label
            FROM respondents r
            LEFT JOIN labels g
                ON g.var_code = '{gender_var_sql}'
               AND g.value_code = CAST(r.gender_code AS VARCHAR)
            LEFT JOIN labels n
                ON n.var_code = '{nse_var_sql}'
               AND n.value_code = CAST(r.nse_code AS VARCHAR)
            LEFT JOIN labels s
                ON s.var_code = '{state_var_sql}'
               AND s.value_code = CAST(r.state_code AS VARCHAR)
        ),
        filtered_respondents AS (
            SELECT respondent_id, q_key
            FROM respondents_labeled
            WHERE {where_clause}
        )
    """
    return cte, params, True

def _classification_for_study(root: Path, study_id: str) -> dict[str, str | None]:
    classification_path = (
        root
        / "data"
        / "warehouse"
        / "taxonomy"
        / "study_classification"
        / f"study_id={study_id}.json"
    )
    if not classification_path.exists():
        return {
            "sector": None,
            "subsector": None,
            "category": None,
            "market_sector": None,
            "market_subsector": None,
            "market_category": None,
            "market_source": None,
        }
    try:
        payload = json.loads(classification_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        payload = json.loads(classification_path.read_text(encoding="utf-8-sig"))
    return resolve_classification(payload, root=root)


def _compute_table_rows_internal(
    study_id: str,
    respondent_cte: str | None,
    respondent_params: list,
    strict_missing: bool,
    apply_consideration_imputation: bool = True,
    apply_satisfaction_imputation: bool = True,
    apply_csat_imputation: bool = True,
) -> list[dict]:
    root = get_repo_root()
    classification = _classification_for_study(root, study_id)
    market_sector = _as_non_empty_text(classification.get("market_sector"))
    market_subsector = _as_non_empty_text(classification.get("market_subsector"))
    market_category = _normalize_market_category(classification.get("market_category"))
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if not curated_path.exists():
        if strict_missing:
            raise HTTPException(status_code=404, detail="Curated mart not found for study.")
        return []

    conn = get_duckdb_connection()
    try:
        load_parquet_as_view(conn, "journey_table", str(curated_path))
        has_value_raw = (
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'journey_table' AND column_name = 'value_raw'
                """
            ).fetchone()[0]
            > 0
        )
        value_expr = (
            "CASE "
            "WHEN LOWER(stage) IN ('satisfaction', 'recommendation') "
            "THEN TRY_CAST(value_raw AS INTEGER) "
            "ELSE COALESCE(TRY_CAST(value AS INTEGER), TRY_CAST(value_raw AS INTEGER)) "
            "END"
            if has_value_raw
            else "TRY_CAST(value AS INTEGER)"
        )
        respondent_filter = (
            "AND respondent_id IN (SELECT respondent_id FROM filtered_respondents)"
            if respondent_cte
            else ""
        )
        cte_prefix = f"{respondent_cte}," if respondent_cte else ""
        query = f"""
            WITH {cte_prefix}
            base AS (
                SELECT
                    LOWER(stage) AS stage,
                    brand,
                    respondent_id,
                    {value_expr} AS v_int
                FROM journey_table
                WHERE study_id = ?
                  AND brand IS NOT NULL
                  AND TRIM(CAST(brand AS VARCHAR)) <> ''
                  {respondent_filter}
            ),
            population AS (
                SELECT
                    COUNT(DISTINCT respondent_id) AS population_n
                FROM (
                    SELECT respondent_id
                    FROM journey_table
                    WHERE study_id = ?
                      AND respondent_id IS NOT NULL
                      {respondent_filter}
                ) pop
            ),
            stage_stats AS (
                SELECT stage, MAX(v_int) AS max_v
                FROM base
                WHERE v_int IS NOT NULL
                GROUP BY stage
            ),
            stage_nums AS (
                SELECT
                    b.brand,
                    COUNT(DISTINCT CASE WHEN b.stage = 'awareness' AND b.v_int = 1 THEN b.respondent_id END) AS awareness_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'awareness' THEN b.respondent_id END) AS awareness_denom,
                    COUNT(DISTINCT CASE WHEN b.stage = 'ad_awareness' AND b.v_int = 1 THEN b.respondent_id END) AS ad_awareness_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'ad_awareness' THEN b.respondent_id END) AS ad_awareness_denom,
                    COUNT(DISTINCT CASE WHEN b.stage = 'consideration' AND b.v_int = 1 THEN b.respondent_id END) AS consideration_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'consideration' THEN b.respondent_id END) AS consideration_denom,
                    COUNT(DISTINCT CASE WHEN b.stage = 'purchase' AND b.v_int = 1 THEN b.respondent_id END) AS purchase_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'purchase' THEN b.respondent_id END) AS purchase_denom,
                    COUNT(
                        DISTINCT CASE
                            WHEN b.stage = 'satisfaction'
                                 AND (
                                    (ss.max_v IS NOT NULL AND ss.max_v >= 5 AND b.v_int IN (4, 5))
                                    OR (ss.max_v IS NOT NULL AND ss.max_v < 5 AND b.v_int = 1)
                                 )
                            THEN b.respondent_id
                        END
                    ) AS satisfaction_num,
                    COUNT(
                        DISTINCT CASE
                            WHEN b.stage = 'satisfaction'
                                 AND ss.max_v IS NOT NULL
                                 AND ss.max_v >= 5
                                 AND b.v_int IN (1, 2)
                            THEN b.respondent_id
                        END
                    ) AS satisfaction_bottom2_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'satisfaction' THEN b.respondent_id END) AS satisfaction_denom,
                    COUNT(
                        DISTINCT CASE
                            WHEN b.stage = 'recommendation'
                                 AND (
                                    (rs.max_v IS NOT NULL AND rs.max_v >= 9 AND b.v_int IN (9, 10))
                                    OR (rs.max_v IS NOT NULL AND rs.max_v < 9 AND b.v_int = 1)
                                 )
                            THEN b.respondent_id
                        END
                    ) AS recommendation_num,
                    COUNT(
                        DISTINCT CASE
                            WHEN b.stage = 'recommendation'
                                 AND (
                                    (rs.max_v IS NOT NULL AND rs.max_v >= 9 AND b.v_int BETWEEN 0 AND 6)
                                    OR (rs.max_v IS NOT NULL AND rs.max_v < 9 AND b.v_int = 0)
                                 )
                            THEN b.respondent_id
                        END
                    ) AS recommendation_detractors_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'recommendation' THEN b.respondent_id END) AS recommendation_denom
                FROM base b
                LEFT JOIN stage_stats ss ON ss.stage = 'satisfaction'
                LEFT JOIN stage_stats rs ON rs.stage = 'recommendation'
                WHERE b.v_int IS NOT NULL
                GROUP BY b.brand
            ),
            purchasers AS (
                SELECT brand, respondent_id
                FROM base
                WHERE stage = 'purchase' AND v_int = 1 AND respondent_id IS NOT NULL
                GROUP BY brand, respondent_id
            ),
            satisfaction_by_resp AS (
                SELECT brand, respondent_id, MAX(v_int) AS sat_v
                FROM base
                WHERE stage = 'satisfaction' AND v_int IS NOT NULL AND respondent_id IS NOT NULL
                GROUP BY brand, respondent_id
            ),
            recommendation_by_resp AS (
                SELECT brand, respondent_id, MAX(v_int) AS rec_v
                FROM base
                WHERE stage = 'recommendation' AND v_int IS NOT NULL AND respondent_id IS NOT NULL
                GROUP BY brand, respondent_id
            ),
            experience AS (
                SELECT
                    p.brand,
                    ss.max_v AS sat_scale_max,
                    rs.max_v AS rec_scale_max,
                    COUNT(DISTINCT p.respondent_id) AS purchase_n,
                    COUNT(
                        DISTINCT CASE
                            WHEN ss.max_v IS NOT NULL AND ss.max_v >= 5 AND s.sat_v IN (4, 5) THEN p.respondent_id
                            WHEN ss.max_v IS NOT NULL AND ss.max_v < 5 AND s.sat_v = 1 THEN p.respondent_id
                            ELSE NULL
                        END
                    ) AS top2_n,
                    COUNT(
                        DISTINCT CASE
                            WHEN ss.max_v IS NOT NULL AND ss.max_v >= 5 AND s.sat_v IN (1, 2) THEN p.respondent_id
                            ELSE NULL
                        END
                    ) AS bottom2_n,
                    COUNT(
                        DISTINCT CASE
                            WHEN rs.max_v IS NOT NULL AND rs.max_v >= 9 AND r.rec_v IN (9, 10) THEN p.respondent_id
                            WHEN rs.max_v IS NOT NULL AND rs.max_v < 9 AND r.rec_v = 1 THEN p.respondent_id
                            ELSE NULL
                        END
                    ) AS promoters_n,
                    COUNT(
                        DISTINCT CASE
                            WHEN rs.max_v IS NOT NULL AND rs.max_v >= 9 AND r.rec_v BETWEEN 0 AND 6 THEN p.respondent_id
                            WHEN rs.max_v IS NOT NULL AND rs.max_v < 9 AND r.rec_v = 0 THEN p.respondent_id
                            ELSE NULL
                        END
                    ) AS detractors_n
                FROM purchasers p
                LEFT JOIN satisfaction_by_resp s
                  ON s.brand = p.brand AND s.respondent_id = p.respondent_id
                LEFT JOIN recommendation_by_resp r
                  ON r.brand = p.brand AND r.respondent_id = p.respondent_id
                LEFT JOIN stage_stats ss ON ss.stage = 'satisfaction'
                LEFT JOIN stage_stats rs ON rs.stage = 'recommendation'
                GROUP BY p.brand, ss.max_v, rs.max_v
            ),
            brands AS (
                SELECT brand FROM stage_nums
                UNION
                SELECT brand FROM experience
            )
            SELECT
                b.brand,
                p.population_n,
                s.awareness_num,
                s.awareness_denom,
                s.ad_awareness_num,
                s.ad_awareness_denom,
                s.consideration_num,
                s.consideration_denom,
                s.purchase_num,
                s.purchase_denom,
                s.satisfaction_num,
                s.satisfaction_bottom2_num,
                s.satisfaction_denom,
                s.recommendation_num,
                s.recommendation_detractors_num,
                s.recommendation_denom,
                e.sat_scale_max,
                e.rec_scale_max,
                e.purchase_n,
                e.top2_n,
                e.bottom2_n,
                e.promoters_n,
                e.detractors_n
            FROM brands b
            CROSS JOIN population p
            LEFT JOIN stage_nums s ON s.brand = b.brand
            LEFT JOIN experience e ON e.brand = b.brand
        """
        params = [*respondent_params, study_id, study_id]
        rows = conn.execute(query, params).fetchall()
        if not rows:
            if strict_missing:
                raise HTTPException(status_code=404, detail=f"No curated data for {study_id}.")
            return []

        result_rows: list[dict] = []
        for (
            brand,
            population_n,
            awareness_num,
            awareness_denom,
            ad_awareness_num,
            ad_awareness_denom,
            consideration_num,
            consideration_denom,
            purchase_num,
            purchase_denom,
            satisfaction_num,
            satisfaction_bottom2_num,
            satisfaction_denom,
            recommendation_num,
            recommendation_detractors_num,
            recommendation_denom,
            sat_scale_max,
            rec_scale_max,
            purchaser_n,
            top2_n,
            bottom2_n,
            promoters_n,
            detractors_n,
        ) in rows:
            if not brand:
                continue
            values: dict[str, float | None] = {value: None for value in TABLE_STAGE_MAP.values()}
            values["base_n_population"] = population_n if population_n and population_n > 0 else None
            values["aggregation_weight_n"] = population_n if population_n and population_n > 0 else None

            if population_n and population_n > 0:
                numerator_map = {
                    "brand_awareness": awareness_num,
                    "ad_awareness": ad_awareness_num,
                    "brand_consideration": consideration_num,
                    "brand_purchase": purchase_num,
                    "brand_satisfaction": satisfaction_num,
                    "brand_recommendation": recommendation_num,
                }
                denominator_map = {
                    "brand_awareness": awareness_denom,
                    "ad_awareness": ad_awareness_denom,
                    "brand_consideration": consideration_denom,
                    "brand_purchase": purchase_denom,
                    "brand_satisfaction": satisfaction_denom,
                    "brand_recommendation": recommendation_denom,
                }
                for metric, numerator in numerator_map.items():
                    stage_denom = denominator_map.get(metric)
                    if not stage_denom or stage_denom <= 0 or numerator is None:
                        continue
                    values[metric] = round((float(numerator) / float(population_n)) * 100, 1)

            values["csat"] = None
            values["nps"] = None
            if purchaser_n and purchaser_n >= MIN_EXPERIENCE_BASE_N:
                # Only compute derived metrics when their source stage exists in this study slice.
                if sat_scale_max is not None:
                    values["csat"] = round(
                        ((float(top2_n or 0) - float(bottom2_n or 0)) / float(purchaser_n)) * 100, 1
                    )
                # Only compute NPS when recommendation scale is NPS-like (0-10 or 1-10).
                if rec_scale_max is not None and rec_scale_max >= 9:
                    values["nps"] = round(
                        ((float(promoters_n or 0) - float(detractors_n or 0)) / float(purchaser_n)) * 100, 1
                    )
            # Fallback when purchase-conditioned base is unavailable but stage data is present.
            if (
                values["csat"] is None
                and satisfaction_denom
                and satisfaction_denom >= MIN_EXPERIENCE_BASE_N
            ):
                values["csat"] = round(
                    ((float(satisfaction_num or 0) - float(satisfaction_bottom2_num or 0)) / float(satisfaction_denom))
                    * 100,
                    1,
                )
            if (
                values["nps"] is None
                and recommendation_denom
                and recommendation_denom >= MIN_EXPERIENCE_BASE_N
            ):
                values["nps"] = round(
                    (
                        (float(recommendation_num or 0) - float(recommendation_detractors_num or 0))
                        / float(recommendation_denom)
                    )
                    * 100,
                    1,
                )

            awareness_ceiling_applied = _apply_awareness_ceiling(values)
            if not _has_brand_signal(values):
                continue
            result_rows.append(
                {
                    "brand": brand,
                    "brand_awareness": values.get("brand_awareness"),
                    "ad_awareness": values.get("ad_awareness"),
                    "brand_consideration": values.get("brand_consideration"),
                    "brand_purchase": values.get("brand_purchase"),
                    "brand_satisfaction": values.get("brand_satisfaction"),
                    "brand_recommendation": values.get("brand_recommendation"),
                    "csat": values.get("csat"),
                    "nps": values.get("nps"),
                    "base_n_population": values.get("base_n_population"),
                    "aggregation_weight_n": values.get("aggregation_weight_n"),
                    "awareness_ceiling_applied": awareness_ceiling_applied,
                    "brand_consideration_imputed": None,
                    "brand_consideration_source": "observed" if values.get("brand_consideration") is not None else "none",
                    "brand_consideration_impute_level": "none",
                    "brand_consideration_impute_version": None,
                    "brand_satisfaction_imputed": None,
                    "brand_satisfaction_source": "observed" if values.get("brand_satisfaction") is not None else "none",
                    "brand_satisfaction_impute_level": "none",
                    "brand_satisfaction_impute_version": None,
                    "csat_imputed": None,
                    "csat_source": "observed" if values.get("csat") is not None else "none",
                    "csat_impute_level": "none",
                    "csat_impute_version": None,
                }
            )

        if apply_consideration_imputation and result_rows:
            _apply_consideration_imputation_to_rows(
                result_rows,
                market_sector=market_sector,
                market_subsector=market_subsector,
                market_category=market_category,
            )
        if apply_satisfaction_imputation and result_rows:
            _apply_satisfaction_imputation_to_rows(
                result_rows,
                market_sector=market_sector,
                market_subsector=market_subsector,
                market_category=market_category,
            )
        if apply_csat_imputation and result_rows:
            _apply_csat_imputation_to_rows(
                result_rows,
                market_sector=market_sector,
                market_subsector=market_subsector,
                market_category=market_category,
            )

        return result_rows
    finally:
        conn.close()


def _compute_table_rows(study_id: str) -> list[dict]:
    return _compute_table_rows_internal(study_id, None, [], True)


def _compute_table_rows_filtered(study_id: str, filters: dict) -> list[dict]:
    respondent_cte, respondent_params, eligible = _respondent_filter_cte(study_id, filters)
    if not eligible:
        return []
    return _compute_table_rows_internal(study_id, respondent_cte, respondent_params, False)


def _compute_table_rows_by_quarter_filtered(study_id: str, filters: dict) -> dict[int, list[dict]]:
    respondent_cte, respondent_params, eligible = _respondent_filter_cte(study_id, filters)
    if not eligible:
        return {}

    root = get_repo_root()
    classification = _classification_for_study(root, study_id)
    market_sector = _as_non_empty_text(classification.get("market_sector"))
    market_subsector = _as_non_empty_text(classification.get("market_subsector"))
    market_category = _normalize_market_category(classification.get("market_category"))
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if not curated_path.exists():
        return {}
    respondents_path = (
        root / "data" / "warehouse" / "raw" / f"study_id={study_id}" / "respondents.parquet"
    )

    conn = get_duckdb_connection()
    try:
        load_parquet_as_view(conn, "journey_table", str(curated_path))
        if respondent_cte is None:
            if not respondents_path.exists():
                return {}
            respondent_columns = _parquet_columns(respondents_path)
            if "respondent_id" not in respondent_columns or "date" not in respondent_columns:
                return {}
            respondent_cte = f"""
                filtered_respondents AS (
                    SELECT
                        respondent_id,
                        EXTRACT(year FROM TRY_CAST(date AS DATE)) * 10
                            + EXTRACT(quarter FROM TRY_CAST(date AS DATE)) AS q_key
                    FROM read_parquet('{respondents_path}')
                    WHERE respondent_id IS NOT NULL
                      AND TRY_CAST(date AS DATE) IS NOT NULL
                )
            """
            respondent_params = []
        has_value_raw = (
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'journey_table' AND column_name = 'value_raw'
                """
            ).fetchone()[0]
            > 0
        )
        value_expr = (
            "CASE "
            "WHEN LOWER(stage) IN ('satisfaction', 'recommendation') "
            "THEN TRY_CAST(value_raw AS INTEGER) "
            "ELSE COALESCE(TRY_CAST(value AS INTEGER), TRY_CAST(value_raw AS INTEGER)) "
            "END"
            if has_value_raw
            else "TRY_CAST(value AS INTEGER)"
        )
        with_prefix = _with_clause(
            respondent_cte,
            f"""
            base AS (
                SELECT
                    fr.q_key AS q_key,
                    LOWER(j.stage) AS stage,
                    j.brand,
                    j.respondent_id,
                    {value_expr} AS v_int
                FROM journey_table j
                JOIN filtered_respondents fr ON fr.respondent_id = j.respondent_id
                WHERE j.study_id = ?
                  AND j.brand IS NOT NULL
                  AND TRIM(CAST(j.brand AS VARCHAR)) <> ''
            ),
            population AS (
                SELECT q_key, COUNT(DISTINCT respondent_id) AS population_n
                FROM filtered_respondents
                WHERE q_key IS NOT NULL
                GROUP BY q_key
            ),
            stage_stats AS (
                SELECT q_key, stage, MAX(v_int) AS max_v
                FROM base
                WHERE v_int IS NOT NULL
                GROUP BY q_key, stage
            ),
            stage_nums AS (
                SELECT
                    b.q_key,
                    b.brand,
                    COUNT(DISTINCT CASE WHEN b.stage = 'awareness' AND b.v_int = 1 THEN b.respondent_id END) AS awareness_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'awareness' THEN b.respondent_id END) AS awareness_denom,
                    COUNT(DISTINCT CASE WHEN b.stage = 'ad_awareness' AND b.v_int = 1 THEN b.respondent_id END) AS ad_awareness_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'ad_awareness' THEN b.respondent_id END) AS ad_awareness_denom,
                    COUNT(DISTINCT CASE WHEN b.stage = 'consideration' AND b.v_int = 1 THEN b.respondent_id END) AS consideration_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'consideration' THEN b.respondent_id END) AS consideration_denom,
                    COUNT(DISTINCT CASE WHEN b.stage = 'purchase' AND b.v_int = 1 THEN b.respondent_id END) AS purchase_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'purchase' THEN b.respondent_id END) AS purchase_denom,
                    COUNT(
                        DISTINCT CASE
                            WHEN b.stage = 'satisfaction'
                                 AND (
                                    (ss.max_v IS NOT NULL AND ss.max_v >= 5 AND b.v_int IN (4, 5))
                                    OR (ss.max_v IS NOT NULL AND ss.max_v < 5 AND b.v_int = 1)
                                 )
                            THEN b.respondent_id
                        END
                    ) AS satisfaction_num,
                    COUNT(
                        DISTINCT CASE
                            WHEN b.stage = 'satisfaction'
                                 AND ss.max_v IS NOT NULL
                                 AND ss.max_v >= 5
                                 AND b.v_int IN (1, 2)
                            THEN b.respondent_id
                        END
                    ) AS satisfaction_bottom2_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'satisfaction' THEN b.respondent_id END) AS satisfaction_denom,
                    COUNT(
                        DISTINCT CASE
                            WHEN b.stage = 'recommendation'
                                 AND (
                                    (rs.max_v IS NOT NULL AND rs.max_v >= 9 AND b.v_int IN (9, 10))
                                    OR (rs.max_v IS NOT NULL AND rs.max_v < 9 AND b.v_int = 1)
                                 )
                            THEN b.respondent_id
                        END
                    ) AS recommendation_num,
                    COUNT(
                        DISTINCT CASE
                            WHEN b.stage = 'recommendation'
                                 AND (
                                    (rs.max_v IS NOT NULL AND rs.max_v >= 9 AND b.v_int BETWEEN 0 AND 6)
                                    OR (rs.max_v IS NOT NULL AND rs.max_v < 9 AND b.v_int = 0)
                                 )
                            THEN b.respondent_id
                        END
                    ) AS recommendation_detractors_num,
                    COUNT(DISTINCT CASE WHEN b.stage = 'recommendation' THEN b.respondent_id END) AS recommendation_denom
                FROM base b
                LEFT JOIN stage_stats ss ON ss.q_key = b.q_key AND ss.stage = 'satisfaction'
                LEFT JOIN stage_stats rs ON rs.q_key = b.q_key AND rs.stage = 'recommendation'
                WHERE b.v_int IS NOT NULL
                GROUP BY b.q_key, b.brand
            ),
            purchasers AS (
                SELECT q_key, brand, respondent_id
                FROM base
                WHERE stage = 'purchase' AND v_int = 1 AND respondent_id IS NOT NULL
                GROUP BY q_key, brand, respondent_id
            ),
            satisfaction_by_resp AS (
                SELECT q_key, brand, respondent_id, MAX(v_int) AS sat_v
                FROM base
                WHERE stage = 'satisfaction' AND v_int IS NOT NULL AND respondent_id IS NOT NULL
                GROUP BY q_key, brand, respondent_id
            ),
            recommendation_by_resp AS (
                SELECT q_key, brand, respondent_id, MAX(v_int) AS rec_v
                FROM base
                WHERE stage = 'recommendation' AND v_int IS NOT NULL AND respondent_id IS NOT NULL
                GROUP BY q_key, brand, respondent_id
            ),
            experience AS (
                SELECT
                    p.q_key,
                    p.brand,
                    ss.max_v AS sat_scale_max,
                    rs.max_v AS rec_scale_max,
                    COUNT(DISTINCT p.respondent_id) AS purchase_n,
                    COUNT(
                        DISTINCT CASE
                            WHEN ss.max_v IS NOT NULL AND ss.max_v >= 5 AND s.sat_v IN (4, 5) THEN p.respondent_id
                            WHEN ss.max_v IS NOT NULL AND ss.max_v < 5 AND s.sat_v = 1 THEN p.respondent_id
                            ELSE NULL
                        END
                    ) AS top2_n,
                    COUNT(
                        DISTINCT CASE
                            WHEN ss.max_v IS NOT NULL AND ss.max_v >= 5 AND s.sat_v IN (1, 2) THEN p.respondent_id
                            ELSE NULL
                        END
                    ) AS bottom2_n,
                    COUNT(
                        DISTINCT CASE
                            WHEN rs.max_v IS NOT NULL AND rs.max_v >= 9 AND r.rec_v IN (9, 10) THEN p.respondent_id
                            WHEN rs.max_v IS NOT NULL AND rs.max_v < 9 AND r.rec_v = 1 THEN p.respondent_id
                            ELSE NULL
                        END
                    ) AS promoters_n,
                    COUNT(
                        DISTINCT CASE
                            WHEN rs.max_v IS NOT NULL AND rs.max_v >= 9 AND r.rec_v BETWEEN 0 AND 6 THEN p.respondent_id
                            WHEN rs.max_v IS NOT NULL AND rs.max_v < 9 AND r.rec_v = 0 THEN p.respondent_id
                            ELSE NULL
                        END
                    ) AS detractors_n
                FROM purchasers p
                LEFT JOIN satisfaction_by_resp s
                  ON s.q_key = p.q_key AND s.brand = p.brand AND s.respondent_id = p.respondent_id
                LEFT JOIN recommendation_by_resp r
                  ON r.q_key = p.q_key AND r.brand = p.brand AND r.respondent_id = p.respondent_id
                LEFT JOIN stage_stats ss ON ss.q_key = p.q_key AND ss.stage = 'satisfaction'
                LEFT JOIN stage_stats rs ON rs.q_key = p.q_key AND rs.stage = 'recommendation'
                GROUP BY p.q_key, p.brand, ss.max_v, rs.max_v
            ),
            brands AS (
                SELECT q_key, brand FROM stage_nums
                UNION
                SELECT q_key, brand FROM experience
            )
            """,
        )
        query = f"""
            {with_prefix}
            SELECT
                b.q_key,
                b.brand,
                p.population_n,
                s.awareness_num,
                s.awareness_denom,
                s.ad_awareness_num,
                s.ad_awareness_denom,
                s.consideration_num,
                s.consideration_denom,
                s.purchase_num,
                s.purchase_denom,
                s.satisfaction_num,
                s.satisfaction_bottom2_num,
                s.satisfaction_denom,
                s.recommendation_num,
                s.recommendation_detractors_num,
                s.recommendation_denom,
                e.sat_scale_max,
                e.rec_scale_max,
                e.purchase_n,
                e.top2_n,
                e.bottom2_n,
                e.promoters_n,
                e.detractors_n
            FROM brands b
            LEFT JOIN population p ON p.q_key = b.q_key
            LEFT JOIN stage_nums s ON s.q_key = b.q_key AND s.brand = b.brand
            LEFT JOIN experience e ON e.q_key = b.q_key AND e.brand = b.brand
        """
        params = [*respondent_params, study_id]
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    buckets: dict[int, list[dict]] = {}
    for (
        q_key,
        brand,
        population_n,
        awareness_num,
        awareness_denom,
        ad_awareness_num,
        ad_awareness_denom,
        consideration_num,
        consideration_denom,
        purchase_num,
        purchase_denom,
        satisfaction_num,
        satisfaction_bottom2_num,
        satisfaction_denom,
        recommendation_num,
        recommendation_detractors_num,
        recommendation_denom,
        sat_scale_max,
        rec_scale_max,
        purchaser_n,
        top2_n,
        bottom2_n,
        promoters_n,
        detractors_n,
    ) in rows:
        if q_key is None or not brand:
            continue
        q_key_int = int(q_key)
        values: dict[str, float | None] = {value: None for value in TABLE_STAGE_MAP.values()}
        values["base_n_population"] = population_n if population_n and population_n > 0 else None
        values["aggregation_weight_n"] = population_n if population_n and population_n > 0 else None

        if population_n and population_n > 0:
            numerator_map = {
                "brand_awareness": awareness_num,
                "ad_awareness": ad_awareness_num,
                "brand_consideration": consideration_num,
                "brand_purchase": purchase_num,
                "brand_satisfaction": satisfaction_num,
                "brand_recommendation": recommendation_num,
            }
            denominator_map = {
                "brand_awareness": awareness_denom,
                "ad_awareness": ad_awareness_denom,
                "brand_consideration": consideration_denom,
                "brand_purchase": purchase_denom,
                "brand_satisfaction": satisfaction_denom,
                "brand_recommendation": recommendation_denom,
            }
            for metric, numerator in numerator_map.items():
                stage_denom = denominator_map.get(metric)
                if not stage_denom or stage_denom <= 0 or numerator is None:
                    continue
                values[metric] = round((float(numerator) / float(population_n)) * 100, 1)

        values["csat"] = None
        values["nps"] = None
        if purchaser_n and purchaser_n >= MIN_EXPERIENCE_BASE_N:
            if sat_scale_max is not None:
                values["csat"] = round(((float(top2_n or 0) - float(bottom2_n or 0)) / float(purchaser_n)) * 100, 1)
            # Only compute NPS when recommendation scale is NPS-like (0-10 or 1-10).
            if rec_scale_max is not None and rec_scale_max >= 9:
                values["nps"] = round(((float(promoters_n or 0) - float(detractors_n or 0)) / float(purchaser_n)) * 100, 1)
        # Fallback when purchase-conditioned base is unavailable but stage data is present.
        if (
            values["csat"] is None
            and satisfaction_denom
            and satisfaction_denom >= MIN_EXPERIENCE_BASE_N
        ):
            values["csat"] = round(
                ((float(satisfaction_num or 0) - float(satisfaction_bottom2_num or 0)) / float(satisfaction_denom))
                * 100,
                1,
            )
        if (
            values["nps"] is None
            and recommendation_denom
            and recommendation_denom >= MIN_EXPERIENCE_BASE_N
        ):
            values["nps"] = round(
                (
                    (float(recommendation_num or 0) - float(recommendation_detractors_num or 0))
                    / float(recommendation_denom)
                )
                * 100,
                1,
            )

        awareness_ceiling_applied = _apply_awareness_ceiling(values)
        if not _has_brand_signal(values):
            continue
        buckets.setdefault(q_key_int, []).append(
            {
                "brand": brand,
                "brand_awareness": values.get("brand_awareness"),
                "ad_awareness": values.get("ad_awareness"),
                "brand_consideration": values.get("brand_consideration"),
                "brand_purchase": values.get("brand_purchase"),
                "brand_satisfaction": values.get("brand_satisfaction"),
                "brand_recommendation": values.get("brand_recommendation"),
                "csat": values.get("csat"),
                "nps": values.get("nps"),
                "base_n_population": values.get("base_n_population"),
                "aggregation_weight_n": values.get("aggregation_weight_n"),
                "awareness_ceiling_applied": awareness_ceiling_applied,
                "brand_consideration_imputed": None,
                "brand_consideration_source": "observed" if values.get("brand_consideration") is not None else "none",
                "brand_consideration_impute_level": "none",
                "brand_consideration_impute_version": None,
                "brand_satisfaction_imputed": None,
                "brand_satisfaction_source": "observed" if values.get("brand_satisfaction") is not None else "none",
                "brand_satisfaction_impute_level": "none",
                "brand_satisfaction_impute_version": None,
                "csat_imputed": None,
                "csat_source": "observed" if values.get("csat") is not None else "none",
                "csat_impute_level": "none",
                "csat_impute_version": None,
            }
        )

    if buckets:
        for quarter_rows in buckets.values():
            _apply_consideration_imputation_to_rows(
                quarter_rows,
                market_sector=market_sector,
                market_subsector=market_subsector,
                market_category=market_category,
            )
            _apply_satisfaction_imputation_to_rows(
                quarter_rows,
                market_sector=market_sector,
                market_subsector=market_subsector,
                market_category=market_category,
            )
            _apply_csat_imputation_to_rows(
                quarter_rows,
                market_sector=market_sector,
                market_subsector=market_subsector,
                market_category=market_category,
            )
    return buckets


def _compute_touchpoint_rows(study_id: str) -> list[dict]:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if not curated_path.exists():
        return []

    conn = get_duckdb_connection()
    try:
        load_parquet_as_view(conn, "touchpoints_table", str(curated_path))
        has_touchpoint = (
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'touchpoints_table' AND column_name = 'touchpoint'
                """
            ).fetchone()[0]
            > 0
        )
        if not has_touchpoint:
            return []

        query = f"""
        WITH base AS (
            SELECT
                LOWER(stage) AS stage,
                brand,
                touchpoint,
                respondent_id,
                TRY_CAST(value AS INTEGER) AS v_int
            FROM touchpoints_table
            WHERE study_id = ?
              AND LOWER(stage) IN ('touchpoints', 'awareness', 'consideration', 'brand_consideration', 'purchase', 'brand_purchase')
              AND touchpoint IS NOT NULL
              AND TRIM(CAST(touchpoint AS VARCHAR)) <> ''
              AND brand IS NOT NULL
              AND TRIM(CAST(brand AS VARCHAR)) <> ''
              AND TRY_CAST(value AS INTEGER) IS NOT NULL
        ),
        nums AS (
            SELECT stage, brand, touchpoint, COUNT(DISTINCT respondent_id) AS num
            FROM base
            WHERE v_int = 1
            GROUP BY stage, brand, touchpoint
        ),
        denoms AS (
            SELECT stage, brand, touchpoint, COUNT(DISTINCT respondent_id) AS denom
            FROM base
            GROUP BY stage, brand, touchpoint
        )
        SELECT
            d.stage,
            d.brand,
            d.touchpoint,
            n.num,
            d.denom
        FROM denoms d
        LEFT JOIN nums n
            ON n.stage = d.stage AND n.brand = d.brand AND n.touchpoint = d.touchpoint
        """
        rows = conn.execute(query, [study_id]).fetchall()
        if not rows:
            return []

        values_by_pair: dict[tuple[str, str], dict[str, float | None]] = {}
        for stage, brand, touchpoint, num, denom in rows:
            metric = TOUCHPOINT_STAGE_METRICS.get(str(stage).lower())
            if not metric:
                continue
            key = (brand, touchpoint)
            if key not in values_by_pair:
                values_by_pair[key] = {"recall": None, "consideration": None, "purchase": None}
            value = None
            if denom and denom > 0:
                value = round((float(num or 0) / denom) * 100, 1)
            values_by_pair[key][metric] = value

        result_rows = []
        for (brand, touchpoint), values in values_by_pair.items():
            result_rows.append(
                {
                    "brand": brand,
                    "touchpoint": touchpoint,
                    "recall": values.get("recall"),
                    "consideration": values.get("consideration"),
                    "purchase": values.get("purchase"),
                }
            )
        return result_rows
    finally:
        conn.close()


def _compute_touchpoint_rows_filtered(study_id: str, filters: dict) -> list[dict]:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if not curated_path.exists():
        return []

    respondent_cte, respondent_params, eligible = _respondent_filter_cte(study_id, filters)
    if not eligible:
        return []

    conn = get_duckdb_connection()
    try:
        load_parquet_as_view(conn, "touchpoints_table", str(curated_path))
        has_touchpoint = (
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'touchpoints_table' AND column_name = 'touchpoint'
                """
            ).fetchone()[0]
            > 0
        )
        if not has_touchpoint:
            return []

        has_value_raw = (
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'touchpoints_table' AND column_name = 'value_raw'
                """
            ).fetchone()[0]
            > 0
        )
        value_expr = (
            "CASE "
            "WHEN LOWER(stage) IN ('satisfaction', 'recommendation') "
            "THEN TRY_CAST(value_raw AS INTEGER) "
            "ELSE COALESCE(TRY_CAST(value AS INTEGER), TRY_CAST(value_raw AS INTEGER)) "
            "END"
            if has_value_raw
            else "TRY_CAST(value AS INTEGER)"
        )

        respondent_filter = (
            "AND respondent_id IN (SELECT respondent_id FROM filtered_respondents)"
            if respondent_cte
            else ""
        )
        cte_prefix = f"{respondent_cte}," if respondent_cte else ""
        query = f"""
        WITH {cte_prefix}
        base AS (
            SELECT
                LOWER(stage) AS stage,
                brand,
                touchpoint,
                respondent_id,
                {value_expr} AS v_int
            FROM touchpoints_table
            WHERE study_id = ?
              AND LOWER(stage) IN ('touchpoints', 'awareness', 'consideration', 'brand_consideration', 'purchase', 'brand_purchase')
              AND touchpoint IS NOT NULL
              AND TRIM(CAST(touchpoint AS VARCHAR)) <> ''
              AND brand IS NOT NULL
              AND TRIM(CAST(brand AS VARCHAR)) <> ''
              AND {value_expr} IS NOT NULL
              {respondent_filter}
        ),
        nums AS (
            SELECT stage, brand, touchpoint, COUNT(DISTINCT respondent_id) AS num
            FROM base
            WHERE v_int = 1
            GROUP BY stage, brand, touchpoint
        ),
        denoms AS (
            SELECT stage, brand, touchpoint, COUNT(DISTINCT respondent_id) AS denom
            FROM base
            GROUP BY stage, brand, touchpoint
        )
        SELECT
            d.stage,
            d.brand,
            d.touchpoint,
            n.num,
            d.denom
        FROM denoms d
        LEFT JOIN nums n
            ON n.stage = d.stage AND n.brand = d.brand AND n.touchpoint = d.touchpoint
        """
        params = [*respondent_params, study_id]
        rows = conn.execute(query, params).fetchall()
        if not rows:
            return []

        values_by_pair: dict[tuple[str, str], dict[str, float | None]] = {}
        for stage, brand, touchpoint, num, denom in rows:
            metric = TOUCHPOINT_STAGE_METRICS.get(str(stage).lower())
            if not metric:
                continue
            key = (brand, touchpoint)
            if key not in values_by_pair:
                values_by_pair[key] = {"recall": None, "consideration": None, "purchase": None}
            value = None
            if denom and denom > 0:
                value = round((float(num or 0) / denom) * 100, 1)
            values_by_pair[key][metric] = value

        result_rows = []
        for (brand, touchpoint), values in values_by_pair.items():
            result_rows.append(
                {
                    "brand": brand,
                    "touchpoint": touchpoint,
                    "recall": values.get("recall"),
                    "consideration": values.get("consideration"),
                    "purchase": values.get("purchase"),
                }
            )
        return result_rows
    finally:
        conn.close()


def _compute_touchpoint_rows_by_quarter_filtered(study_id: str, filters: dict) -> dict[int, list[dict]]:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if not curated_path.exists():
        return {}

    respondent_cte, respondent_params, eligible = _respondent_filter_cte(study_id, filters)
    if not eligible:
        return {}
    respondents_path = (
        root / "data" / "warehouse" / "raw" / f"study_id={study_id}" / "respondents.parquet"
    )

    conn = get_duckdb_connection()
    try:
        load_parquet_as_view(conn, "touchpoints_table", str(curated_path))
        if respondent_cte is None:
            if not respondents_path.exists():
                return {}
            respondent_columns = _parquet_columns(respondents_path)
            if "respondent_id" not in respondent_columns or "date" not in respondent_columns:
                return {}
            respondent_cte = f"""
                filtered_respondents AS (
                    SELECT
                        respondent_id,
                        EXTRACT(year FROM TRY_CAST(date AS DATE)) * 10
                            + EXTRACT(quarter FROM TRY_CAST(date AS DATE)) AS q_key
                    FROM read_parquet('{respondents_path}')
                    WHERE respondent_id IS NOT NULL
                      AND TRY_CAST(date AS DATE) IS NOT NULL
                )
            """
            respondent_params = []
        has_touchpoint = (
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'touchpoints_table' AND column_name = 'touchpoint'
                """
            ).fetchone()[0]
            > 0
        )
        if not has_touchpoint:
            return {}

        has_value_raw = (
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'touchpoints_table' AND column_name = 'value_raw'
                """
            ).fetchone()[0]
            > 0
        )
        value_expr = (
            "CASE "
            "WHEN LOWER(stage) IN ('satisfaction', 'recommendation') "
            "THEN TRY_CAST(value_raw AS INTEGER) "
            "ELSE COALESCE(TRY_CAST(value AS INTEGER), TRY_CAST(value_raw AS INTEGER)) "
            "END"
            if has_value_raw
            else "TRY_CAST(value AS INTEGER)"
        )

        with_prefix = _with_clause(
            respondent_cte,
            f"""
        base AS (
            SELECT
                fr.q_key AS q_key,
                LOWER(t.stage) AS stage,
                t.brand,
                t.touchpoint,
                t.respondent_id,
                {value_expr} AS v_int
            FROM touchpoints_table t
            JOIN filtered_respondents fr ON fr.respondent_id = t.respondent_id
            WHERE t.study_id = ?
              AND LOWER(t.stage) IN ('touchpoints', 'awareness', 'consideration', 'brand_consideration', 'purchase', 'brand_purchase')
              AND t.touchpoint IS NOT NULL
              AND TRIM(CAST(t.touchpoint AS VARCHAR)) <> ''
              AND t.brand IS NOT NULL
              AND TRIM(CAST(t.brand AS VARCHAR)) <> ''
              AND {value_expr} IS NOT NULL
              AND fr.q_key IS NOT NULL
        ),
        nums AS (
            SELECT q_key, stage, brand, touchpoint, COUNT(DISTINCT respondent_id) AS num
            FROM base
            WHERE v_int = 1
            GROUP BY q_key, stage, brand, touchpoint
        ),
        denoms AS (
            SELECT q_key, stage, brand, touchpoint, COUNT(DISTINCT respondent_id) AS denom
            FROM base
            GROUP BY q_key, stage, brand, touchpoint
        )
        """,
        )
        query = f"""
        {with_prefix}
        SELECT
            d.q_key,
            d.stage,
            d.brand,
            d.touchpoint,
            n.num,
            d.denom
        FROM denoms d
        LEFT JOIN nums n
            ON n.q_key = d.q_key AND n.stage = d.stage AND n.brand = d.brand AND n.touchpoint = d.touchpoint
        """
        params = [*respondent_params, study_id]
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    values_by_quarter_pair: dict[tuple[int, str, str], dict[str, float | None]] = {}
    for q_key, stage, brand, touchpoint, num, denom in rows:
        if q_key is None:
            continue
        metric = TOUCHPOINT_STAGE_METRICS.get(str(stage).lower())
        if not metric:
            continue
        key = (int(q_key), str(brand), str(touchpoint))
        if key not in values_by_quarter_pair:
            values_by_quarter_pair[key] = {"recall": None, "consideration": None, "purchase": None}
        value = None
        if denom and denom > 0:
            value = round((float(num or 0) / denom) * 100, 1)
        values_by_quarter_pair[key][metric] = value

    buckets: dict[int, list[dict]] = {}
    for (q_key, brand, touchpoint), values in values_by_quarter_pair.items():
        buckets.setdefault(q_key, []).append(
            {
                "brand": brand,
                "touchpoint": touchpoint,
                "recall": values.get("recall"),
                "consideration": values.get("consideration"),
                "purchase": values.get("purchase"),
            }
        )
    return buckets


@router.get("/journey", response_model=JourneyResponse)
def journey_analytics(study_id: str = Query(..., description="Study id")) -> JourneyResponse:
    root = get_repo_root()
    curated_path = root / "data" / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    if curated_path.exists():
        parquet_path = curated_path
        source = "curated"
    else:
        raise HTTPException(status_code=404, detail="Curated mart not found for study.")

    conn = get_duckdb_connection()
    try:
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
    finally:
        conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No data available for {study_id}")

    data = [JourneyPoint(stage=row[0], brand=row[1], percentage=float(row[2])) for row in rows]
    return JourneyResponse(study_id=study_id, points=data, source=source)


@router.get("/journey/table")
def journey_table(study_id: str = Query(..., description="Study id")) -> dict:
    classification = _classification_for_study(get_repo_root(), study_id)
    result_rows = _compute_table_rows(study_id)

    has_awareness = any(row.get("brand_awareness") is not None for row in result_rows)
    if has_awareness:
        result_rows.sort(key=lambda row: row.get("brand_awareness") or -1, reverse=True)
    else:
        result_rows.sort(key=lambda row: row.get("brand") or "")

    return {
        "study_id": study_id,
        "classification": classification,
        "source": "curated",
        "rows": [
            {
                "sector": classification.get("sector"),
                "subsector": classification.get("subsector"),
                "category": classification.get("category"),
                **row,
            }
            for row in result_rows
        ],
        "notes": {
            "brand_satisfaction_definition": "% of values 4 or 5",
            "csat_definition": "(% values 4-5 - % values 1-2) among purchasers",
            "brand_recommendation_definition": "% of values 9 or 10",
            "nps_definition": "(% values 9-10 - % values 0-6) among purchasers",
            "quality_guardrail": "Brand Consideration/Purchase/Satisfaction/Recommendation are capped at Brand Awareness per study-brand.",
        },
    }


def _journey_table_multi_filtered(
    filters: dict,
    limit_mode: str,
    sort_by: str,
    sort_dir: str,
    include_global_benchmark: bool = False,
    response_mode: str = "full",
) -> dict:
    started_at = time.perf_counter()
    normalized_limit = limit_mode.lower()
    sort_metric = sort_by if sort_by in SORTABLE_METRICS else "brand_awareness"
    normalized_mode = response_mode.lower()
    if normalized_mode not in {"benchmark_global", "benchmark_selection", "full"}:
        normalized_mode = "full"
    if normalized_mode != "full":
        include_global_benchmark = False
    cache_key = _journey_table_multi_cache_key(
        filters, normalized_limit, sort_metric, sort_dir, include_global_benchmark, normalized_mode
    )
    cached = _journey_table_multi_get_cached(cache_key)
    if cached:
        cached_payload = dict(cached)
        meta = dict(cached_payload.get("meta") or {})
        meta["cache_hit"] = True
        cached_payload["meta"] = meta
        return cached_payload

    root, study_ids = _resolve_study_ids(filters)

    if not study_ids:
        payload = {
            "rows": [],
            "meta": {
                "studies_included": [],
                "limit_mode": normalized_limit,
                "sort_by": sort_metric,
                "sort_dir": sort_dir,
                "row_count": 0,
                "cache_hit": False,
                "studies_processed": 0,
                "query_ms": 0,
                "collect_ms": 0,
                "aggregate_ms": 0,
                "total_ms": round((time.perf_counter() - started_at) * 1000, 2),
            },
        }
        _journey_table_multi_set_cached(cache_key, payload)
        return payload

    classification_cache: dict[str, dict[str, str | None]] = {}
    collect_started = time.perf_counter()
    selection_rows_all: list[dict] = []
    selection_rows: list[dict] = []
    selection_studies: list[str] = []
    global_rows_all: list[dict] = []
    global_rows: list[dict] = []
    global_studies: list[str] = []
    processed = len(study_ids)

    if normalized_mode in {"benchmark_selection", "full"}:
        selection_rows_all, selection_studies, processed = _collect_journey_rows(
            root, study_ids, filters, classification_cache
        )

    if normalized_mode in {"benchmark_global"} or include_global_benchmark:
        global_filters = dict(filters)
        global_filters["sector"] = None
        global_filters["subsector"] = None
        global_filters["category"] = None
        global_rows_all, global_studies, _ = _collect_journey_rows(
            root, study_ids, global_filters, classification_cache
        )
    collect_ms = round((time.perf_counter() - collect_started) * 1000, 2)

    aggregate_started = time.perf_counter()
    if normalized_mode in {"benchmark_selection", "full"}:
        selection_rows = _sort_and_limit_journey_rows(selection_rows_all, sort_metric, sort_dir, normalized_limit)
    if normalized_mode in {"benchmark_global"} or include_global_benchmark:
        global_rows = _sort_and_limit_journey_rows(global_rows_all, sort_metric, sort_dir, normalized_limit)
    aggregate_ms = round((time.perf_counter() - aggregate_started) * 1000, 2)
    query_ms = round(collect_ms + aggregate_ms, 2)

    payload: dict = {
        "rows": selection_rows,
        "meta": {
            "studies_included": selection_studies,
            "limit_mode": normalized_limit,
            "sort_by": sort_metric,
            "sort_dir": sort_dir,
            "row_count": len(selection_rows),
            "cache_hit": False,
            "studies_processed": processed,
            "query_ms": query_ms,
            "collect_ms": collect_ms,
            "aggregate_ms": aggregate_ms,
            "total_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "response_mode": normalized_mode,
        },
    }
    if normalized_mode == "benchmark_global":
        payload["rows"] = []
        payload["summary_global"] = _build_benchmark_summary(global_rows_all, "Global Benchmark")
        payload["global_rows"] = global_rows
        payload["meta"]["global_row_count"] = len(global_rows)
        payload["meta"]["global_studies_included"] = global_studies
    elif normalized_mode == "benchmark_selection":
        payload["summary_selection"] = _build_benchmark_summary(selection_rows_all, "Selection Benchmark")
        payload["meta"]["selection_row_count"] = len(selection_rows)
        payload["meta"]["selection_studies_included"] = selection_studies
    elif include_global_benchmark:
        payload["summary_selection"] = _build_benchmark_summary(selection_rows_all, "Selection Benchmark")
        payload["summary_global"] = _build_benchmark_summary(global_rows_all, "Global Benchmark")
        payload["selection_rows"] = selection_rows
        payload["global_rows"] = global_rows
        payload["meta"]["selection_row_count"] = len(selection_rows)
        payload["meta"]["global_row_count"] = len(global_rows)
        payload["meta"]["selection_studies_included"] = selection_studies
        payload["meta"]["global_studies_included"] = global_studies

    _journey_table_multi_set_cached(cache_key, payload)
    return payload


@router.get("/journey/table_multi")
def journey_table_multi(
    studies: str | None = Query(None, description="Comma-separated study ids"),
    sector: str | None = Query(None),
    subsector: str | None = Query(None),
    category: str | None = Query(None),
    taxonomy_view: str | None = Query("market", description="market|standard"),
    limit_mode: str = Query("top10", description="top10|top25|all"),
    sort_by: str = Query("brand_awareness", description="Metric to sort by"),
    sort_dir: str = Query("desc", description="asc|desc"),
    include_global_benchmark: bool = Query(False, description="Include selection and global rows in one response"),
    response_mode: str = Query("full", description="full|benchmark_global|benchmark_selection"),
) -> dict:
    filters = _parse_filters(
        {
            "study_ids": studies,
            "sector": sector,
            "subsector": subsector,
            "category": category,
            "taxonomy_view": taxonomy_view,
        }
    )
    return _journey_table_multi_filtered(
        filters, limit_mode, sort_by, sort_dir, include_global_benchmark, response_mode
    )


@router.post("/journey/table_multi")
async def journey_table_multi_post(
    request: Request,
    limit_mode: str = Query("top10", description="top10|top25|all"),
    sort_by: str = Query("brand_awareness", description="Metric to sort by"),
    sort_dir: str = Query("desc", description="asc|desc"),
    include_global_benchmark: bool = Query(False, description="Include selection and global rows in one response"),
    response_mode: str = Query("full", description="full|benchmark_global|benchmark_selection"),
) -> dict:
    payload = await request.json()
    filters = _parse_filters(payload)
    return _journey_table_multi_filtered(
        filters, limit_mode, sort_by, sort_dir, include_global_benchmark, response_mode
    )


def _touchpoints_table_multi_filtered(
    filters: dict,
    limit_mode: str,
    sort_by: str,
    sort_dir: str,
) -> dict:
    root = get_repo_root()
    discovered = _discover_curated_studies(root)
    requested = filters.get("study_ids") or []
    if requested:
        study_ids = [study_id for study_id in requested if study_id in discovered]
    else:
        study_ids = discovered

    if not study_ids:
        return {
            "rows": [],
            "meta": {
                "studies_included": [],
                "limit_mode": limit_mode,
                "sort_by": sort_by,
                "sort_dir": sort_dir,
                "row_count": 0,
            },
        }

    rows: list[dict] = []
    for study_id in study_ids:
        classification = _classification_for_study(root, study_id)
        if not _study_matches_taxonomy(filters, classification):
            continue
        safe_classification = _effective_classification_values(classification, filters)
        filtered_rows = _compute_touchpoint_rows_filtered(study_id, filters)
        for row in filtered_rows:
            rows.append(
                {
                    "study_id": study_id,
                    "study_name": study_id,
                    **safe_classification,
                    **row,
                }
            )

    reverse = sort_dir.lower() != "asc"
    metric_sort_key = sort_by if sort_by in {"recall", "consideration", "purchase"} else "recall"
    rows.sort(key=lambda row: row.get(metric_sort_key) or -1, reverse=reverse)

    limit_mode = limit_mode.lower()
    if limit_mode == "top10":
        rows = rows[:10]
    elif limit_mode == "top25":
        rows = rows[:25]

    return {
        "rows": rows,
        "meta": {
            "studies_included": study_ids,
            "limit_mode": limit_mode,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
            "row_count": len(rows),
        },
    }


@router.get("/touchpoints/table_multi")
def touchpoints_table_multi(
    studies: str | None = Query(None, description="Comma-separated study ids"),
    sector: str | None = Query(None),
    subsector: str | None = Query(None),
    category: str | None = Query(None),
    taxonomy_view: str | None = Query("market", description="market|standard"),
    limit_mode: str = Query("top25", description="top10|top25|all"),
    sort_by: str = Query("recall", description="Metric to sort by"),
    sort_dir: str = Query("desc", description="asc|desc"),
) -> dict:
    filters = _parse_filters(
        {
            "study_ids": studies,
            "sector": sector,
            "subsector": subsector,
            "category": category,
            "taxonomy_view": taxonomy_view,
        }
    )
    return _touchpoints_table_multi_filtered(filters, limit_mode, sort_by, sort_dir)


@router.post("/touchpoints/table_multi")
async def touchpoints_table_multi_post(
    request: Request,
    limit_mode: str = Query("top25", description="top10|top25|all"),
    sort_by: str = Query("recall", description="Metric to sort by"),
    sort_dir: str = Query("desc", description="asc|desc"),
) -> dict:
    payload = await request.json()
    filters = _parse_filters(payload)
    return _touchpoints_table_multi_filtered(filters, limit_mode, sort_by, sort_dir)


@router.get("/tracking/series")
def tracking_series_get(
    studies: str | None = Query(None, description="Comma-separated study ids"),
    sector: str | None = Query(None),
    subsector: str | None = Query(None),
    category: str | None = Query(None),
    gender: str | None = Query(None),
    nse: str | None = Query(None),
    state: str | None = Query(None),
    age_min: int | None = Query(None),
    age_max: int | None = Query(None),
    years: str | None = Query(None, description="Comma-separated years"),
    brands: str | None = Query(None, description="Comma-separated brand names"),
    taxonomy_view: str | None = Query("market", description="market|standard"),
) -> dict:
    filters = _parse_filters(
        {
            "study_ids": studies,
            "sector": sector,
            "subsector": subsector,
            "category": category,
            "taxonomy_view": taxonomy_view,
            "gender": gender,
            "nse": nse,
            "state": state,
            "age_min": age_min,
            "age_max": age_max,
            "years": years,
            "brands": brands,
        }
    )
    return _tracking_series_filtered(filters)


@router.post("/tracking/series")
async def tracking_series_post(request: Request) -> dict:
    payload = await request.json()
    filters = _parse_filters(payload)
    return _tracking_series_filtered(filters)
