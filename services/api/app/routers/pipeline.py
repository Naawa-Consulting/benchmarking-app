from __future__ import annotations

import logging
import unicodedata

import duckdb
import httpx
import pandas as pd
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Request

from app.core.config import get_settings
from app.data.ingest_from_landing import ensure_raw_from_landing, rebuild_raw_for_study
from app.data.rule_engine import (
    apply_rules_to_variables,
    filter_rules_by_scope,
    load_rules,
    load_study_rule_scope,
)
from app.data.study_config import load_methodology_overrides
from app.data.warehouse import blob_exists, read_parquet_blob, write_parquet_blob
from app.storage.question_map import question_map_path

logger = logging.getLogger(__name__)

router = APIRouter()
IMPUTE_WARN_THRESHOLD = 0.40


# The shared warehouse/mapping/question_map_v0.csv was retired on 2026-09-02. It was a
# fully derived dump of each study's warehouse/raw/study_id=*/question_map.parquet (row
# counts matched exactly across all 33 populated studies) that no curated build ever read,
# yet four callers rewrote the whole 1.9 MB file with an unsynchronized read-modify-write —
# the root cause of the Question Mapper race and of a real corrupt row in the live file.
# A copy is archived at warehouse/_archive/question_map_v0.2026-09-02.csv.
# See BITACORA.md 2026-09-02.


def _normalize_match(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return " ".join(text.lower().split())


def _code_tokens(value_code: object) -> str:
    if value_code is None:
        return ""
    text = str(value_code).strip()
    if not text:
        return ""
    try:
        num = float(text)
        if num.is_integer():
            iv = str(int(num))
            return f"{iv}|{iv}.0"
    except Exception:
        pass
    return text


def _apply_brand_label_true_code_override(study_id: str, df: pd.DataFrame) -> pd.DataFrame:
    labels_key = f"warehouse/raw/study_id={study_id}/raw_value_labels.parquet"
    if not blob_exists(labels_key) or df.empty:
        return df
    labels = read_parquet_blob(labels_key)
    if labels.empty:
        return df
    labels = labels[["var_code", "value_code", "value_label"]].copy()
    labels["var_code"] = labels["var_code"].astype(str)
    labels["_norm_label"] = labels["value_label"].map(_normalize_match)
    labels = labels[labels["_norm_label"].astype(str).str.len() > 0]
    if labels.empty:
        return df

    index: dict[tuple[str, str], str] = {}
    for row in labels.itertuples(index=False):
        var_code = row[0]
        value_code = row[1]
        norm_label = row[3]
        key = (str(var_code), str(norm_label))
        if key in index:
            continue
        tokens = _code_tokens(value_code)
        if tokens:
            index[key] = tokens

    if not index:
        return df

    out = df.copy()
    var_codes = out["var_code"].astype(str)
    brand_norm = out["brand"].map(_normalize_match)
    resolved: list[str | None] = []
    for var_code, bnorm in zip(var_codes.tolist(), brand_norm.tolist()):
        if not bnorm:
            resolved.append(None)
            continue
        resolved.append(index.get((var_code, bnorm)))

    out["_resolved_true_codes"] = resolved
    mask = out["_resolved_true_codes"].notna()
    if mask.any():
        out.loc[mask, "value_true_codes"] = out.loc[mask, "_resolved_true_codes"]
        out.loc[mask, "true_codes"] = out.loc[mask, "value_true_codes"].astype(str).str.split("|")
    out = out.drop(columns=["_resolved_true_codes"], errors="ignore")
    return out


def _apply_catalog_brand_mode(study_id: str, df: pd.DataFrame) -> pd.DataFrame:
    labels_key = f"warehouse/raw/study_id={study_id}/raw_value_labels.parquet"
    if not blob_exists(labels_key) or df.empty:
        return df
    labels = read_parquet_blob(labels_key)
    if labels.empty:
        return df
    labels = labels[["var_code", "value_label"]].copy()
    labels["var_code"] = labels["var_code"].astype(str)
    labels["norm_label"] = labels["value_label"].map(_normalize_match)
    labels = labels[labels["norm_label"].astype(str).str.len() > 0]
    labels = labels[~labels["norm_label"].str.startswith("otro")]
    if labels.empty:
        return df

    grouped = labels.groupby("var_code")["norm_label"].agg(lambda s: tuple(sorted(set(s)))).to_dict()
    if not grouped:
        return df

    out = df.copy()
    out["var_code"] = out["var_code"].astype(str)
    out["stage"] = out["stage"].astype(str)
    out["_brand_norm"] = out["brand"].map(_normalize_match)
    out["_labels"] = out["var_code"].map(grouped)

    candidate_mask = out["_labels"].map(lambda x: isinstance(x, tuple) and len(x) >= 5)
    if not candidate_mask.any():
        return out.drop(columns=["_brand_norm", "_labels"], errors="ignore")

    stage_grouped = (
        out[candidate_mask]
        .groupby(["stage", "_labels"])["var_code"]
        .nunique()
        .reset_index(name="var_count")
    )
    valid_stage_sets = {
        (row["stage"], row["_labels"])
        for _, row in stage_grouped.iterrows()
        if int(row["var_count"]) >= 3
    }
    if not valid_stage_sets:
        return out.drop(columns=["_brand_norm", "_labels"], errors="ignore")

    def _is_catalog_row(row: pd.Series) -> bool:
        key = (row["stage"], row["_labels"])
        if key not in valid_stage_sets:
            return False
        labels_tuple = row["_labels"]
        brand_norm = row["_brand_norm"]
        return bool(brand_norm and isinstance(labels_tuple, tuple) and brand_norm in set(labels_tuple))

    catalog_mask = out.apply(_is_catalog_row, axis=1)
    if catalog_mask.any():
        out.loc[catalog_mask, "brand"] = "__VALUE_LABEL__"
        out.loc[catalog_mask, "value_true_codes"] = "__LABEL_VALUE__"
        out.loc[catalog_mask, "true_codes"] = out.loc[catalog_mask, "value_true_codes"].astype(str).str.split("|")

    return out.drop(columns=["_brand_norm", "_labels"], errors="ignore")


def _load_mapping_df_from_question_map(study_id: str, rules: dict) -> pd.DataFrame:
    expected_cols = [
        "study_id",
        "var_code",
        "stage",
        "brand",
        "touchpoint",
        "value_true_codes",
        "true_codes",
    ]
    map_key = question_map_path(study_id)
    if not blob_exists(map_key):
        return pd.DataFrame(columns=expected_cols)
    df = read_parquet_blob(map_key)
    df = df[df["stage"].notna() & (df["stage"].astype(str).str.strip() != "")]
    df = df[df["brand_value"].notna() & (df["brand_value"].astype(str).str.strip() != "")]
    if df.empty:
        return pd.DataFrame(columns=expected_cols)
    default_true_codes = (rules.get("defaults") or {}).get("value_true_codes", "1")
    df = df.assign(
        study_id=study_id,
        var_code=df["var_code"].astype(str),
        stage=df["stage"],
        brand=df["brand_value"],
        touchpoint=df.get("touchpoint_value"),
        value_true_codes=default_true_codes,
    )
    df["true_codes"] = df["value_true_codes"].astype(str).str.split("|")
    df = _apply_catalog_brand_mode(study_id, df)
    df = _apply_brand_label_true_code_override(study_id, df)
    return df[expected_cols]


def _build_consideration_imputation_report(study_id: str) -> dict:
    try:
        from app.routers.analytics import _compute_table_rows
    except Exception:
        return {
            "version": "v1.0",
            "total_rows": 0,
            "imputed_rows": 0,
            "imputed_pct": 0.0,
            "levels": {"category": 0, "subsector": 0, "sector": 0, "global": 0, "none": 0},
            "post_purchase_gt_consideration_rows": 0,
            "post_comparable_rows": 0,
            "post_purchase_gt_consideration_pct": None,
            "warnings": [],
        }

    rows = _compute_table_rows(study_id)
    levels = {"category": 0, "subsector": 0, "sector": 0, "global": 0, "none": 0}
    imputed_rows = 0
    comparable = 0
    purchase_gt_consideration = 0
    for row in rows:
        source = str(row.get("brand_consideration_source") or "none").strip().lower()
        level = str(row.get("brand_consideration_impute_level") or "none").strip().lower()
        if level not in levels:
            level = "none"
        if source == "imputed":
            imputed_rows += 1
            levels[level] += 1
        consideration = row.get("brand_consideration")
        purchase = row.get("brand_purchase")
        if isinstance(consideration, (int, float)) and isinstance(purchase, (int, float)):
            comparable += 1
            if float(purchase) > float(consideration):
                purchase_gt_consideration += 1

    total_rows = len(rows)
    imputed_pct = round((imputed_rows / total_rows) * 100, 1) if total_rows else 0.0
    post_pct = round((purchase_gt_consideration / comparable) * 100, 1) if comparable else None
    warnings: list[dict] = []
    if comparable and (purchase_gt_consideration / comparable) > IMPUTE_WARN_THRESHOLD:
        warnings.append(
            {
                "level": "study",
                "warning": "High purchase>consideration rate after imputation.",
                "purchase_gt_consideration_pct": post_pct,
                "post_n": comparable,
            }
        )

    return {
        "version": "v1.0",
        "total_rows": total_rows,
        "imputed_rows": imputed_rows,
        "imputed_pct": imputed_pct,
        "levels": levels,
        "post_purchase_gt_consideration_rows": purchase_gt_consideration,
        "post_comparable_rows": comparable,
        "post_purchase_gt_consideration_pct": post_pct,
        "warnings": warnings,
    }


def _build_satisfaction_imputation_report(study_id: str) -> dict:
    try:
        from app.routers.analytics import _compute_table_rows
    except Exception:
        return {
            "version": "v1.0",
            "total_rows": 0,
            "imputed_rows": 0,
            "imputed_pct": 0.0,
            "levels": {"category": 0, "subsector": 0, "sector": 0, "global": 0, "none": 0},
            "post_recommendation_gt_satisfaction_rows": 0,
            "post_comparable_rows": 0,
            "post_recommendation_gt_satisfaction_pct": None,
            "warnings": [],
        }

    rows = _compute_table_rows(study_id)
    levels = {"category": 0, "subsector": 0, "sector": 0, "global": 0, "none": 0}
    imputed_rows = 0
    comparable = 0
    recommendation_gt_satisfaction = 0
    for row in rows:
        source = str(row.get("brand_satisfaction_source") or "none").strip().lower()
        level = str(row.get("brand_satisfaction_impute_level") or "none").strip().lower()
        if level not in levels:
            level = "none"
        if source == "imputed":
            imputed_rows += 1
            levels[level] += 1
        satisfaction = row.get("brand_satisfaction")
        recommendation = row.get("brand_recommendation")
        if isinstance(satisfaction, (int, float)) and isinstance(recommendation, (int, float)):
            comparable += 1
            if float(recommendation) > float(satisfaction):
                recommendation_gt_satisfaction += 1

    total_rows = len(rows)
    imputed_pct = round((imputed_rows / total_rows) * 100, 1) if total_rows else 0.0
    post_pct = round((recommendation_gt_satisfaction / comparable) * 100, 1) if comparable else None
    warnings: list[dict] = []
    if comparable and (recommendation_gt_satisfaction / comparable) > IMPUTE_WARN_THRESHOLD:
        warnings.append(
            {
                "level": "study",
                "warning": "High recommendation>satisfaction rate after imputation.",
                "recommendation_gt_satisfaction_pct": post_pct,
                "post_n": comparable,
            }
        )

    return {
        "version": "v1.0",
        "total_rows": total_rows,
        "imputed_rows": imputed_rows,
        "imputed_pct": imputed_pct,
        "levels": levels,
        "post_recommendation_gt_satisfaction_rows": recommendation_gt_satisfaction,
        "post_comparable_rows": comparable,
        "post_recommendation_gt_satisfaction_pct": post_pct,
        "warnings": warnings,
    }


def _build_csat_imputation_report(study_id: str) -> dict:
    try:
        from app.routers.analytics import _compute_table_rows
    except Exception:
        return {
            "version": "v1.0",
            "total_rows": 0,
            "eligible_rows": 0,
            "imputed_rows": 0,
            "imputed_pct": 0.0,
            "levels": {"category": 0, "subsector": 0, "sector": 0, "global": 0, "none": 0},
            "post_csat_gt_satisfaction_rows": 0,
            "post_comparable_rows": 0,
            "post_csat_gt_satisfaction_pct": None,
            "warnings": [],
        }

    rows = _compute_table_rows(study_id)
    levels = {"category": 0, "subsector": 0, "sector": 0, "global": 0, "none": 0}
    imputed_rows = 0
    eligible_rows = 0
    comparable = 0
    csat_gt_satisfaction = 0
    for row in rows:
        source = str(row.get("csat_source") or "none").strip().lower()
        level = str(row.get("csat_impute_level") or "none").strip().lower()
        if level not in levels:
            level = "none"
        if str(row.get("brand_satisfaction_source") or "none").strip().lower() == "imputed":
            eligible_rows += 1
        if source == "imputed":
            imputed_rows += 1
            levels[level] += 1
        csat = row.get("csat")
        satisfaction = row.get("brand_satisfaction")
        if isinstance(csat, (int, float)) and isinstance(satisfaction, (int, float)):
            comparable += 1
            if float(csat) > float(satisfaction):
                csat_gt_satisfaction += 1

    total_rows = len(rows)
    imputed_pct = round((imputed_rows / eligible_rows) * 100, 1) if eligible_rows else 0.0
    post_pct = round((csat_gt_satisfaction / comparable) * 100, 1) if comparable else None
    warnings: list[dict] = []
    if comparable and (csat_gt_satisfaction / comparable) > IMPUTE_WARN_THRESHOLD:
        warnings.append(
            {
                "level": "study",
                "warning": "High csat>satisfaction rate after imputation.",
                "csat_gt_satisfaction_pct": post_pct,
                "post_n": comparable,
            }
        )

    return {
        "version": "v1.0",
        "total_rows": total_rows,
        "eligible_rows": eligible_rows,
        "imputed_rows": imputed_rows,
        "imputed_pct": imputed_pct,
        "levels": levels,
        "post_csat_gt_satisfaction_rows": csat_gt_satisfaction,
        "post_comparable_rows": comparable,
        "post_csat_gt_satisfaction_pct": post_pct,
        "warnings": warnings,
    }


def _apply_consideration_from_purchase_override(study_id: str, df: pd.DataFrame) -> pd.DataFrame:
    """Force stage='consideration' value=1 wherever the same respondent+brand has
    stage='purchase' value=1.

    Some studies field consideration and purchase as a single-select grid per brand
    instead of a cumulative funnel, so respondents who bought a brand directly often
    never mark it as considered — consideration reads artificially lower than
    purchase. Gated per-study via methodology_overrides so it never affects studies
    fielded normally.
    """
    purchased_pairs = set(
        map(
            tuple,
            df.loc[(df["stage"] == "purchase") & (df["value"] == 1), ["respondent_id", "brand"]].itertuples(
                index=False, name=None
            ),
        )
    )
    if not purchased_pairs:
        return df

    is_consideration = df["stage"] == "consideration"
    pair_in_purchased = list(zip(df["respondent_id"], df["brand"]))
    force_mask = is_consideration & pd.Series(
        [pair in purchased_pairs for pair in pair_in_purchased], index=df.index
    )
    force_mask &= df["value"] != 1
    if force_mask.any():
        logger.info(
            "consideration_from_purchase override: forced %d consideration rows to value=1 for study %s",
            int(force_mask.sum()),
            study_id,
        )
        df.loc[force_mask, "value"] = 1
    return df


@router.post("/pipeline/journey/ensure")
def ensure_journey_pipeline(
    study_id: str = Query(..., description="Study id"),
    sync_raw: bool = Query(True, description="Sync raw from landing"),
    force: bool = Query(False, description="Force rebuild curated mart"),
) -> dict:
    synced_raw = False
    rebuilt_raw = False
    errors: list[str] = []
    if sync_raw:
        summary = ensure_raw_from_landing()
        synced_raw = True
        for err in summary.get("errors", []):
            errors.append(f"{err.get('study_id')}: {err.get('error')}")
        # Keep raw aligned with latest study_config when user forces a rebuild.
        # This prevents stale respondent_id/weight extraction from older ingestions.
        if force:
            raw_rebuild = rebuild_raw_for_study(study_id, force=True)
            if raw_rebuild.get("status") == "error":
                errors.append(f"{study_id}: {raw_rebuild.get('reason')}")
            elif raw_rebuild.get("status") == "ok":
                rebuilt_raw = True

    variables_key = f"warehouse/raw/study_id={study_id}/raw_variables.parquet"
    if not blob_exists(variables_key):
        raise HTTPException(status_code=404, detail="raw_variables.parquet not found for study.")

    rules = load_rules()
    scope = load_study_rule_scope(study_id, rules)
    rules = filter_rules_by_scope(rules, scope)
    df_vars = read_parquet_blob(variables_key)
    mapped_df, stats = apply_rules_to_variables(df_vars, rules)

    question_map_df = _load_mapping_df_from_question_map(study_id, rules)

    curated_key = f"warehouse/curated/study_id={study_id}/fact_journey.parquet"

    curated_status = "skipped" if blob_exists(curated_key) and not force else "ok"
    if curated_status == "ok":
        responses_key = f"warehouse/raw/study_id={study_id}/raw_responses.parquet"
        if not blob_exists(responses_key):
            raise HTTPException(status_code=404, detail="raw_responses.parquet not found for study.")

        mapping_df = question_map_df if not question_map_df.empty else mapped_df.copy()
        if mapping_df is mapped_df:
            mapping_df.insert(0, "study_id", study_id)
            mapping_df["value_true_codes"] = mapping_df["value_true_codes"].fillna(
                rules.get("defaults", {}).get("value_true_codes", "1")
            )
            mapping_df["true_codes"] = mapping_df["value_true_codes"].astype(str).str.split("|")

        required_cols = {"study_id", "var_code", "stage", "brand", "touchpoint", "value_true_codes", "true_codes"}
        if mapping_df.empty or not required_cols.issubset(set(mapping_df.columns)):
            curated_status = "error"
            errors.append("No mapping rows available for this study. Define mappings or rules before running pipeline.")
            return {
                "study_id": study_id,
                "synced_raw": synced_raw,
                "mapping": {
                    "status": "ok",
                    "mapped_rows": stats.get("mapped_rows", 0),
                    "unmapped_rows": stats.get("unmapped_rows", 0),
                },
                "curated": {
                    "status": curated_status,
                    "path": curated_key,
                },
                "errors": errors,
            }

        conn = duckdb.connect()
        conn.register("responses", read_parquet_blob(responses_key))
        conn.register("mapping", mapping_df)
        labels_key = f"warehouse/raw/study_id={study_id}/raw_value_labels.parquet"
        if blob_exists(labels_key):
            conn.register("value_labels", read_parquet_blob(labels_key))
        else:
            conn.execute("CREATE OR REPLACE TEMP VIEW value_labels AS SELECT NULL::VARCHAR AS var_code, NULL::VARCHAR AS value_code, NULL::VARCHAR AS value_label WHERE 1=0")
        weight_exists = (
            conn.execute(
                """
                SELECT COUNT(*) FROM information_schema.columns
                WHERE table_name = 'responses' AND column_name = 'weight'
                """
            ).fetchone()[0]
            > 0
        )
        weight_expr = "COALESCE(TRY_CAST(r.weight AS DOUBLE), 1.0)" if weight_exists else "1.0"
        query = """
            SELECT
                r.study_id,
                r.respondent_id,
                m.stage,
                CASE
                    WHEN m.value_true_codes = '__LABEL_VALUE__'
                        THEN NULLIF(TRIM(CAST(l.value_label AS VARCHAR)), '')
                    ELSE m.brand
                END AS brand,
                m.touchpoint,
                {weight_expr} AS weight,
                TRY_CAST(r.value AS INTEGER) AS value_raw,
                CASE
                    WHEN m.value_true_codes = '__LABEL_VALUE__'
                        THEN CASE
                            WHEN l.value_label IS NULL THEN 0
                            WHEN LOWER(TRIM(CAST(l.value_label AS VARCHAR))) LIKE 'otro%' THEN 0
                            ELSE 1
                        END
                    WHEN list_contains(m.true_codes, CAST(r.value AS VARCHAR)) THEN 1
                    ELSE 0
                END AS value
            FROM responses r
            INNER JOIN mapping m
                ON r.var_code = m.var_code
                AND r.study_id = m.study_id
            LEFT JOIN value_labels l
                ON l.var_code = r.var_code
                AND TRY_CAST(l.value_code AS DOUBLE) = TRY_CAST(r.value AS DOUBLE)
        """
        df = conn.execute(query.format(weight_expr=weight_expr)).df()
        if df.empty:
            curated_status = "error"
            errors.append("No rows matched mapping criteria.")
        else:
            overrides = load_methodology_overrides(study_id)
            if overrides.get("consideration_from_purchase"):
                df = _apply_consideration_from_purchase_override(study_id, df)
            write_parquet_blob(curated_key, df)

    return {
        "study_id": study_id,
        "synced_raw": synced_raw,
        "rebuilt_raw": rebuilt_raw,
        "mapping": {
            "status": "ok",
            "mapped_rows": stats.get("mapped_rows", 0),
            "unmapped_rows": stats.get("unmapped_rows", 0),
        },
        "curated": {
            "status": curated_status,
            "path": curated_key,
        },
        "errors": errors,
    }


@router.get("/pipeline/journey/status")
def journey_pipeline_status(study_id: str = Query(..., description="Study id")) -> dict:
    raw_prefix = f"warehouse/raw/study_id={study_id}"
    raw_ready = blob_exists(f"{raw_prefix}/raw_responses.parquet") and blob_exists(f"{raw_prefix}/raw_variables.parquet")
    demographics_ready = blob_exists(f"{raw_prefix}/respondents.parquet")

    # Was `len(csv_rows_for_study) > 0 or question_map_exists`. The CSV only ever held
    # rows derived from that same parquet, so "has CSV rows" implied "parquet exists"
    # and the whole expression collapses to the parquet check — same result, no extra read.
    question_map_exists = blob_exists(question_map_path(study_id))
    mapping_ready = question_map_exists

    curated_key = f"warehouse/curated/study_id={study_id}/fact_journey.parquet"
    curated_ready = blob_exists(curated_key)
    consideration_imputation = (
        _build_consideration_imputation_report(study_id) if curated_ready else None
    )
    satisfaction_imputation = (
        _build_satisfaction_imputation_report(study_id) if curated_ready else None
    )
    csat_imputation = (
        _build_csat_imputation_report(study_id) if curated_ready else None
    )

    return {
        "study_id": study_id,
        "raw_ready": raw_ready,
        "mapping_ready": mapping_ready,
        "curated_ready": curated_ready,
        "demographics_ready": demographics_ready,
        "consideration_imputation": consideration_imputation,
        "satisfaction_imputation": satisfaction_imputation,
        "csat_imputation": csat_imputation,
        "paths": {
            "raw_dir": raw_prefix,
            "question_map": question_map_path(study_id),
            "curated_path": curated_key,
        },
    }


@router.post("/pipeline/base/rebuild")
def rebuild_base_pipeline(
    study_id: str = Query(..., description="Study id"),
    force: bool = Query(False, description="Force rebuild raw"),
) -> dict:
    raw_summary = rebuild_raw_for_study(study_id, force=force)

    curated_key = f"warehouse/curated/study_id={study_id}/fact_journey.parquet"
    curated_status = "skipped"
    if blob_exists(curated_key):
        try:
            ensure_journey_pipeline(study_id=study_id, sync_raw=False, force=True)
            curated_status = "ok"
        except HTTPException:
            curated_status = "error"

    return {
        "study_id": study_id,
        "raw": raw_summary,
        "curated": {"status": curated_status},
    }


def _run_push_job(job_id: str, study_ids: list[str], callback_url: str) -> None:
    """Computes everything apps/web's Push button needs, then reports the result
    to Vercel by HTTP callback. Runs as a FastAPI BackgroundTasks job, so it has
    no per-request time ceiling the way the triggering Vercel function does —
    that's the whole point: Push can take as long as it needs here.

    Imports are lazy to avoid a module-load-time circular import between
    analytics/filters/studies and this router (same pattern already used by
    the imputation-report builders above)."""
    settings = get_settings()
    headers = {"Content-Type": "application/json"}
    if settings.internal_api_key:
        headers["x-internal-api-key"] = settings.internal_api_key

    try:
        from app.routers import analytics, filters
        from app.routers import studies as studies_router

        job_filters = analytics._parse_filters({"study_ids": study_ids, "taxonomy_view": "standard"})
        journey_result = analytics._journey_table_multi_filtered(
            job_filters, "all", "brand_awareness", "desc"
        )
        touchpoints_result = analytics._touchpoints_table_multi_filtered(
            job_filters, "all", "recall", "desc"
        )
        taxonomy_result = filters.filter_taxonomy_options(view="standard")
        demographics_result = filters.filter_demographic_options(study_ids=",".join(study_ids))

        studies_result = []
        for study_id in study_ids:
            classification = studies_router._classification_for_study(study_id)
            studies_result.append(
                {
                    "id": study_id,
                    "name": study_id,
                    "sector": classification.get("sector"),
                    "subsector": classification.get("subsector"),
                    "category": classification.get("category"),
                    "market_sector": classification.get("market_sector"),
                    "market_subsector": classification.get("market_subsector"),
                    "market_category": classification.get("market_category"),
                    "market_source": classification.get("market_source"),
                }
            )

        body = {
            "job_id": job_id,
            "study_ids": study_ids,
            "journey": journey_result,
            "touchpoints": touchpoints_result,
            "studies": studies_result,
            "taxonomy": taxonomy_result,
            "demographics": demographics_result,
        }
        resp = httpx.post(callback_url, json=body, headers=headers, timeout=90.0, follow_redirects=True)
        if resp.status_code >= 400:
            logger.error(
                "Push callback to %s returned %s for job %s: %s",
                callback_url, resp.status_code, job_id, resp.text[:500],
            )
        else:
            logger.info("Push callback to %s succeeded (%s) for job %s", callback_url, resp.status_code, job_id)
    except Exception as exc:
        logger.exception("Push job %s failed before reporting results", job_id)
        try:
            fail_resp = httpx.post(
                callback_url,
                json={"job_id": job_id, "error": str(exc)},
                headers=headers,
                timeout=30.0,
                follow_redirects=True,
            )
            if fail_resp.status_code >= 400:
                logger.error(
                    "Push failure-callback to %s also failed (%s) for job %s",
                    callback_url, fail_resp.status_code, job_id,
                )
        except Exception:
            logger.exception("Push failure-callback to %s raised for job %s", callback_url, job_id)


@router.post("/pipeline/push/start")
async def start_push(request: Request, background_tasks: BackgroundTasks) -> dict:
    payload = await request.json()
    job_id = payload.get("job_id")
    study_ids = payload.get("study_ids") or []
    callback_url = payload.get("callback_url")
    if not job_id or not study_ids or not callback_url:
        raise HTTPException(status_code=400, detail="job_id, study_ids and callback_url are required.")
    background_tasks.add_task(_run_push_job, job_id, study_ids, callback_url)
    return {"status": "scheduled", "job_id": job_id}
