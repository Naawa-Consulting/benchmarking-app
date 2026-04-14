from __future__ import annotations

import csv
import unicodedata
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from app.data.ingest_from_landing import ensure_raw_from_landing, rebuild_raw_for_study
from app.data.rule_engine import (
    apply_rules_to_variables,
    filter_rules_by_scope,
    load_rules,
    load_study_rule_scope,
)
from app.data.warehouse import get_repo_root
from app.storage.question_map import question_map_path

router = APIRouter()
IMPUTE_WARN_THRESHOLD = 0.40


def _mapping_csv_path() -> Path:
    return get_repo_root() / "data" / "warehouse" / "mapping" / "question_map_v0.csv"


def _mapping_rows_for_study(study_id: str) -> list[dict]:
    path = _mapping_csv_path()
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [row for row in reader if row.get("study_id") == study_id]


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
    labels_path = (
        get_repo_root()
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_value_labels.parquet"
    )
    if not labels_path.exists() or df.empty:
        return df
    labels = pd.read_parquet(labels_path)
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
    labels_path = (
        get_repo_root()
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_value_labels.parquet"
    )
    if not labels_path.exists() or df.empty:
        return df
    labels = pd.read_parquet(labels_path)
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
    map_path = question_map_path(study_id)
    if not map_path.exists():
        return pd.DataFrame(columns=expected_cols)
    df = pd.read_parquet(map_path)
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


@router.post("/pipeline/journey/ensure")
def ensure_journey_pipeline(
    study_id: str = Query(..., description="Study id"),
    sync_raw: bool = Query(True, description="Sync raw from landing"),
    force: bool = Query(False, description="Force rebuild curated mart"),
) -> dict:
    root = get_repo_root()
    base_data_dir = root / "data"

    synced_raw = False
    rebuilt_raw = False
    errors: list[str] = []
    if sync_raw:
        summary = ensure_raw_from_landing(base_data_dir)
        synced_raw = True
        for err in summary.get("errors", []):
            errors.append(f"{err.get('study_id')}: {err.get('error')}")
        # Keep raw aligned with latest study_config when user forces a rebuild.
        # This prevents stale respondent_id/weight extraction from older ingestions.
        if force:
            raw_rebuild = rebuild_raw_for_study(base_data_dir, study_id, force=True)
            if raw_rebuild.get("status") == "error":
                errors.append(f"{study_id}: {raw_rebuild.get('reason')}")
            elif raw_rebuild.get("status") == "ok":
                rebuilt_raw = True

    variables_path = (
        base_data_dir / "warehouse" / "raw" / f"study_id={study_id}" / "raw_variables.parquet"
    )
    if not variables_path.exists():
        raise HTTPException(status_code=404, detail="raw_variables.parquet not found for study.")

    rules = load_rules()
    scope = load_study_rule_scope(study_id, rules)
    rules = filter_rules_by_scope(rules, scope)
    df_vars = pd.read_parquet(variables_path)
    mapped_df, stats = apply_rules_to_variables(df_vars, rules)

    question_map_df = _load_mapping_df_from_question_map(study_id, rules)

    mapping_path = _mapping_csv_path()
    mapping_path.parent.mkdir(parents=True, exist_ok=True)
    existing_rows: list[dict] = []
    if mapping_path.exists():
        existing_rows = list(pd.read_csv(mapping_path).to_dict(orient="records"))
    remaining = [row for row in existing_rows if row.get("study_id") != study_id]
    if not question_map_df.empty:
        merged_rows = remaining + question_map_df.to_dict(orient="records")
    else:
        mapped_rows = mapped_df.copy()
        mapped_rows.insert(0, "study_id", study_id)
        merged_rows = remaining + mapped_rows.to_dict(orient="records")
    pd.DataFrame(merged_rows).to_csv(mapping_path, index=False)

    curated_path = (
        base_data_dir
        / "warehouse"
        / "curated"
        / f"study_id={study_id}"
        / "fact_journey.parquet"
    )
    curated_path.parent.mkdir(parents=True, exist_ok=True)

    curated_status = "skipped" if curated_path.exists() and not force else "ok"
    if curated_status == "ok":
        responses_path = (
            base_data_dir / "warehouse" / "raw" / f"study_id={study_id}" / "raw_responses.parquet"
        )
        if not responses_path.exists():
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
                    "path": str(curated_path),
                },
                "errors": errors,
            }

        import duckdb

        conn = duckdb.connect()
        conn.execute(
            f"CREATE OR REPLACE VIEW responses AS SELECT * FROM read_parquet('{responses_path}')"
        )
        conn.register("mapping", mapping_df)
        labels_path = (
            base_data_dir / "warehouse" / "raw" / f"study_id={study_id}" / "raw_value_labels.parquet"
        )
        if labels_path.exists():
            conn.execute(f"CREATE OR REPLACE VIEW value_labels AS SELECT * FROM read_parquet('{labels_path}')")
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
            df.to_parquet(curated_path, index=False)

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
            "path": str(curated_path),
        },
        "errors": errors,
    }


@router.get("/pipeline/journey/status")
def journey_pipeline_status(study_id: str = Query(..., description="Study id")) -> dict:
    root = get_repo_root()
    base_data_dir = root / "data"
    raw_dir = base_data_dir / "warehouse" / "raw" / f"study_id={study_id}"
    raw_ready = (raw_dir / "raw_responses.parquet").exists() and (raw_dir / "raw_variables.parquet").exists()
    demographics_ready = (raw_dir / "respondents.parquet").exists()

    mapping_rows = _mapping_rows_for_study(study_id)
    question_map_exists = question_map_path(study_id).exists()
    mapping_ready = len(mapping_rows) > 0 or question_map_exists

    curated_path = (
        base_data_dir / "warehouse" / "curated" / f"study_id={study_id}" / "fact_journey.parquet"
    )
    curated_ready = curated_path.exists()
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
            "raw_dir": str(raw_dir),
            "mapping_csv": str(_mapping_csv_path()),
            "question_map": str(question_map_path(study_id)),
            "curated_path": str(curated_path),
        },
    }


@router.post("/pipeline/base/rebuild")
def rebuild_base_pipeline(
    study_id: str = Query(..., description="Study id"),
    force: bool = Query(False, description="Force rebuild raw"),
) -> dict:
    base_data_dir = get_repo_root() / "data"
    raw_summary = rebuild_raw_for_study(base_data_dir, study_id, force=force)

    curated_path = (
        base_data_dir
        / "warehouse"
        / "curated"
        / f"study_id={study_id}"
        / "fact_journey.parquet"
    )
    curated_status = "skipped"
    if curated_path.exists():
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
