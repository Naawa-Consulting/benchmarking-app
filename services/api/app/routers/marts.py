from __future__ import annotations

import csv
import logging
import unicodedata
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from app.data.rule_engine import load_rules
from app.data.warehouse import get_duckdb_connection, get_repo_root, load_parquet_as_view
from app.storage.question_map import question_map_path
from app.models.schemas import MartBuildResponse

logger = logging.getLogger(__name__)

router = APIRouter()


def _mapping_csv_path() -> Path:
    return get_repo_root() / "data" / "warehouse" / "mapping" / "question_map_v0.csv"


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
    """
    Detect coded-brand catalog questions (mention slots) and switch them to dynamic brand mode.
    In this mode, brand is resolved from raw_value_labels using the response code.
    """
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

    # Find repeated catalog sets per stage.
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


def _load_mapping_df(study_id: str) -> pd.DataFrame:
    map_path = question_map_path(study_id)
    if map_path.exists():
        df = pd.read_parquet(map_path)
        df = df[df["stage"].notna() & (df["stage"].astype(str).str.strip() != "")]
        df = df[df["brand_value"].notna() & (df["brand_value"].astype(str).str.strip() != "")]
        if df.empty:
            return pd.DataFrame()
        rules = load_rules()
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
        return df[["study_id", "var_code", "stage", "brand", "touchpoint", "value_true_codes", "true_codes"]]

    path = _mapping_csv_path()
    if not path.exists():
        return pd.DataFrame()
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = [row for row in reader if row.get("study_id") == study_id]
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["value_true_codes"] = df["value_true_codes"].fillna("1")
    df["true_codes"] = df["value_true_codes"].astype(str).str.split("|")
    df = _apply_catalog_brand_mode(study_id, df)
    df = _apply_brand_label_true_code_override(study_id, df)
    return df


@router.post("/marts/journey/build", response_model=MartBuildResponse)
def build_journey_mart(study_id: str = Query(..., description="Study id")) -> MartBuildResponse:
    logger.info("Building journey mart for %s", study_id)
    root = get_repo_root()
    mapping_df = _load_mapping_df(study_id)
    if mapping_df.empty:
        raise HTTPException(status_code=400, detail="No mapping rows found for study.")

    responses_path = root / "data" / "warehouse" / "raw" / f"study_id={study_id}" / "raw_responses.parquet"
    if not responses_path.exists():
        raise HTTPException(status_code=404, detail="Raw responses parquet not found.")

    curated_dir = root / "data" / "warehouse" / "curated" / f"study_id={study_id}"
    curated_dir.mkdir(parents=True, exist_ok=True)
    output_path = curated_dir / "fact_journey.parquet"

    conn = get_duckdb_connection()
    load_parquet_as_view(conn, "responses", str(responses_path))
    conn.register("mapping", mapping_df)
    labels_path = root / "data" / "warehouse" / "raw" / f"study_id={study_id}" / "raw_value_labels.parquet"
    if labels_path.exists():
        load_parquet_as_view(conn, "value_labels", str(labels_path))
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
        raise HTTPException(status_code=400, detail="No rows matched mapping criteria.")

    df.to_parquet(output_path, index=False)

    respondents = int(df["respondent_id"].nunique())
    rows = int(len(df))
    brands = int(df["brand"].nunique())
    stages = int(df["stage"].nunique())

    return MartBuildResponse(
        study_id=study_id,
        respondents=respondents,
        rows=rows,
        brands=brands,
        stages=stages,
        path=str(output_path),
    )
