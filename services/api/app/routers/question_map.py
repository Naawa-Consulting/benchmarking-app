from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request

from app.data.rule_engine import (
    filter_rules_by_scope,
    infer_question_mapping,
    load_rules,
    load_study_rule_scope,
)
from app.data.warehouse import get_repo_root
from app.storage.question_map import load_question_map, save_question_map

router = APIRouter()


def _is_empty(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


@router.get("/question-map")
def get_question_map(
    study_id: str = Query(..., description="Study id"),
    q: str | None = Query(None, description="Search question text"),
    unmapped_only: str | None = Query(None, description="Only rows with empty mapping"),
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> dict:
    try:
        df = load_question_map(study_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    if q:
        query = str(q).lower()
        df = df[
            df["var_code"].astype(str).str.lower().str.contains(query)
            | df["question_text"].astype(str).str.lower().str.contains(query)
        ]

    if unmapped_only == "1":
        df = df[
            df["stage"].isna()
            | (df["stage"].astype(str).str.strip() == "")
            | df["brand_value"].isna()
            | (df["brand_value"].astype(str).str.strip() == "")
            | df["touchpoint_value"].isna()
            | (df["touchpoint_value"].astype(str).str.strip() == "")
        ]

    total = int(len(df))
    df = df.iloc[offset : offset + limit]
    return {"study_id": study_id, "rows": df.to_dict(orient="records"), "meta": {"row_count": total}}


@router.post("/question-map/bulk-update")
async def bulk_update_question_map(
    study_id: str = Query(..., description="Study id"),
    request: Request = ...,
) -> dict:
    payload = await request.json()
    var_codes = payload.get("var_codes") or []
    patch = payload.get("patch") or {}
    mode = payload.get("mode") or {}
    updated_by = payload.get("updated_by") or "admin-ui"

    if not var_codes:
        raise HTTPException(status_code=400, detail="var_codes is required.")

    df = load_question_map(study_id)
    var_codes_set = {str(code) for code in var_codes}
    now = datetime.utcnow().isoformat()

    def apply_stage(row: pd.Series) -> pd.Series:
        stage_mode = mode.get("stage")
        if stage_mode == "manual" and patch.get("stage") is not None:
            row["stage"] = patch.get("stage")
            row["source_stage"] = "manual"
        elif stage_mode == "clear":
            row["stage"] = None
            row["source_stage"] = "empty"
        return row

    def apply_brand(row: pd.Series) -> pd.Series:
        brand_mode = mode.get("brand")
        if brand_mode == "manual":
            if patch.get("brand_value") is not None:
                row["brand_value"] = patch.get("brand_value")
            if patch.get("brand_extractor_id") is not None:
                row["brand_extractor_id"] = patch.get("brand_extractor_id")
            row["brand_mode"] = "manual"
            row["source_brand"] = "manual"
        elif brand_mode == "clear":
            row["brand_value"] = None
            row["brand_extractor_id"] = None
            row["brand_mode"] = "none"
            row["source_brand"] = "empty"
        return row

    def apply_touchpoint(row: pd.Series) -> pd.Series:
        touchpoint_mode = mode.get("touchpoint")
        if touchpoint_mode == "manual":
            if patch.get("touchpoint_value") is not None:
                row["touchpoint_value"] = patch.get("touchpoint_value")
            if patch.get("touchpoint_rule_id") is not None:
                row["touchpoint_rule_id"] = patch.get("touchpoint_rule_id")
            row["touchpoint_mode"] = "manual"
            row["source_touchpoint"] = "manual"
        elif touchpoint_mode == "clear":
            row["touchpoint_value"] = None
            row["touchpoint_rule_id"] = None
            row["touchpoint_mode"] = "none"
            row["source_touchpoint"] = "empty"
        return row

    updated = 0
    for idx, row in df.iterrows():
        if str(row.get("var_code")) not in var_codes_set:
            continue
        row = apply_stage(row)
        row = apply_brand(row)
        row = apply_touchpoint(row)
        row["updated_at"] = now
        row["updated_by"] = updated_by
        df.loc[idx] = row
        updated += 1

    save_question_map(study_id, df)
    return {"study_id": study_id, "updated": updated}


@router.post("/question-map/apply-suggestions")
async def apply_suggestions(
    study_id: str = Query(..., description="Study id"),
    request: Request = ...,
) -> dict:
    payload = await request.json()
    targets = payload.get("targets") or ["stage", "brand", "touchpoint"]
    only_empty = payload.get("only_empty", True)

    df = load_question_map(study_id)
    rules = load_rules()
    scope = load_study_rule_scope(study_id, rules)
    rules = filter_rules_by_scope(rules, scope)

    filled = {"stage": 0, "brand": 0, "touchpoint": 0}
    skipped_manual = {"stage": 0, "brand": 0, "touchpoint": 0}
    now = datetime.utcnow().isoformat()

    for idx, row in df.iterrows():
        question_text = str(row.get("question_text") or "")
        var_code = str(row.get("var_code") or "")
        mapping = infer_question_mapping(question_text, var_code, rules)

        if "stage" in targets:
            if row.get("source_stage") == "manual":
                skipped_manual["stage"] += 1
            else:
                current_stage = row.get("stage")
                if not only_empty or _is_empty(current_stage):
                    if mapping.get("stage"):
                        row["stage"] = mapping.get("stage")
                        row["source_stage"] = "rule"
                        filled["stage"] += 1

        if "brand" in targets:
            if row.get("source_brand") == "manual":
                skipped_manual["brand"] += 1
            else:
                current_brand = row.get("brand_value")
                if not only_empty or _is_empty(current_brand):
                    if mapping.get("brand"):
                        row["brand_value"] = mapping.get("brand")
                        row["brand_mode"] = "rule"
                        row["source_brand"] = "rule"
                        row["brand_extractor_id"] = mapping.get("brand_extractor_id")
                        filled["brand"] += 1

        if "touchpoint" in targets:
            if row.get("source_touchpoint") == "manual":
                skipped_manual["touchpoint"] += 1
            else:
                current_touchpoint = row.get("touchpoint_value")
                if not only_empty or _is_empty(current_touchpoint):
                    if mapping.get("touchpoint"):
                        row["touchpoint_value"] = mapping.get("touchpoint")
                        row["touchpoint_mode"] = "rule"
                        row["source_touchpoint"] = "rule"
                        row["touchpoint_rule_id"] = mapping.get("touchpoint_rule_id")
                        filled["touchpoint"] += 1

        row["updated_at"] = now
        row["updated_by"] = "rules"
        df.loc[idx] = row

    save_question_map(study_id, df)
    return {"study_id": study_id, "filled": filled, "skipped_manual": skipped_manual}


@router.get("/question-map/value-preview")
def question_value_preview(
    study_id: str = Query(..., description="Study id"),
    var_code: str = Query(..., description="Variable code"),
    mode: str = Query("labels", description="labels|samples"),
    n: int = Query(12, ge=1, le=50),
) -> dict:
    root = get_repo_root()
    labels_path = (
        root
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_value_labels.parquet"
    )
    responses_path = (
        root
        / "data"
        / "warehouse"
        / "raw"
        / f"study_id={study_id}"
        / "raw_responses.parquet"
    )
    if mode == "labels" and labels_path.exists():
        df = pd.read_parquet(labels_path)
        df = df[df["var_code"].astype(str) == str(var_code)]
        if not df.empty:
            items = [
                {"code": str(row["value_code"]), "label": str(row["value_label"])}
                for row in df.to_dict(orient="records")
            ]
            return {"var_code": var_code, "kind": "labels", "items": items}

    if not responses_path.exists():
        raise HTTPException(status_code=404, detail="raw_responses.parquet not found for study.")

    conn = duckdb.connect()
    conn.execute(f"CREATE OR REPLACE VIEW responses AS SELECT * FROM read_parquet('{responses_path}')")
    rows = conn.execute(
        """
        SELECT DISTINCT value
        FROM responses
        WHERE var_code = ?
        LIMIT ?
        """,
        [var_code, n],
    ).fetchall()
    return {"var_code": var_code, "kind": "samples", "items": [str(row[0]) for row in rows]}
