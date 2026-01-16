from __future__ import annotations

import logging
from pathlib import Path
import re

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Request

from app.data.rule_engine import (
    apply_rules_to_variables,
    filter_rules_by_scope,
    load_rules,
    load_study_rule_scope,
    save_rules,
    save_study_rule_scope,
)
from app.data.warehouse import get_repo_root
from app.models.schemas import RuleCoverageResponse, RuleSaveResponse

logger = logging.getLogger(__name__)

router = APIRouter()


def _raw_variables_path(study_id: str) -> Path:
    return get_repo_root() / "data" / "warehouse" / "raw" / f"study_id={study_id}" / "raw_variables.parquet"


def _mapping_csv_path() -> Path:
    return get_repo_root() / "data" / "warehouse" / "mapping" / "question_map_v0.csv"


@router.get("/rules")
def get_rules() -> dict:
    rules = load_rules()
    return rules


@router.post("/rules", response_model=RuleSaveResponse)
async def save_rules_endpoint(request: Request) -> RuleSaveResponse:
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Rules payload must be a JSON object.")
    if "version" not in payload or "stage_rules" not in payload or "brand_extractors" not in payload:
        raise HTTPException(status_code=400, detail="Rules payload missing required fields.")

    path = save_rules(payload)
    version = int(payload.get("version", 1))
    return RuleSaveResponse(ok=True, path=str(path), version=version)


@router.get("/rules/study")
def get_study_rules(study_id: str = Query(..., description="Study id")) -> dict:
    rules = load_rules()
    scope = load_study_rule_scope(study_id, rules)
    return scope


@router.post("/rules/study")
async def save_study_rules(study_id: str = Query(..., description="Study id"), request: Request = ...) -> dict:
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Scope payload must be a JSON object.")

    rules = load_rules()
    valid_stage = {rule.get("id") for rule in rules.get("stage_rules", [])}
    valid_brand = {rule.get("id") for rule in rules.get("brand_extractors", [])}
    valid_ignore = {rule.get("id") for rule in rules.get("ignore_rules", [])}

    stage_ids = set(payload.get("enabled_stage_rules", []))
    brand_ids = set(payload.get("enabled_brand_extractors", []))
    ignore_ids = set(payload.get("enabled_ignore_rules", []))

    invalid = sorted(
        (stage_ids - valid_stage)
        | (brand_ids - valid_brand)
        | (ignore_ids - valid_ignore)
    )
    if invalid:
        raise HTTPException(status_code=400, detail=f"Invalid rule ids: {', '.join(map(str, invalid))}")

    scope = {
        "study_id": study_id,
        "enabled_stage_rules": list(stage_ids),
        "enabled_brand_extractors": list(brand_ids),
        "enabled_ignore_rules": list(ignore_ids),
    }
    path = save_study_rule_scope(study_id, scope, rules)
    return {"ok": True, "path": str(path), "study_id": study_id}


@router.post("/rules/run", response_model=RuleCoverageResponse)
def run_rules(study_id: str = Query(..., description="Study id")) -> RuleCoverageResponse:
    variables_path = _raw_variables_path(study_id)
    if not variables_path.exists():
        raise HTTPException(status_code=404, detail="raw_variables.parquet not found for study.")

    rules = load_rules()
    scope = load_study_rule_scope(study_id, rules)
    rules = filter_rules_by_scope(rules, scope)
    df_vars = pd.read_parquet(variables_path)
    try:
        mapped_df, stats = apply_rules_to_variables(df_vars, rules)
    except re.error as exc:  # type: ignore[name-defined]
        raise HTTPException(status_code=400, detail=f"Regex error: {exc}") from exc

    mapping_path = _mapping_csv_path()
    mapping_path.parent.mkdir(parents=True, exist_ok=True)

    existing_rows: list[dict] = []
    if mapping_path.exists():
        existing_rows = list(pd.read_csv(mapping_path).to_dict(orient="records"))

    remaining = [row for row in existing_rows if row.get("study_id") != study_id]
    mapped_rows = mapped_df.copy()
    mapped_rows.insert(0, "study_id", study_id)

    merged_rows = remaining + mapped_rows.to_dict(orient="records")
    pd.DataFrame(merged_rows).to_csv(mapping_path, index=False)

    return RuleCoverageResponse(
        study_id=study_id,
        mapped_rows=stats["mapped_rows"],
        unmapped_rows=stats["unmapped_rows"],
        ignored_rows=stats["ignored_rows"],
        touchpoint_mapped_rows=stats.get("touchpoint_mapped_rows"),
        output_path=str(mapping_path),
        examples=stats["examples"],
    )


@router.get("/rules/coverage", response_model=RuleCoverageResponse)
def rule_coverage(study_id: str = Query(..., description="Study id")) -> RuleCoverageResponse:
    variables_path = _raw_variables_path(study_id)
    if not variables_path.exists():
        raise HTTPException(status_code=404, detail="raw_variables.parquet not found for study.")

    rules = load_rules()
    scope = load_study_rule_scope(study_id, rules)
    rules = filter_rules_by_scope(rules, scope)
    df_vars = pd.read_parquet(variables_path)
    try:
        _, stats = apply_rules_to_variables(df_vars, rules)
    except re.error as exc:  # type: ignore[name-defined]
        raise HTTPException(status_code=400, detail=f"Regex error: {exc}") from exc

    return RuleCoverageResponse(
        study_id=study_id,
        mapped_rows=stats["mapped_rows"],
        unmapped_rows=stats["unmapped_rows"],
        ignored_rows=stats["ignored_rows"],
        touchpoint_mapped_rows=stats.get("touchpoint_mapped_rows"),
        output_path=None,
        examples=stats["examples"],
    )
