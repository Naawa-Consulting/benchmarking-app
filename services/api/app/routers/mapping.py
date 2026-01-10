from __future__ import annotations

import csv
import io
import logging
import re
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Response

from app.data.warehouse import get_duckdb_connection, get_repo_root, load_parquet_as_view
from app.models.schemas import (
    MappingCandidate,
    MappingListResponse,
    MappingRowInput,
    MappingSaveRequest,
    MappingSaveResponse,
    MappingSuggestResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()

RULES: dict[str, str] = {
    "awareness": r"(conoce|ha\s+escuchado|conocimiento|awareness)",
    "consideration": r"(considera|consideraría|probable|intención|preferiría)",
    "purchase": r"(compr(ó|a)|adquir(ió|iría)|última\s+compra|purchase)",
}


def _mapping_csv_path() -> Path:
    return get_repo_root() / "data" / "warehouse" / "mapping" / "question_map_v0.csv"


def _load_mapping_rows() -> list[dict[str, str]]:
    path = _mapping_csv_path()
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [row for row in reader]


def _write_mapping_rows(rows: list[dict[str, str]]) -> None:
    path = _mapping_csv_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["study_id", "var_code", "stage", "brand", "value_true_codes"]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _variables_from_raw(study_id: str) -> pd.DataFrame:
    raw_root = get_repo_root() / "data" / "warehouse" / "raw"
    variables_path = raw_root / f"study_id={study_id}" / "raw_variables.parquet"
    responses_path = raw_root / f"study_id={study_id}" / "raw_responses.parquet"

    if variables_path.exists():
        df = pd.read_parquet(variables_path)
        return df[["var_code", "question_text"]]

    if not responses_path.exists():
        raise HTTPException(status_code=404, detail="Raw data not found for study.")

    conn = get_duckdb_connection()
    load_parquet_as_view(conn, "responses", str(responses_path))
    rows = conn.execute("SELECT DISTINCT var_code FROM responses").fetchall()
    return pd.DataFrame(rows, columns=["var_code"])


def _infer_candidates(df: pd.DataFrame, limit: int) -> list[MappingCandidate]:
    candidates: list[MappingCandidate] = []
    for _, row in df.iterrows():
        var_code = str(row.get("var_code", "") or "")
        question_text = row.get("question_text")
        question_text_str = str(question_text) if question_text is not None else ""
        combined = f"{var_code} {question_text_str}".strip()
        if not combined:
            continue

        matches: list[tuple[str, bool]] = []
        for stage, pattern in RULES.items():
            if re.search(pattern, combined, flags=re.IGNORECASE):
                strong_match = bool(question_text_str) and re.search(pattern, question_text_str, flags=re.IGNORECASE)
                matches.append((stage, strong_match))

        if not matches:
            continue

        if len(matches) == 1:
            stage, strong_match = matches[0]
            confidence = 0.9 if strong_match else 0.3
        else:
            stage = matches[0][0]
            confidence = 0.6

        candidates.append(
            MappingCandidate(
                var_code=var_code,
                question_text=question_text_str or None,
                suggested_stage=stage,
                confidence=confidence,
            )
        )

        if len(candidates) >= limit:
            break

    return candidates


@router.get("/mapping/suggest", response_model=MappingSuggestResponse)
def suggest_mapping(
    study_id: str = Query(..., description="Study id"),
    limit: int = Query(200, ge=1, le=500),
) -> MappingSuggestResponse:
    logger.info("Suggesting mapping candidates for %s", study_id)
    df = _variables_from_raw(study_id)
    candidates = _infer_candidates(df, limit)
    return MappingSuggestResponse(study_id=study_id, rules=RULES, candidates=candidates)


@router.get("/mapping/template")
def mapping_template(study_id: str = Query(..., description="Study id")) -> Response:
    logger.info("Generating mapping template for %s", study_id)
    df = _variables_from_raw(study_id)
    candidates = _infer_candidates(df, 10)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["study_id", "var_code", "stage", "brand", "value_true_codes"])
    for candidate in candidates:
        writer.writerow(
            [study_id, candidate.var_code, candidate.suggested_stage, "", "1"]
        )

    return Response(content=output.getvalue(), media_type="text/csv")


@router.get("/mapping", response_model=MappingListResponse)
def get_mapping(study_id: str = Query(..., description="Study id")) -> MappingListResponse:
    rows = [row for row in _load_mapping_rows() if row.get("study_id") == study_id]
    return MappingListResponse(study_id=study_id, rows=rows)


@router.post("/mapping/save", response_model=MappingSaveResponse)
def save_mapping(payload: MappingSaveRequest) -> MappingSaveResponse:
    logger.info("Saving mapping rows for %s", payload.study_id)
    existing_rows = _load_mapping_rows()
    remaining = [row for row in existing_rows if row.get("study_id") != payload.study_id]

    new_rows: list[dict[str, Any]] = []
    for row in payload.rows:
        new_rows.append(
            {
                "study_id": payload.study_id,
                "var_code": row.var_code,
                "stage": row.stage,
                "brand": row.brand,
                "value_true_codes": row.value_true_codes,
            }
        )

    _write_mapping_rows(remaining + new_rows)
    return MappingSaveResponse(
        study_id=payload.study_id,
        saved_rows=len(new_rows),
        total_rows=len(remaining) + len(new_rows),
        path=str(_mapping_csv_path()),
    )
