from __future__ import annotations

import csv
import io
import logging
import re
import duckdb
import pandas as pd
from fastapi import APIRouter, HTTPException, Query, Response

from app.data.warehouse import blob_exists, load_parquet_as_view, read_parquet_blob
from app.models.schemas import (
    MappingCandidate,
    MappingSuggestResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()

RULES: dict[str, str] = {
    "awareness": r"(conoce|ha\s+escuchado|conocimiento|awareness)",
    "consideration": r"(considera|consideraría|probable|intención|preferiría)",
    "purchase": r"(compr(ó|a)|adquir(ió|iría)|última\s+compra|purchase)",
}


def _variables_from_raw(study_id: str) -> pd.DataFrame:
    variables_key = f"warehouse/raw/study_id={study_id}/raw_variables.parquet"
    responses_key = f"warehouse/raw/study_id={study_id}/raw_responses.parquet"

    if blob_exists(variables_key):
        df = read_parquet_blob(variables_key)
        return df[["var_code", "question_text"]]

    if not blob_exists(responses_key):
        raise HTTPException(status_code=404, detail="Raw data not found for study.")

    conn = duckdb.connect()
    load_parquet_as_view(conn, "responses", responses_key)
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


# GET /mapping and POST /mapping/save were removed on 2026-09-02 together with the shared
# warehouse/mapping/question_map_v0.csv they read and rewrote. Neither had a caller in
# apps/web, and /mapping/save was actively dangerous: it rewrote the whole file through a
# 5-column writer, which would have silently dropped 4 columns from every OTHER study's
# rows. Per-study question mapping lives in app/routers/question_map.py, which only ever
# writes that one study's blob. See BITACORA.md 2026-09-02.

