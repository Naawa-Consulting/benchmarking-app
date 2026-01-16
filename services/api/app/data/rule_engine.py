from __future__ import annotations

import json
import logging
import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd

from app.data.warehouse import get_repo_root

logger = logging.getLogger(__name__)


DEFAULT_RULES = {
    "version": 1,
    "stage_rules": [
        {
            "id": "awareness_listed_brands",
            "stage": "awareness",
            "question_text_regex": r"Del siguiente listado.*conoces\?",
            "var_code_regex": None,
            "priority": 100,
        }
    ],
    "brand_extractors": [
        {
            "id": "brand_after_question_mark",
            "applies_if_question_text_regex": r"Del siguiente listado.*conoces\?",
            "extract_regex": r"\?\s*(.+)$",
            "extract_group": 1,
            "normalize": True,
        }
    ],
    "ignore_rules": [
        {
            "id": "demographics_common",
            "question_text_regex": r"(Edad|Sexo|Regi?n|NSE|Estado|Municipio)",
            "var_code_regex": r"^(Edad|Sexo|Region|Regi[o?]n|NSE|Estado|Municipio)$",
        }
    ],
    "touchpoint_rules": [
        {
            "id": "tp_tv",
            "touchpoint": "TV",
            "question_regex": r"Anuncios en TV|Televisi[o?]n|TV",
            "var_code_regex": None,
            "priority": 90,
        },
        {
            "id": "tp_radio",
            "touchpoint": "Radio",
            "question_regex": r"Radio",
            "var_code_regex": None,
            "priority": 90,
        },
        {
            "id": "tp_internet",
            "touchpoint": "Internet",
            "question_regex": r"Internet|Web|Sitio web",
            "var_code_regex": None,
            "priority": 80,
        },
        {
            "id": "tp_facebook",
            "touchpoint": "Facebook",
            "question_regex": r"Facebook|Anuncios en Facebook",
            "var_code_regex": None,
            "priority": 100,
        },
        {
            "id": "tp_instagram",
            "touchpoint": "Instagram",
            "question_regex": r"Instagram",
            "var_code_regex": None,
            "priority": 100,
        },
        {
            "id": "tp_tiktok",
            "touchpoint": "TikTok",
            "question_regex": r"Tik-?Tok",
            "var_code_regex": None,
            "priority": 100,
        },
        {
            "id": "tp_youtube",
            "touchpoint": "YouTube",
            "question_regex": r"You\s?Tube|YouTube",
            "var_code_regex": None,
            "priority": 100,
        },
        {
            "id": "tp_search",
            "touchpoint": "Search",
            "question_regex": r"Google|Buscador|Search",
            "var_code_regex": None,
            "priority": 90,
        },
        {
            "id": "tp_ooh",
            "touchpoint": "OOH",
            "question_regex": r"Exterior|OOH|Vallas|Espectaculares",
            "var_code_regex": None,
            "priority": 80,
        },
    ],
    "defaults": {
        "value_true_codes": "1",
    },
}


def _rules_path() -> Path:
    return get_repo_root() / "data" / "warehouse" / "mapping" / "rules_v1.json"


def _study_rules_path(study_id: str) -> Path:
    return (
        get_repo_root()
        / "data"
        / "warehouse"
        / "mapping"
        / "study_rules"
        / f"study_id={study_id}.json"
    )


def load_rules() -> dict[str, Any]:
    path = _rules_path()
    if not path.exists():
        save_rules(DEFAULT_RULES)
        return DEFAULT_RULES
    try:
        rules = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        rules = json.loads(path.read_text(encoding="utf-8-sig"))
    if "touchpoint_rules" not in rules:
        rules["touchpoint_rules"] = DEFAULT_RULES.get("touchpoint_rules", [])
    return rules


def save_rules(rules: dict[str, Any]) -> Path:
    path = _rules_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rules, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _default_scope(rules: dict[str, Any], study_id: str) -> dict[str, Any]:
    return {
        "study_id": study_id,
        "enabled_stage_rules": [rule.get("id") for rule in rules.get("stage_rules", [])],
        "enabled_brand_extractors": [rule.get("id") for rule in rules.get("brand_extractors", [])],
        "enabled_ignore_rules": [rule.get("id") for rule in rules.get("ignore_rules", [])],
    }


def load_study_rule_scope(study_id: str, rules: dict[str, Any]) -> dict[str, Any]:
    path = _study_rules_path(study_id)
    if not path.exists():
        return _default_scope(rules, study_id)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    data.setdefault("study_id", study_id)
    data.setdefault("enabled_stage_rules", _default_scope(rules, study_id)["enabled_stage_rules"])
    data.setdefault(
        "enabled_brand_extractors", _default_scope(rules, study_id)["enabled_brand_extractors"]
    )
    data.setdefault("enabled_ignore_rules", _default_scope(rules, study_id)["enabled_ignore_rules"])
    return data


def save_study_rule_scope(
    study_id: str, scope: dict[str, Any], rules: dict[str, Any]
) -> Path:
    valid_stage = {rule.get("id") for rule in rules.get("stage_rules", [])}
    valid_brand = {rule.get("id") for rule in rules.get("brand_extractors", [])}
    valid_ignore = {rule.get("id") for rule in rules.get("ignore_rules", [])}

    stage_ids = [rule_id for rule_id in scope.get("enabled_stage_rules", []) if rule_id in valid_stage]
    brand_ids = [rule_id for rule_id in scope.get("enabled_brand_extractors", []) if rule_id in valid_brand]
    ignore_ids = [rule_id for rule_id in scope.get("enabled_ignore_rules", []) if rule_id in valid_ignore]

    payload = {
        "study_id": study_id,
        "enabled_stage_rules": stage_ids,
        "enabled_brand_extractors": brand_ids,
        "enabled_ignore_rules": ignore_ids,
    }

    path = _study_rules_path(study_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def filter_rules_by_scope(rules: dict[str, Any], scope: dict[str, Any]) -> dict[str, Any]:
    stage_ids = set(scope.get("enabled_stage_rules", []))
    brand_ids = set(scope.get("enabled_brand_extractors", []))
    ignore_ids = set(scope.get("enabled_ignore_rules", []))

    filtered = dict(rules)
    filtered["stage_rules"] = [
        rule for rule in rules.get("stage_rules", []) if rule.get("id") in stage_ids
    ]
    filtered["brand_extractors"] = [
        rule for rule in rules.get("brand_extractors", []) if rule.get("id") in brand_ids
    ]
    filtered["ignore_rules"] = [
        rule for rule in rules.get("ignore_rules", []) if rule.get("id") in ignore_ids
    ]
    return filtered


def _normalize_brand(value: str) -> str:
    collapsed = " ".join(value.strip().split())
    return collapsed.title()

def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _simplify_pattern(pattern: str) -> str:
    stripped = _strip_accents(pattern)
    return "".join(char if ord(char) < 128 else "." for char in stripped)


def _simplify_text(text: str) -> str:
    stripped = _strip_accents(text)
    return stripped.encode("ascii", "ignore").decode("ascii")

def _unescape_pattern(pattern: str) -> str:
    if "\\\\" in pattern:
        return pattern.replace("\\\\", "\\")
    return pattern


def _safe_search(pattern: str | None, text: str) -> bool:
    if not pattern:
        return False
    if re.search(pattern, text, flags=re.IGNORECASE | re.UNICODE):
        return True
    try:
        unescaped = _unescape_pattern(pattern)
        if unescaped != pattern and re.search(unescaped, text, flags=re.IGNORECASE | re.UNICODE):
            return True
    except re.error:
        return False
    try:
        stripped_pattern = _strip_accents(pattern)
        stripped_text = _strip_accents(text)
        if re.search(stripped_pattern, stripped_text, flags=re.IGNORECASE | re.UNICODE):
            return True
        simplified_pattern = _simplify_pattern(pattern)
        simplified_text = _simplify_text(text)
        return bool(re.search(simplified_pattern, simplified_text, flags=re.IGNORECASE | re.UNICODE))
    except re.error:
        return False


def _safe_extract(pattern: str, text: str, group: int) -> str | None:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.UNICODE)
    if not match:
        try:
            unescaped = _unescape_pattern(pattern)
            if unescaped != pattern:
                match = re.search(unescaped, text, flags=re.IGNORECASE | re.UNICODE)
                if match:
                    try:
                        value = match.group(group)
                    except IndexError:
                        return None
                    return value.strip() if value else None
            match = re.search(
                _strip_accents(pattern), _strip_accents(text), flags=re.IGNORECASE | re.UNICODE
            )
            if not match:
                match = re.search(
                    _simplify_pattern(pattern),
                    _simplify_text(text),
                    flags=re.IGNORECASE | re.UNICODE,
                )
        except re.error:
            return None
        if not match:
            return None
    try:
        value = match.group(group)
    except IndexError:
        return None
    return value.strip() if value else None


def _fallback_brand(question_text: str) -> str | None:
    if not question_text:
        return None
    fallback_patterns = [
        r"\?\s*(.+)$",
        r"30\s*d.{0,2}as\s+(.+)$",
    ]
    for pattern in fallback_patterns:
        extracted = _safe_extract(pattern, question_text, 1)
        if extracted:
            return _normalize_brand(extracted)
    return None


def _extract_brand_with_rule(rule: dict[str, Any], question_text: str) -> str | None:
    mode = str(rule.get("mode") or "regex")
    extract_group = int(rule.get("extract_group", 1))
    if mode == "between":
        between = rule.get("between") or {}
        left = between.get("left")
        right = between.get("right")
        if not left or not right:
            return None
        pattern = f"{left}(.+?)(?:{right})"
        group = int(between.get("group", 1))
        return _safe_extract(pattern, question_text, group)
    if mode == "start":
        pattern = rule.get("extract_regex") or r"^\s*(.+?)\s*[-–—:]"
        return _safe_extract(pattern, question_text, extract_group)
    if mode == "end":
        pattern = rule.get("extract_regex") or r"\?\s*(.+)$"
        return _safe_extract(pattern, question_text, extract_group)
    pattern = rule.get("extract_regex")
    if not pattern:
        return None
    return _safe_extract(pattern, question_text, extract_group)


def _select_best_rule(
    rules: list[dict[str, Any]], question_text: str, var_code: str
) -> dict[str, Any] | None:
    matched_rules: list[tuple[dict[str, Any], int]] = []
    for rule in rules:
        question_pattern = rule.get("question_regex") or rule.get("question_text_regex")
        var_pattern = rule.get("var_code_regex")
        if question_pattern:
            if not _safe_search(question_pattern, question_text):
                continue
            matched_rules.append((rule, 2))
            continue
        if _safe_search(var_pattern, var_code):
            matched_rules.append((rule, 1))

    if not matched_rules:
        return None
    matched_rules.sort(
        key=lambda item: (int(item[0].get("priority", 0)), item[1]),
        reverse=True,
    )
    return matched_rules[0][0]


def infer_question_mapping(question_text: str, var_code: str, rules: dict[str, Any]) -> dict[str, Any]:
    stage_rules = rules.get("stage_rules", [])
    brand_extractors = rules.get("brand_extractors", [])
    ignore_rules = rules.get("ignore_rules", [])
    touchpoint_rules = rules.get("touchpoint_rules", [])
    defaults = rules.get("defaults", {}) or {}

    for rule in ignore_rules:
        if _safe_search(rule.get("question_text_regex"), question_text) or _safe_search(
            rule.get("var_code_regex"), var_code
        ):
            return {"ignored": True}

    stage_rule = _select_best_rule(stage_rules, question_text, var_code)
    stage = stage_rule.get("stage") if stage_rule else None

    touchpoint_rule = _select_best_rule(touchpoint_rules, question_text, var_code)
    touchpoint = touchpoint_rule.get("touchpoint") if touchpoint_rule else None
    touchpoint_rule_id = touchpoint_rule.get("id") if touchpoint_rule else None

    brand: str | None = None
    brand_extractors_sorted = sorted(
        brand_extractors, key=lambda item: int(item.get("priority", 0)), reverse=True
    )
    for extractor in brand_extractors_sorted:
        applies_pattern = extractor.get("applies_if_question_regex") or extractor.get(
            "applies_if_question_text_regex"
        )
        if applies_pattern and not _safe_search(applies_pattern, question_text):
            continue
        extracted = _extract_brand_with_rule(extractor, question_text)
        if extracted:
            brand = _normalize_brand(extracted) if extractor.get("normalize") else extracted
            break

    if brand is None:
        brand = _fallback_brand(question_text)

    return {
        "ignored": False,
        "stage": stage,
        "brand": brand,
        "touchpoint": touchpoint,
        "touchpoint_rule_id": touchpoint_rule_id,
        "value_true_codes": defaults.get("value_true_codes", "1"),
    }


def apply_rules_to_variables(
    df_vars: pd.DataFrame, rules: dict[str, Any]
) -> tuple[pd.DataFrame, dict[str, Any]]:
    mapped_rows: list[dict[str, Any]] = []
    unmapped_rows: list[dict[str, Any]] = []
    ignored_rows: list[dict[str, Any]] = []
    touchpoint_mapped_rows = 0

    for _, row in df_vars.iterrows():
        var_code = str(row.get("var_code", "") or "")
        question_text = row.get("question_text")
        question_text_str = str(question_text) if question_text is not None else ""
        mapping = infer_question_mapping(question_text_str, var_code, rules)

        if mapping.get("ignored"):
            ignored_rows.append(
                {
                    "var_code": var_code,
                    "question_text": question_text_str or None,
                }
            )
            continue

        if not mapping.get("stage"):
            unmapped_rows.append(
                {
                    "var_code": var_code,
                    "question_text": question_text_str or None,
                }
            )
            continue

        if mapping.get("touchpoint"):
            touchpoint_mapped_rows += 1

        mapped_rows.append(
            {
                "var_code": var_code,
                "stage": mapping.get("stage"),
                "brand": mapping.get("brand"),
                "touchpoint": mapping.get("touchpoint"),
                "touchpoint_rule_id": mapping.get("touchpoint_rule_id"),
                "question_text": question_text_str or None,
                "value_true_codes": mapping.get("value_true_codes", "1"),
            }
        )

    mapped_df = pd.DataFrame(mapped_rows)
    stats = {
        "mapped_rows": len(mapped_rows),
        "unmapped_rows": len(unmapped_rows),
        "ignored_rows": len(ignored_rows),
        "touchpoint_mapped_rows": touchpoint_mapped_rows,
        "examples": {
            "mapped": mapped_rows[:5],
            "unmapped": unmapped_rows[:5],
            "ignored": ignored_rows[:5],
        },
    }
    return mapped_df, stats
