from __future__ import annotations

from typing import Any

from src.search_ranking import (
    has_real_theme_match,
    should_apply_hard_keyword_filter,
    passes_person_filter,
    are_similar_tokens,
    text_similarity,
)

from src.document_field_normalizer import normalize_document_type,  normalize_text
from src.search_business_rules import extract_meaningful_keyword_tokens
from src.search_ranking import tokenize_text

def sort_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        results,
        key=lambda r: (
            1 if r.get("persona_match") else 0,
            r.get("persona_token_matches") or 0,
            1 if r.get("text_match") else 0,
            1 if r.get("tipo_match") else 0,
            r.get("boosted_score") or 0,
            r.get("score") or 0,
        ),
        reverse=True,
    )


def dedupe_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped_results = []
    seen_ids = set()

    for r in results:
        doc_id = r.get("id")
        if doc_id in seen_ids:
            continue
        seen_ids.add(doc_id)
        deduped_results.append(r)

    return deduped_results



def apply_person_filter(
    results: list[dict[str, Any]],
    persona_gpt: str | None,
) -> list[dict[str, Any]]:
    if not persona_gpt:
        return results

    persona_tokens = [t for t in tokenize_text(persona_gpt) if len(t) >= 2]
    total_tokens = len(persona_tokens)

    filtered = []

    for r in results:
        strong = r.get("persona_token_matches_strong", 0) or 0
        medium = r.get("persona_token_matches_medium", 0) or 0
        generic = r.get("persona_token_matches_generic", 0) or 0
        chunk = r.get("persona_token_matches_chunk", 0) or 0
        raw_ocr = r.get("raw_ocr_person_match", False)

        # query corta: es. "mohamed amine"
        if total_tokens <= 2:
            if strong >= total_tokens or chunk >= total_tokens or raw_ocr:
                filtered.append(r)
            continue

        # query più lunga: resta più severo
        combined_best = max(strong, medium, generic, chunk)
        if combined_best >= total_tokens or raw_ocr:
            filtered.append(r)

    return filtered
    
def is_tipo_business_filter(campo_target: str | None) -> bool:
    campo_target_norm = normalize_text(campo_target or "")
    return campo_target_norm in {"tipo documento", "tipo_documento"}


def is_date_business_filter(parsed_query: dict[str, Any]) -> bool:
    return (
        parsed_query.get("tipo_valore") == "date"
        and (
            parsed_query.get("data_da") is not None
            or parsed_query.get("data_a") is not None
            or parsed_query.get("operatore") in {">", ">=", "<", "<=", "between"}
        )
    )


def apply_business_field_filter(
    results: list[dict[str, Any]],
    parsed_query: dict[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    campo_target = parsed_query.get("campo_target")

    if not campo_target:
        return results, "skipped:no_field"

    if is_tipo_business_filter(campo_target):
        return results, "skipped:tipo_documento"

    if is_date_business_filter(parsed_query):
        return results, "skipped:date_already_applied"

    filtered = [r for r in results if r.get("structured_field_match") is True]
    return filtered, "applied"


def extract_tipo_condition_values(conditions: list[dict[str, Any]] | None) -> set[str]:
    tipo_condition_values = set()

    for cond in conditions or []:
        target_norm = normalize_text(str(cond.get("target") or ""))
        field_norm = normalize_text(str(cond.get("field") or ""))

        is_tipo_condition = (
            target_norm in {"tipo documento", "tipo_documento"}
            and field_norm in {"tipo documento", "tipo_documento"}
        )

        if not is_tipo_condition:
            continue

        value = cond.get("value")

        if isinstance(value, list):
            for v in value:
                v_norm = normalize_document_type(v) or normalize_text(v)
                if v_norm:
                    tipo_condition_values.add(v_norm)
        else:
            v_norm = normalize_document_type(value) or normalize_text(value)
            if v_norm:
                tipo_condition_values.add(v_norm)

    return tipo_condition_values


def is_theme_query(query: str) -> bool:
    q = normalize_text(query)

    return any(
        phrase in q
        for phrase in [
            "parlano di",
            "parla di",
            "che parlano di",
            "che parla di",
            "contengono",
            "contiene",
            "che contengono",
            "che contiene",
            "relative a",
            "relativi a",
        ]
    )


def get_effective_keywords(
    *,
    keywords_filter: list[str],
    conditions: list[dict[str, Any]] | None,
    query: str,
) -> list[str]:
    tipo_condition_values = extract_tipo_condition_values(conditions)

    themed_query = is_theme_query(query)
    effective_keywords = []

    for kw in keywords_filter or []:
        kw_norm = normalize_text(kw)
        kw_tipo_norm = normalize_document_type(kw) or kw_norm
        meaningful_tokens = extract_meaningful_keyword_tokens(kw_norm)

        skip_as_pure_tipo = (
            kw_tipo_norm in tipo_condition_values
            and (
                kw_norm == kw_tipo_norm
                or not meaningful_tokens
                or set(meaningful_tokens) == {kw_tipo_norm}
            )
        )

        if skip_as_pure_tipo and not themed_query:
            continue

        effective_keywords.append(kw)

    return effective_keywords


def apply_keyword_filter(
    results: list[dict[str, Any]],
    *,
    effective_keywords: list[str],
    parsed_query: dict[str, Any],
    persona_gpt: str | None,
) -> tuple[list[dict[str, Any]], str]:
    if not effective_keywords:
        return results, "skipped:no_keywords"

    apply_hard = should_apply_hard_keyword_filter(
        effective_keywords=effective_keywords,
        persona_query=persona_gpt,
        tipo_documento_query=parsed_query.get("tipo_documento"),
        campo_target=parsed_query.get("campo_target"),
    )

    if apply_hard:
        filtered = [r for r in results if has_real_theme_match(r)]
        return filtered, "applied:hard"

    themed_results = [r for r in results if has_real_theme_match(r)]

    if persona_gpt and not parsed_query.get("campo_target") and not parsed_query.get("tipo_documento"):
        if themed_results:
            return themed_results, "applied:soft_theme_match"
        return [], "applied:soft_no_match_empty"

    return results, "skipped:soft_ranking_only"


def compute_threshold(
    *,
    persona_gpt: str | None,
    keywords_filter: list[str],
    parsed_query: dict[str, Any],
) -> float:
    threshold = 0.55

    if persona_gpt:
        return 0.0

    if keywords_filter and not parsed_query.get("campo_target") and not parsed_query.get("tipo_documento"):
        return 0.0

    if keywords_filter and not parsed_query.get("campo_target"):
        return 0.35

    return threshold


def apply_threshold_filter(
    results: list[dict[str, Any]],
    threshold: float,
) -> list[dict[str, Any]]:
    return [r for r in results if (r.get("boosted_score") or 0) >= threshold]


def postprocess_search_results(
    results: list[dict[str, Any]],
    *,
    parsed_query: dict[str, Any],
    query: str,
    persona_gpt: str | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    debug_info: dict[str, Any] = {}

    results = sort_results(results)
    debug_info["after_initial_sort"] = len(results)

    results = apply_person_filter(results, persona_gpt)
    debug_info["after_person_filter"] = len(results)

    results, business_filter_status = apply_business_field_filter(results, parsed_query)
    debug_info["business_field_filter_status"] = business_filter_status
    debug_info["after_business_field_filter"] = len(results)

    keywords_filter = parsed_query.get("keywords") or []
    conditions = parsed_query.get("conditions") or []

    effective_keywords = get_effective_keywords(
        keywords_filter=keywords_filter,
        conditions=conditions,
        query=query,
    )
    debug_info["effective_keywords"] = effective_keywords

    results, keyword_filter_status = apply_keyword_filter(
        results,
        effective_keywords=effective_keywords,
        parsed_query=parsed_query,
        persona_gpt=persona_gpt,
    )
    debug_info["keyword_filter_status"] = keyword_filter_status
    debug_info["after_keyword_filter"] = len(results)

    threshold = compute_threshold(
        persona_gpt=persona_gpt,
        keywords_filter=keywords_filter,
        parsed_query=parsed_query,
    )
    debug_info["threshold"] = threshold

    results = apply_threshold_filter(results, threshold)
    debug_info["after_threshold"] = len(results)

    results = sort_results(results)
    results = dedupe_results(results)
    debug_info["after_dedupe"] = len(results)

    return results, debug_info