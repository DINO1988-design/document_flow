# src/search_ranking.py

from __future__ import annotations

import json
import os
import re
from difflib import SequenceMatcher

from src.document_field_flattener import flatten_document_fields
from src.search_business_rules import (
    evaluate_structured_field_match,
    compute_weighted_keyword_coverage,
    extract_meaningful_keyword_tokens,
)
from src.document_field_normalizer import (
    normalize_text,
    normalize_document_type,
    document_type_matches,
)


def tokenize_text(value: str) -> list[str]:
    norm = normalize_text(value)
    if not norm:
        return []
    return [token for token in norm.split() if len(token) > 1]


def text_similarity(a: str, b: str) -> float:
    a_norm = normalize_text(a)
    b_norm = normalize_text(b)

    if not a_norm or not b_norm:
        return 0.0

    return SequenceMatcher(None, a_norm, b_norm).ratio()


def token_fuzzy_match(token: str, text: str, min_ratio: float = 0.82) -> bool:
    token_norm = normalize_text(token)
    text_norm = normalize_text(text)

    if not token_norm or not text_norm:
        return False

    words = text_norm.split()

    for w in words:
        if w == token_norm:
            return True

        if SequenceMatcher(None, token_norm, w).ratio() >= min_ratio:
            return True

    return False


def are_similar_tokens(a: str, b: str, threshold: float = 0.84) -> bool:
    a = normalize_text(a)
    b = normalize_text(b)

    if not a or not b:
        return False

    if a == b:
        return True

    return SequenceMatcher(None, a, b).ratio() >= threshold


def count_similar_token_matches(
    query_tokens: list[str],
    target_text: str,
    threshold: float = 0.84
) -> int:
    if not query_tokens or not target_text:
        return 0

    target_tokens = tokenize_text(target_text)
    if not target_tokens:
        return 0

    matched = 0
    used_targets = set()

    for q in query_tokens:
        for i, t in enumerate(target_tokens):
            if i in used_targets:
                continue

            if are_similar_tokens(q, t, threshold=threshold):
                matched += 1
                used_targets.add(i)
                break

    return matched


def raw_ocr_person_match(persona: str, raw_text: str) -> bool:
    persona_norm = normalize_text(persona)
    raw_norm = normalize_text(raw_text)

    if not persona_norm or not raw_norm:
        return False

    persona_tokens = tokenize_text(persona_norm)
    if not persona_tokens:
        return False

    # Query mononimo: match più severo
    if len(persona_tokens) == 1:
        token = persona_tokens[0]

        if re.search(rf"\b{re.escape(token)}\b", raw_norm, re.IGNORECASE):
            return True

        matched = count_similar_token_matches([token], raw_norm, threshold=0.92)
        return matched >= 1

    # Query 2+ token
    if persona_norm in raw_norm:
        return True

    matched = count_similar_token_matches(persona_tokens, raw_norm, threshold=0.86)
    return matched >= min(2, len(persona_tokens))


def build_searchable_text(tipo_documento: str, fields: dict) -> str:
    parts = []

    if tipo_documento:
        parts.append(str(tipo_documento))

    flat_fields = flatten_document_fields(fields or {})

    for k, v in flat_fields.items():
        parts.append(str(k))

        if v in [None, "", []]:
            continue

        if isinstance(v, (dict, list)):
            try:
                v = json.dumps(v, ensure_ascii=False)
            except Exception:
                v = str(v)

        parts.append(str(v))

    return normalize_text(" ".join(parts))


def get_normalized_filename(file_path: str) -> str:
    if not file_path:
        return ""
    return normalize_text(os.path.basename(file_path))


def looks_like_person_name(value: str) -> bool:
    if value is None:
        return False

    value = str(value).strip()
    if not value:
        return False

    if len(value) < 3 or len(value) > 80:
        return False

    if not re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", value):
        return False

    if re.fullmatch(r"[\d\s./,_\-|:]+", value):
        return False

    digit_count = sum(ch.isdigit() for ch in value)
    if digit_count > 4:
        return False

    words = [w for w in re.split(r"\s+", value) if w]
    if len(words) < 1 or len(words) > 6:
        return False

    alpha_words = [
        w for w in words
        if re.search(r"[A-Za-zÀ-ÖØ-öø-ÿ]", w)
    ]
    if not alpha_words:
        return False

    return True


def classify_person_field_key(field_key: str) -> str | None:
    key_norm = normalize_text(field_key)

    if not key_norm:
        return None

    tokens = set(key_norm.split())

    non_person_patterns = [
        "azienda nome",
        "ditta nome",
        "datore lavoro nome",
        "datore di lavoro nome",
        "emittente nome",
        "legal contact",
        "studio nome",
        "studi nome",
        "banca nome",
        "booking agency",
        "ufficio emissione nome",
        "titolare trattamento",
    ]

    for pattern in non_person_patterns:
        if pattern in key_norm:
            return None

    strong_exact_patterns = [
        "nome completo",
        "full name",
        "passenger name",
        "traveller name",
        "traveler name",
        "guest name",
        "customer name",
        "employee name",
        "holder name",
        "applicant name",
        "beneficiario nome",
        "ordinante nome",
        "lavoratore nome",
        "lavoratore cognome nome",
        "dipendente nome",
        "contraente nome",
        "destinatario nome",
        "persona nome",
        "nome cognome",
        "cognome nome",
        "cognome_nome",
        "nome_completo",
    ]

    for pattern in strong_exact_patterns:
        if pattern in key_norm:
            return "strong"

    strong_last_token_patterns = {
        "name",
        "full_name",
        "nome",
        "nome_completo",
        "cognome_nome",
        "passenger_name",
        "traveller_name",
        "traveler_name",
        "employee_name",
        "holder_name",
        "applicant_name",
        "guest_name",
        "customer_name",
    }

    raw_last_token = field_key.split(".")[-1].strip().lower()
    if raw_last_token in strong_last_token_patterns:
        return "strong"

    person_container_tokens = {
        "dipendente", "lavoratore", "passenger", "traveller", "traveler",
        "guest", "customer", "cliente", "beneficiario", "ordinante",
        "contraente", "applicant", "holder", "persona", "recipient",
        "destinatario"
    }

    if ("nome" in tokens or "name" in tokens) and (tokens & person_container_tokens):
        return "strong"

    if ("cognome" in tokens and "nome" in tokens):
        return "strong"

    medium_patterns = [
        "first name",
        "last name",
        "surname",
        "given name",
        "given_names",
        "recipient",
        "destinatario",
        "cliente",
        "passeggero",
        "traveler",
        "traveller",
        "guest",
        "employee",
        "worker",
        "dipendente",
        "lavoratore",
        "contraente",
        "beneficiario",
        "ordinante",
    ]

    for pattern in medium_patterns:
        if pattern in key_norm:
            return "medium"

    if ("name" in tokens or "nome" in tokens) and any(
        x in key_norm for x in [
            "passenger", "traveller", "traveler", "guest",
            "customer", "cliente", "beneficiario", "ordinante",
            "dipendente", "lavoratore"
        ]
    ):
        return "medium"

    return None


def extract_field_groups(fields: dict) -> dict:
    person_values_strong = []
    person_values_medium = []
    generic_values = []

    flat_fields = flatten_document_fields(fields or {})

    for k, v in flat_fields.items():
        if v in [None, "", []]:
            continue

        value_str = str(v)
        key_class = classify_person_field_key(k)

        if key_class == "strong":
            person_values_strong.append(value_str)
        elif key_class == "medium":
            person_values_medium.append(value_str)

        generic_values.append(f"{k} {value_str}")

    return {
        "person_strong_text": normalize_text(" ".join(person_values_strong)),
        "person_medium_text": normalize_text(" ".join(person_values_medium)),
        "generic_text": normalize_text(" ".join(generic_values)),
    }


def has_fuzzy_person_match(fields: dict, person_tokens: list[str]) -> bool:
    if not fields or not person_tokens:
        return False

    field_groups = extract_field_groups(fields)

    strong_text = field_groups["person_strong_text"]
    medium_text = field_groups["person_medium_text"]
    generic_text = field_groups["generic_text"]

    strong_matches = count_similar_token_matches(person_tokens, strong_text, threshold=0.84)
    medium_matches = count_similar_token_matches(person_tokens, medium_text, threshold=0.84)
    generic_matches = count_similar_token_matches(person_tokens, generic_text, threshold=0.88)

    total_tokens = len(person_tokens)

    if total_tokens == 1:
        return (strong_matches + medium_matches + generic_matches) > 0

    return (
        strong_matches >= 1
        or medium_matches >= 2
        or (strong_matches + medium_matches) >= 2
        or (strong_matches + medium_matches + generic_matches) >= 2
    )


def is_generic_document_type(value: str) -> bool:
    v = normalize_text(value)
    return v in {
        "",
        "generico",
        "documento",
        "documenti",
        "doc",
        "docs",
        "file",
        "files",
        "altro",
        "atto",
        "atti",
        "pratica",
        "pratiche",
        "modulo",
        "moduli",
    }


def has_meaningful_keywords(
    keywords: list[str] | None,
    tipo_documento: str | None = None,
    persona: str | None = None,
    nome_file: str | None = None
) -> bool:
    kws = [normalize_text(k) for k in (keywords or []) if normalize_text(k)]

    tipo_norm = normalize_text(tipo_documento)
    persona_norm = normalize_text(persona)
    file_norm = normalize_text(nome_file)

    filtered = []
    for kw in kws:
        if kw == tipo_norm:
            continue
        if kw == persona_norm:
            continue
        if kw == file_norm:
            continue
        filtered.append(kw)

    return len(filtered) > 0


def should_apply_hard_keyword_filter(
    effective_keywords: list[str] | None,
    persona_query: str | None = None,
    tipo_documento_query: str | None = None,
    campo_target: str | None = None,
) -> bool:
    if not effective_keywords:
        return False

    if campo_target:
        return True

    if tipo_documento_query:
        return True

    if not persona_query:
        return True

    return False


def has_real_theme_match(result: dict) -> bool:
    return bool(
        result.get("keyword_match")
        or result.get("text_match")
        or result.get("tipo_match")
        or result.get("structured_field_match")
    )


def passes_person_filter(result: dict, persona_query: str | None) -> bool:
    if not persona_query:
        return True

    person_tokens = tokenize_text(persona_query)
    total_tokens = len(person_tokens)

    strong = result.get("persona_token_matches_strong", 0) or 0
    medium = result.get("persona_token_matches_medium", 0) or 0
    generic = result.get("persona_token_matches_generic", 0) or 0
    chunk = result.get("persona_token_matches_chunk", 0) or 0
    raw_ocr = result.get("raw_ocr_person_match", False)

    best_signal = max(strong, medium, generic, chunk)
    total_signal = strong + medium + generic + chunk

    if total_tokens <= 1:
        return best_signal >= 1

    if total_tokens == 2:
        return best_signal >= 2 or (best_signal >= 1 and raw_ocr)

    return total_signal >= 2 or raw_ocr


SEARCH_SCORE_WEIGHTS = {
    "semantic_weight": 0.45,
    "person_strong_bonus": 0.55,
    "person_strong_coverage_weight": 0.25,
    "person_medium_bonus": 0.30,
    "person_medium_coverage_weight": 0.15,
    "person_generic_bonus": 0.18,
    "person_generic_coverage_weight": 0.10,
    "person_chunk_bonus": 0.22,
    "person_chunk_coverage_weight": 0.12,
    "raw_ocr_bonus": 0.28,
    "filename_token_bonus_per_match": 0.04,
    "filename_token_bonus_cap": 0.08,
    "full_person_phrase_strong_bonus": 0.20,
    "full_person_phrase_medium_bonus": 0.10,
    "full_person_phrase_generic_bonus": 0.06,
    "full_person_phrase_filename_bonus": 0.03,
    "penalty_no_person": -0.45,
    "penalty_filename_only": -0.15,
    "tipo_match_bonus": 0.22,
    "tipo_wrong_penalty": -0.25,
    "text_match_bonus": 0.12,
    "query_token_coverage_cap": 0.12,
    "explicit_filename_bonus": 0.10,
    "penalty_low_signal_person_query": -0.20,
    "penalty_no_real_signal": -0.30,
    "structured_field_match_bonus": 0.40,
    "structured_field_exact_bonus": 0.20,
    "structured_field_partial_bonus": 0.12,
    "structured_field_wrong_penalty": -0.18,
}


def compute_document_score(
    query,
    parsed_query,
    row,
    fields,
    semantic_score=None,
    best_chunk_text=None,
    raw_ocr_text=None,
    weights=None,
    document_frequency=None
) -> dict:
    weights = weights or SEARCH_SCORE_WEIGHTS

    query_norm = normalize_text(query)
    keywords_from_query = parsed_query.get("keywords") or []
    normalized_keywords = [normalize_text(k) for k in keywords_from_query if normalize_text(k)]

    keyword_match = False
    keyword_match_count = 0
    keyword_bonus = 0.0
    keyword_penalty = 0.0

    tipo_doc_gpt = normalize_text(parsed_query.get("tipo_documento"))
    persona_gpt = normalize_text(parsed_query.get("persona"))
    nome_file_gpt = normalize_text(parsed_query.get("nome_file"))

    tipo_documento = row.get("tipo_documento") or ""
    file_path = row.get("file_path") or ""

    file_name_norm = get_normalized_filename(file_path)
    searchable_text = build_searchable_text(tipo_documento, fields)
    field_groups = extract_field_groups(fields)

    strong_person_text = field_groups["person_strong_text"]
    medium_person_text = field_groups["person_medium_text"]
    generic_text = field_groups["generic_text"]

    person_tokens = tokenize_text(persona_gpt) if persona_gpt else []

    semantic_score = semantic_score or 0.0
    score = 0.0

    semantic_base = 0.0
    person_strong_bonus = 0.0
    person_strong_coverage_bonus = 0.0
    person_medium_bonus = 0.0
    person_medium_coverage_bonus = 0.0
    person_generic_bonus = 0.0
    person_generic_coverage_bonus = 0.0
    person_chunk_bonus = 0.0
    person_chunk_coverage_bonus = 0.0
    raw_ocr_bonus = 0.0
    filename_bonus = 0.0
    full_person_phrase_bonus = 0.0
    tipo_score_adjustment = 0.0
    text_match_bonus = 0.0
    query_token_coverage_bonus = 0.0
    explicit_filename_bonus = 0.0
    penalty_no_person = 0.0
    penalty_filename_only = 0.0
    penalty_low_signal_person_query = 0.0
    penalty_no_real_signal = 0.0

    campo_target = parsed_query.get("campo_target")
    operatore_target = parsed_query.get("operatore")
    valore_target = parsed_query.get("valore_target")
    tipo_valore_target = parsed_query.get("tipo_valore")
    data_da_target = parsed_query.get("data_da")
    data_a_target = parsed_query.get("data_a")

    structured_field_bonus = 0.0
    structured_field_exact_bonus = 0.0
    structured_field_partial_bonus = 0.0
    structured_field_penalty = 0.0
    structured_field_match = False
    structured_field_info = None

    matched_person_tokens_strong = count_similar_token_matches(person_tokens, strong_person_text, threshold=0.84)
    matched_person_tokens_medium = count_similar_token_matches(person_tokens, medium_person_text, threshold=0.84)
    matched_person_tokens_generic = count_similar_token_matches(person_tokens, generic_text, threshold=0.88)
    matched_person_tokens_filename = count_similar_token_matches(person_tokens, file_name_norm, threshold=0.90)
    matched_person_tokens_chunk = count_similar_token_matches(person_tokens, best_chunk_text or "", threshold=0.84)
    raw_ocr_match = raw_ocr_person_match(persona_gpt, raw_ocr_text or "") if persona_gpt else False

    total_person_tokens = len(person_tokens)

    person_coverage_strong = (
        matched_person_tokens_strong / total_person_tokens if total_person_tokens else 0.0
    )
    person_coverage_medium = (
        matched_person_tokens_medium / total_person_tokens if total_person_tokens else 0.0
    )
    person_coverage_generic = (
        matched_person_tokens_generic / total_person_tokens if total_person_tokens else 0.0
    )
    person_coverage_filename = (
        matched_person_tokens_filename / total_person_tokens if total_person_tokens else 0.0
    )
    person_coverage_chunk = (
        matched_person_tokens_chunk / total_person_tokens if total_person_tokens else 0.0
    )

    persona_match = (
        matched_person_tokens_strong > 0
        or matched_person_tokens_medium > 0
        or matched_person_tokens_generic > 0
        or matched_person_tokens_chunk > 0
        or raw_ocr_match
    )
    filename_only_match = matched_person_tokens_filename > 0 and not persona_match

    semantic_base = semantic_score * weights["semantic_weight"]
    score += semantic_base

    if matched_person_tokens_strong > 0:
        person_strong_bonus = weights["person_strong_bonus"]
        person_strong_coverage_bonus = (
            person_coverage_strong * weights["person_strong_coverage_weight"]
        )
        score += person_strong_bonus
        score += person_strong_coverage_bonus

    if matched_person_tokens_medium > 0:
        person_medium_bonus = weights["person_medium_bonus"]
        person_medium_coverage_bonus = (
            person_coverage_medium * weights["person_medium_coverage_weight"]
        )
        score += person_medium_bonus
        score += person_medium_coverage_bonus

    if matched_person_tokens_generic > 0:
        person_generic_bonus = weights["person_generic_bonus"]
        person_generic_coverage_bonus = (
            person_coverage_generic * weights["person_generic_coverage_weight"]
        )
        score += person_generic_bonus
        score += person_generic_coverage_bonus

    if matched_person_tokens_chunk > 0:
        person_chunk_bonus = weights["person_chunk_bonus"]
        person_chunk_coverage_bonus = (
            person_coverage_chunk * weights["person_chunk_coverage_weight"]
        )
        score += person_chunk_bonus
        score += person_chunk_coverage_bonus

    if raw_ocr_match:
        raw_ocr_bonus = weights["raw_ocr_bonus"]
        score += raw_ocr_bonus

    if matched_person_tokens_filename > 0:
        filename_bonus = min(
            weights["filename_token_bonus_per_match"] * matched_person_tokens_filename,
            weights["filename_token_bonus_cap"]
        )
        score += filename_bonus

    if persona_gpt:
        if persona_gpt in strong_person_text or text_similarity(persona_gpt, strong_person_text) >= 0.84:
            full_person_phrase_bonus = weights["full_person_phrase_strong_bonus"]
            score += full_person_phrase_bonus
        elif persona_gpt in medium_person_text or text_similarity(persona_gpt, medium_person_text) >= 0.84:
            full_person_phrase_bonus = weights["full_person_phrase_medium_bonus"]
            score += full_person_phrase_bonus
        elif persona_gpt in generic_text or text_similarity(persona_gpt, generic_text) >= 0.88:
            full_person_phrase_bonus = weights["full_person_phrase_generic_bonus"]
            score += full_person_phrase_bonus
        elif persona_gpt in file_name_norm or text_similarity(persona_gpt, file_name_norm) >= 0.90:
            full_person_phrase_bonus = weights["full_person_phrase_filename_bonus"]
            score += full_person_phrase_bonus

    if person_tokens and not persona_match:
        penalty_no_person = weights["penalty_no_person"]
        score += penalty_no_person

    if filename_only_match:
        penalty_filename_only = weights["penalty_filename_only"]
        score += penalty_filename_only

    tipo_match = False

    if tipo_doc_gpt:
        if document_type_matches(tipo_doc_gpt, tipo_documento):
            tipo_match = True
            tipo_score_adjustment = weights["tipo_match_bonus"]
            score += tipo_score_adjustment
        elif is_generic_document_type(tipo_documento):
            tipo_score_adjustment = 0.0
        else:
            tipo_score_adjustment = weights["tipo_wrong_penalty"]
            score += tipo_score_adjustment

    if campo_target:
        structured_field_info = evaluate_structured_field_match(
            fields=fields,
            canonical_field=campo_target,
            operator=operatore_target,
            target_value=valore_target,
            value_type=tipo_valore_target,
            date_from=data_da_target,
            date_to=data_a_target
        )

        if structured_field_info.get("matched"):
            structured_field_match = True
            structured_field_bonus = weights["structured_field_match_bonus"]
            score += structured_field_bonus

            strength = structured_field_info.get("match_strength", 0.0) or 0.0

            if strength >= 0.999:
                structured_field_exact_bonus = weights["structured_field_exact_bonus"]
                score += structured_field_exact_bonus
            elif strength >= 0.60:
                structured_field_partial_bonus = weights["structured_field_partial_bonus"]
                score += structured_field_partial_bonus
        else:
            structured_field_penalty = weights["structured_field_wrong_penalty"]
            score += structured_field_penalty

    text_match = False

    if query_norm and query_norm in searchable_text:
        text_match = True

    if not text_match and normalized_keywords:
        for kw in normalized_keywords:
            if kw and kw in searchable_text:
                text_match = True
                break

    if not text_match and normalized_keywords and best_chunk_text:
        best_chunk_norm = normalize_text(best_chunk_text)
        for kw in normalized_keywords:
            if kw and kw in best_chunk_norm:
                text_match = True
                break

    if text_match:
        text_match_bonus = weights["text_match_bonus"]
        score += text_match_bonus

    keyword_sources = [
        searchable_text,
        best_chunk_text or "",
        raw_ocr_text or "",
        file_name_norm,
    ]

    if normalized_keywords:
        for kw in normalized_keywords:
            kw_norm = normalize_text(kw)
            if not kw_norm:
                continue

            meaningful_kw_tokens = extract_meaningful_keyword_tokens(kw_norm)
            if not meaningful_kw_tokens:
                continue

            found = False

            for src in keyword_sources:
                src_norm = normalize_text(src)
                if not src_norm:
                    continue

                if all(token in src_norm for token in meaningful_kw_tokens):
                    found = True
                    break

                coverage = compute_weighted_keyword_coverage(
                    kw_norm,
                    src_norm,
                    document_frequency or {}
                )

                if coverage >= 0.5:
                    found = True
                    break

            if found:
                keyword_match_count += 1

        if keyword_match_count > 0:
            keyword_match = True
            keyword_bonus = min(0.10 * keyword_match_count, 0.25)
            score += keyword_bonus
        else:
            keyword_penalty = -0.20
            score += keyword_penalty

    generic_query_tokens = {
        "documento", "documenti", "doc", "docs", "file", "files",
        "modulo", "moduli", "atto", "atti", "pratica", "pratiche"
    }

    query_tokens = [
        t for t in tokenize_text(query_norm)
        if t not in generic_query_tokens
    ]

    matched_query_tokens = sum(1 for t in query_tokens if t in searchable_text)
    query_token_coverage = (matched_query_tokens / len(query_tokens)) if query_tokens else 0.0

    if matched_query_tokens > 0:
        query_token_coverage_bonus = min(
            query_token_coverage * weights["query_token_coverage_cap"],
            weights["query_token_coverage_cap"]
        )
        score += query_token_coverage_bonus

    if nome_file_gpt and nome_file_gpt in file_name_norm:
        explicit_filename_bonus = weights["explicit_filename_bonus"]
        score += explicit_filename_bonus

    if person_tokens and not persona_match and semantic_score < 0.35:
        penalty_low_signal_person_query = weights["penalty_low_signal_person_query"]
        score += penalty_low_signal_person_query

    if (
        semantic_score <= 0.0
        and not persona_match
        and not text_match
        and not tipo_match
    ):
        penalty_no_real_signal = weights["penalty_no_real_signal"]
        score += penalty_no_real_signal

    return {
        "score": round(score, 4),
        "persona_match": persona_match,
        "raw_ocr_person_match": raw_ocr_match,
        "filename_only_match": filename_only_match,
        "keyword_match": keyword_match,
        "keyword_match_count": keyword_match_count,
        "persona_token_matches": (
            matched_person_tokens_strong
            + matched_person_tokens_medium
            + matched_person_tokens_generic
        ),
        "persona_token_matches_strong": matched_person_tokens_strong,
        "persona_token_matches_medium": matched_person_tokens_medium,
        "persona_token_matches_generic": matched_person_tokens_generic,
        "persona_token_matches_filename": matched_person_tokens_filename,
        "persona_token_matches_chunk": matched_person_tokens_chunk,
        "tipo_match": tipo_match,
        "text_match": text_match,
        "searchable_text": searchable_text,
        "structured_field_match": structured_field_match,
        "structured_field_info": structured_field_info,
        "score_breakdown": {
            "semantic_base": round(semantic_base, 4),
            "person_strong_bonus": round(person_strong_bonus, 4),
            "person_strong_coverage_bonus": round(person_strong_coverage_bonus, 4),
            "person_medium_bonus": round(person_medium_bonus, 4),
            "person_medium_coverage_bonus": round(person_medium_coverage_bonus, 4),
            "person_generic_bonus": round(person_generic_bonus, 4),
            "person_generic_coverage_bonus": round(person_generic_coverage_bonus, 4),
            "person_chunk_bonus": round(person_chunk_bonus, 4),
            "person_chunk_coverage_bonus": round(person_chunk_coverage_bonus, 4),
            "raw_ocr_bonus": round(raw_ocr_bonus, 4),
            "filename_bonus": round(filename_bonus, 4),
            "full_person_phrase_bonus": round(full_person_phrase_bonus, 4),
            "tipo_score_adjustment": round(tipo_score_adjustment, 4),
            "text_match_bonus": round(text_match_bonus, 4),
            "query_token_coverage_bonus": round(query_token_coverage_bonus, 4),
            "explicit_filename_bonus": round(explicit_filename_bonus, 4),
            "penalty_no_person": round(penalty_no_person, 4),
            "penalty_filename_only": round(penalty_filename_only, 4),
            "penalty_low_signal_person_query": round(penalty_low_signal_person_query, 4),
            "penalty_no_real_signal": round(penalty_no_real_signal, 4),
            "structured_field_bonus": round(structured_field_bonus, 4),
            "structured_field_exact_bonus": round(structured_field_exact_bonus, 4),
            "structured_field_partial_bonus": round(structured_field_partial_bonus, 4),
            "structured_field_penalty": round(structured_field_penalty, 4),
            "keyword_bonus": round(keyword_bonus, 4),
            "keyword_penalty": round(keyword_penalty, 4),
        },
    }