from __future__ import annotations

import re

from src.search_ranking import  tokenize_text
from src.document_field_normalizer import normalize_text

def build_chunk_preview(text: str, max_len: int = 280) -> str:
    if not text:
        return ""

    text = str(text).strip()
    text = re.sub(r"\s+", " ", text)

    if len(text) <= max_len:
        return text

    return text[:max_len].rstrip() + "..."


def build_match_excerpt(
    text: str,
    query: str,
    focus_text: str | None = None,
    window: int = 120
) -> str:
    if not text:
        return ""

    clean_text = re.sub(r"\s+", " ", str(text)).strip()
    if not clean_text:
        return ""

    if focus_text:
        focus_norm = normalize_text(focus_text)

        if focus_norm:
            m = re.search(re.escape(focus_norm), normalize_text(clean_text), re.IGNORECASE)
            if m:
                best_pos = m.start()
                start = max(0, best_pos - window)
                end = min(len(clean_text), best_pos + window)
                excerpt = clean_text[start:end].strip()

                if start > 0:
                    excerpt = "... " + excerpt
                if end < len(clean_text):
                    excerpt = excerpt + " ..."

                return excerpt

            focus_tokens = [t for t in tokenize_text(focus_text) if len(t) >= 3]
            earliest_pos = None

            for token in focus_tokens:
                m = re.search(re.escape(token), clean_text, re.IGNORECASE)
                if m:
                    if earliest_pos is None or m.start() < earliest_pos:
                        earliest_pos = m.start()

            if earliest_pos is not None:
                start = max(0, earliest_pos - window)
                end = min(len(clean_text), earliest_pos + window)
                excerpt = clean_text[start:end].strip()

                if start > 0:
                    excerpt = "... " + excerpt
                if end < len(clean_text):
                    excerpt = excerpt + " ..."

                return excerpt

    tokens = [t for t in tokenize_text(query) if len(t) >= 3]

    if not tokens:
        return clean_text[:240] + ("..." if len(clean_text) > 240 else "")

    earliest_pos = None
    for token in tokens:
        m = re.search(re.escape(token), clean_text, re.IGNORECASE)
        if m:
            if earliest_pos is None or m.start() < earliest_pos:
                earliest_pos = m.start()

    if earliest_pos is None:
        return clean_text[:240] + ("..." if len(clean_text) > 240 else "")

    start = max(0, earliest_pos - window)
    end = min(len(clean_text), earliest_pos + window)
    excerpt = clean_text[start:end].strip()

    if start > 0:
        excerpt = "... " + excerpt
    if end < len(clean_text):
        excerpt = excerpt + " ..."

    return excerpt


def highlight_query_terms(text: str, query: str) -> str:
    if not text or not query:
        return text or ""

    stopwords = {
        "di", "del", "della", "dello", "dei", "degli", "delle",
        "il", "lo", "la", "i", "gli", "le",
        "un", "uno", "una",
        "e", "ed", "o",
        "a", "da", "in", "con", "su", "per", "tra", "fra"
    }

    result = text
    query_tokens = []
    seen = set()

    for token in tokenize_text(query):
        if token in stopwords:
            continue
        if len(token) < 3:
            continue
        if token not in seen:
            seen.add(token)
            query_tokens.append(token)

    query_tokens.sort(key=len, reverse=True)

    for token in query_tokens:
        pattern = re.compile(rf"({re.escape(token)})", re.IGNORECASE)
        result = pattern.sub(r"<mark>\1</mark>", result)

    return result


def build_match_explanation(score_info: dict) -> dict:
    breakdown = score_info.get("score_breakdown", {}) or {}

    person_strong = breakdown.get("person_strong_bonus", 0.0)
    person_medium = breakdown.get("person_medium_bonus", 0.0)
    person_generic = breakdown.get("person_generic_bonus", 0.0)
    person_chunk = breakdown.get("person_chunk_bonus", 0.0)
    raw_ocr = breakdown.get("raw_ocr_bonus", 0.0)
    tipo_adj = breakdown.get("tipo_score_adjustment", 0.0)
    text_bonus = breakdown.get("text_match_bonus", 0.0)
    query_token_bonus = breakdown.get("query_token_coverage_bonus", 0.0)
    filename_bonus = breakdown.get("filename_bonus", 0.0)
    full_person_phrase_bonus = breakdown.get("full_person_phrase_bonus", 0.0)

    structured_field_bonus = breakdown.get("structured_field_bonus", 0.0)
    structured_field_exact_bonus = breakdown.get("structured_field_exact_bonus", 0.0)
    structured_field_partial_bonus = breakdown.get("structured_field_partial_bonus", 0.0)

    explanations = []

    if person_strong > 0:
        explanations.append("Match persona forte")
    elif person_medium > 0:
        explanations.append("Match persona medio")
    elif person_generic > 0:
        explanations.append("Match persona debole")

    if structured_field_bonus > 0:
        explanations.append("Filtro business coerente")

    if structured_field_exact_bonus > 0:
        explanations.append("Valore campo centrato")
    elif structured_field_partial_bonus > 0:
        explanations.append("Valore campo parzialmente coerente")

    if full_person_phrase_bonus > 0:
        explanations.append("Nome completo riconosciuto")

    if raw_ocr > 0:
        explanations.append("Persona confermata da OCR")

    if person_chunk > 0:
        explanations.append("Contesto rilevante nel contenuto")

    if tipo_adj > 0:
        explanations.append("Tipo documento coerente")
    elif tipo_adj < 0:
        explanations.append("Tipo documento non coerente")

    if text_bonus > 0 or query_token_bonus > 0:
        explanations.append("Match testuale presente")

    if filename_bonus > 0 and not score_info.get("persona_match"):
        explanations.append("Corrispondenza solo nel nome file")
    elif filename_bonus > 0:
        explanations.append("Nome file parzialmente coerente")

    primary_reason = "Corrispondenza generica"

    if structured_field_bonus > 0 and tipo_adj > 0:
        primary_reason = "Tipo documento e filtro business coerenti"
    elif structured_field_bonus > 0:
        primary_reason = "Filtro business coerente"
    elif person_strong > 0 and tipo_adj > 0:
        primary_reason = "Persona e tipo documento coerenti"
    elif person_strong > 0:
        primary_reason = "Persona trovata in campo forte"
    elif person_medium > 0:
        primary_reason = "Persona trovata in campo medio"
    elif raw_ocr > 0:
        primary_reason = "Persona trovata tramite OCR"
    elif tipo_adj > 0:
        primary_reason = "Tipo documento coerente"
    elif text_bonus > 0 or query_token_bonus > 0:
        primary_reason = "Match testuale"

    confidence_label = "Bassa"
    boosted_score = score_info.get("boosted_score", 0) or 0

    if boosted_score >= 0.95:
        confidence_label = "Alta"
    elif boosted_score >= 0.7:
        confidence_label = "Media"
    else:
        confidence_label = "Bassa"

    return {
        "primary_reason": primary_reason,
        "confidence_label": confidence_label,
        "explanations": explanations
    }


def get_semantic_label(score: float | None) -> str:
    if score is None:
        return "N/D"
    if score >= 0.80:
        return "Molto pertinente"
    if score >= 0.60:
        return "Buona corrispondenza"
    if score >= 0.45:
        return "Corrispondenza debole"
    return "Bassa pertinenza"


def get_result_label(score: float | None, text_match: bool) -> str:
    if text_match and score is not None and score >= 0.45:
        return "Match testuale + semantico"
    if text_match:
        return "Match testuale"
    return get_semantic_label(score)