# src/search_result_builder.py

from src.document_field_normalizer import normalize_document_type, normalize_text

from src.search_explain import (
    build_match_excerpt,
    highlight_query_terms,
    build_match_explanation,
    get_result_label,
)


def is_positive_ticket_query(parsed_query: dict, query: str) -> bool:
    query_norm = normalize_text(query)

    ticket_query_terms = ["bigliett", "ticket", "volo", "voli"]
    has_ticket_term_in_query = any(x in query_norm for x in ticket_query_terms)

    exclude_tipi_norm = {
        normalize_document_type(x) or normalize_text(x)
        for x in (parsed_query.get("exclude_tipo_documento") or [])
        if x
    }

    ticket_excluded = "biglietto" in exclude_tipi_norm

    conditions = parsed_query.get("conditions") or []
    included_tipi = set()

    for cond in conditions:
        if not isinstance(cond, dict):
            continue

        target_norm = normalize_text(str(cond.get("target") or ""))
        field_norm = normalize_text(str(cond.get("field") or ""))
        operator = str(cond.get("operator") or "").strip().lower()
        value = cond.get("value")

        is_tipo_condition = (
            target_norm in {"tipo documento", "tipo_documento"}
            and field_norm in {"tipo documento", "tipo_documento"}
        )

        if not is_tipo_condition:
            continue

        if operator in {"=", "==", "in"}:
            if isinstance(value, list):
                for v in value:
                    v_norm = normalize_document_type(v) or normalize_text(v)
                    if v_norm:
                        included_tipi.add(v_norm)
            else:
                v_norm = normalize_document_type(value) or normalize_text(value)
                if v_norm:
                    included_tipi.add(v_norm)

    return (
        has_ticket_term_in_query
        and not ticket_excluded
        and included_tipi == {"biglietto"}
    )


def build_result_row(
    *,
    doc_id: int,
    row: dict,
    fields: dict,
    score_info: dict,
    semantic_score: float,
    boosted_score: float,
    best_chunk_text: str | None,
    best_page_number: int | None,
    best_chunk_index: int | None,
    best_chunk_preview: str,
    best_chunk_preview_html: str,
    text_doc_ids: set,
    hit: dict | None,
    raw_ocr_first_page_by_doc: dict,
    match_explanation: dict,
):
    return {
        "id": doc_id,
        "tipo_documento": row["tipo_documento"],
        "file_path": row["file_path"],
        "data_creazione": row["data_creazione"].strftime("%Y-%m-%d %H:%M:%S"),
        "campi": fields,
        "score": semantic_score,
        "boosted_score": boosted_score,
        "score_label": get_result_label(min(boosted_score, 1.0), score_info["text_match"]),
        "score_percent": min(round((boosted_score or 0) * 100, 1), 100),
        "text_match": score_info["text_match"],
        "keyword_match": score_info.get("keyword_match", False),
        "keyword_match_count": score_info.get("keyword_match_count", 0),
        "persona_match": score_info["persona_match"],
        "raw_ocr_person_match": score_info.get("raw_ocr_person_match", False),
        "tipo_match": score_info["tipo_match"],
        "persona_token_matches": score_info["persona_token_matches"],
        "persona_token_matches_strong": score_info["persona_token_matches_strong"],
        "persona_token_matches_medium": score_info["persona_token_matches_medium"],
        "persona_token_matches_generic": score_info["persona_token_matches_generic"],
        "persona_token_matches_chunk": score_info["persona_token_matches_chunk"],
        "persona_token_matches_filename": score_info["persona_token_matches_filename"],
        "structured_field_match": score_info.get("structured_field_match", False),
        "structured_field_info": score_info.get("structured_field_info"),
        "match_type": (
            "hybrid_chunk" if (doc_id in text_doc_ids and best_chunk_text)
            else "hybrid_doc" if (doc_id in text_doc_ids and hit and hit.get("source") == "doc")
            else "text" if doc_id in text_doc_ids
            else "semantic_chunk" if best_chunk_text
            else "semantic_doc"
        ),
        "semantic_source": hit.get("source") if hit else None,
        "best_chunk_text": best_chunk_text,
        "best_chunk_preview": best_chunk_preview,
        "best_chunk_preview_html": best_chunk_preview_html,
        "best_chunk_index": best_chunk_index,
        "best_chunk_page": best_page_number,
        "persona_page": raw_ocr_first_page_by_doc.get(doc_id),
        "match_explanation": match_explanation,
        "match_primary_reason": match_explanation.get("primary_reason"),
        "match_confidence_label": match_explanation.get("confidence_label"),
        "match_explanations": match_explanation.get("explanations", []),
    }


def build_preview_data(
    *,
    build_preview_source_text,
    score_info,
    best_chunk_text,
    query,
    persona_gpt,
):
    preview_source_text = build_preview_source_text(
        score_info=score_info,
        best_chunk_text=best_chunk_text,
    )

    best_chunk_preview = build_match_excerpt(
        preview_source_text,
        query,
        focus_text=persona_gpt,
        window=120
    )
    best_chunk_preview_html = highlight_query_terms(best_chunk_preview, query)

    return {
        "best_chunk_preview": best_chunk_preview,
        "best_chunk_preview_html": best_chunk_preview_html,
    }

def build_complete_result_row(
    *,
    doc_id: int,
    row: dict,
    fields: dict,
    score_info: dict,
    semantic_score: float,
    best_chunk_text: str | None,
    best_page_number: int | None,
    best_chunk_index: int | None,
    text_doc_ids: set,
    hit: dict | None,
    raw_ocr_first_page_by_doc: dict,
    query: str,
    persona_gpt: str | None,
    build_preview_source_text,
):
    preview_data = build_preview_data(
        build_preview_source_text=build_preview_source_text,
        score_info=score_info,
        best_chunk_text=best_chunk_text,
        query=query,
        persona_gpt=persona_gpt,
    )

    boosted_score = score_info["score"]
    if hit and hit.get("source") == "chunk":
        boosted_score += 0.05
    boosted_score = min(boosted_score, 1.0)

    match_explanation = build_match_explanation({
        **score_info,
        "boosted_score": boosted_score
    })

    return build_result_row(
        doc_id=doc_id,
        row=row,
        fields=fields,
        score_info=score_info,
        semantic_score=semantic_score,
        boosted_score=boosted_score,
        best_chunk_text=best_chunk_text,
        best_page_number=best_page_number,
        best_chunk_index=best_chunk_index,
        best_chunk_preview=preview_data["best_chunk_preview"],
        best_chunk_preview_html=preview_data["best_chunk_preview_html"],
        text_doc_ids=text_doc_ids,
        hit=hit,
        raw_ocr_first_page_by_doc=raw_ocr_first_page_by_doc,
        match_explanation=match_explanation,
    )
    
def is_ticket_document(tipo_documento: str | None) -> bool:
    tipo_norm = normalize_text(tipo_documento or "")

    return any(
        x in tipo_norm
        for x in ["ticket", "biglietto", "biglietto aereo", "biglietto traghetto"]
    )
    
def enrich_persona_match_from_raw_ocr(
    *,
    score_info: dict,
    doc_id: int,
    raw_ocr_doc_ids: set | list,
) -> dict:
    raw_ocr_hit = doc_id in raw_ocr_doc_ids

    if raw_ocr_hit and not score_info.get("persona_match", False):
        score_info["persona_match"] = True
        score_info["persona_token_matches_generic"] = max(
            score_info.get("persona_token_matches_generic", 0), 1
        )
        score_info["persona_token_matches"] = (
            (score_info.get("persona_token_matches_strong", 0) or 0)
            + (score_info.get("persona_token_matches_medium", 0) or 0)
            + (score_info.get("persona_token_matches_generic", 0) or 0)
        )

    return score_info
    
def should_skip_filename_only_person_match(
    *,
    persona_gpt: str | None,
    score_info: dict,
    semantic_score: float | None,
) -> bool:
    if not persona_gpt:
        return False

    return (
        not score_info.get("persona_match", False)
        and score_info.get("persona_token_matches_filename", 0) > 0
        and (semantic_score or 0) < 0.40
    )