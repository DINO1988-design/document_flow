from src.search_result_builder import (
    is_positive_ticket_query,
    is_ticket_document,
    enrich_persona_match_from_raw_ocr,
    should_skip_filename_only_person_match,
    build_complete_result_row,
)


async def build_search_results(
    *,
    all_doc_ids,
    docs_map,
    docs_payload,
    parsed_query,
    query,
    persona_gpt,
    text_doc_ids,
    semantic_hits_by_id,
    best_chunk_per_doc,
    raw_ocr_doc_ids,
    raw_ocr_first_page_by_doc,
    document_frequency,
    compute_scored_document,
    build_preview_source_text,
):
    final_results = []
    skipped_missing_row = 0
    skipped_not_ticket = 0
    processed_docs = 0

    is_ticket_query = is_positive_ticket_query(parsed_query, query)

    for doc_id in all_doc_ids:
        row = docs_map.get(doc_id)
        if not row:
            skipped_missing_row += 1
            continue

        payload = docs_payload.get(doc_id)
        if not payload:
            skipped_missing_row += 1
            continue

        fields = payload["fields"]
        raw_ocr_text = payload["raw_ocr_text"]

        scored_doc = compute_scored_document(
            doc_id=doc_id,
            row=row,
            fields=fields,
            raw_ocr_text=raw_ocr_text,
            parsed_query=parsed_query,
            query=query,
            document_frequency=document_frequency,
            semantic_hits_by_id=semantic_hits_by_id,
            best_chunk_per_doc=best_chunk_per_doc,
            raw_ocr_doc_ids=raw_ocr_doc_ids,
            raw_ocr_first_page_by_doc=raw_ocr_first_page_by_doc,
        )

        score_info = scored_doc["score_info"]
        semantic_score = scored_doc["semantic_score"]
        best_chunk_text = scored_doc["best_chunk_text"]
        best_page_number = scored_doc["best_page_number"]
        best_chunk_index = scored_doc["best_chunk_index"]
        hit = scored_doc["semantic_hit"]

        score_info = enrich_persona_match_from_raw_ocr(
            score_info=score_info,
            doc_id=doc_id,
            raw_ocr_doc_ids=raw_ocr_doc_ids,
        )

        if should_skip_filename_only_person_match(
            persona_gpt=persona_gpt,
            score_info=score_info,
            semantic_score=semantic_score,
        ):
            continue

        if is_ticket_query and not is_ticket_document(row):
            skipped_not_ticket += 1
            continue

        processed_docs += 1

        final_results.append(
            build_complete_result_row(
                doc_id=doc_id,
                row=row,
                fields=fields,
                score_info=score_info,
                semantic_score=semantic_score,
                best_chunk_text=best_chunk_text,
                best_page_number=best_page_number,
                best_chunk_index=best_chunk_index,
                text_doc_ids=text_doc_ids,
                hit=hit,
                raw_ocr_first_page_by_doc=raw_ocr_first_page_by_doc,
                query=query,
                persona_gpt=persona_gpt,
                build_preview_source_text=build_preview_source_text,
            )
        )

    final_results.sort(
        key=lambda r: (
            1 if r.get("persona_match") else 0,
            r.get("persona_token_matches") or 0,
            1 if r.get("text_match") else 0,
            1 if r.get("tipo_match") else 0,
            r.get("boosted_score") or 0,
            r.get("score") or 0
        ),
        reverse=True
    )

    debug_info = {
        "skipped_missing_row": skipped_missing_row,
        "skipped_not_ticket": skipped_not_ticket,
        "processed_docs": processed_docs,
    }

    return final_results, debug_info