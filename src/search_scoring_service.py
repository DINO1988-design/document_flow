from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select

from src.db_setup import documents, document_fields
from src.search_business_rules import build_document_frequency
from src.search_ranking import build_searchable_text, compute_document_score


async def load_documents_full_payload(conn, doc_ids: list[int]) -> dict[int, dict[str, Any]]:
    """
    Carica in un colpo solo:
    - riga documento
    - fields
    - OCR text/pages

    Output:
    {
        doc_id: {
            "row": {...},
            "fields": {...},
            "raw_ocr_text": "...",
            "ocr_pages_raw": ...,
        }
    }
    """
    if not doc_ids:
        return {}

    docs_stmt = select(
        documents.c.id,
        documents.c.tipo_documento,
        documents.c.file_path,
        documents.c.data_creazione,
        documents.c.ocr_text,
        documents.c.ocr_pages,
    ).where(documents.c.id.in_(doc_ids))

    doc_rows = (await conn.execute(docs_stmt)).mappings().all()

    fields_stmt = select(
        document_fields.c.document_id,
        document_fields.c.campo,
        document_fields.c.valore,
    ).where(document_fields.c.document_id.in_(doc_ids))

    fields_rows = (await conn.execute(fields_stmt)).mappings().all()

    fields_map: dict[int, dict[str, Any]] = {}
    for f in fields_rows:
        fields_map.setdefault(f["document_id"], {})[f["campo"]] = f["valore"]

    result: dict[int, dict[str, Any]] = {}

    for row in doc_rows:
        raw_ocr_text = str(row.get("ocr_text") or "")
        ocr_pages_val = row.get("ocr_pages") or ""

        if ocr_pages_val:
            if isinstance(ocr_pages_val, str):
                raw_ocr_text += " " + ocr_pages_val
            else:
                raw_ocr_text += " " + str(ocr_pages_val)

        result[row["id"]] = {
            "row": dict(row),
            "fields": fields_map.get(row["id"], {}),
            "raw_ocr_text": raw_ocr_text,
            "ocr_pages_raw": row.get("ocr_pages"),
        }

    return result


def build_candidate_texts_for_df(
    docs_payload: dict[int, dict[str, Any]],
    best_chunk_per_doc: dict[int, dict[str, Any]],
) -> list[str]:
    candidate_texts: list[str] = []

    for doc_id, payload in docs_payload.items():
        row = payload["row"]
        fields = payload["fields"]

        searchable_text = build_searchable_text(
            row.get("tipo_documento") or "",
            fields
        )

        chunk_text = ""
        best_chunk_hit = best_chunk_per_doc.get(doc_id)
        if best_chunk_hit:
            chunk_text = best_chunk_hit.get("text") or ""

        candidate_texts.append(f"{searchable_text} {chunk_text}".strip())

    return candidate_texts


def build_document_frequency_for_candidates(
    docs_payload: dict[int, dict[str, Any]],
    best_chunk_per_doc: dict[int, dict[str, Any]],
):
    candidate_texts = build_candidate_texts_for_df(
        docs_payload=docs_payload,
        best_chunk_per_doc=best_chunk_per_doc,
    )
    return build_document_frequency(candidate_texts)


def get_best_chunk_data(
    doc_id: int,
    semantic_hits_by_id: dict[int, dict[str, Any]],
    best_chunk_per_doc: dict[int, dict[str, Any]],
    raw_ocr_first_page_by_doc: dict[int, int],
) -> dict[str, Any]:
    hit = semantic_hits_by_id.get(doc_id)
    best_chunk_hit = best_chunk_per_doc.get(doc_id)

    semantic_score = hit.get("score") if hit else 0.0

    best_chunk_text = None
    best_page_number = None
    best_chunk_index = None

    if hit and hit.get("source") == "chunk":
        best_chunk_text = hit.get("chunk_text")
        best_page_number = hit.get("page_number")
        best_chunk_index = hit.get("chunk_index")
    elif best_chunk_hit:
        best_chunk_text = best_chunk_hit.get("text")
        best_page_number = best_chunk_hit.get("page_number")
        best_chunk_index = best_chunk_hit.get("chunk_index")

    persona_page_number = raw_ocr_first_page_by_doc.get(doc_id)
    if persona_page_number is not None:
        best_page_number = persona_page_number

    return {
        "semantic_score": semantic_score,
        "best_chunk_text": best_chunk_text,
        "best_page_number": best_page_number,
        "best_chunk_index": best_chunk_index,
        "semantic_hit": hit,
        "best_chunk_hit": best_chunk_hit,
    }


def build_preview_source_text(
    score_info: dict[str, Any],
    best_chunk_text: str | None,
) -> str:
    structured_info = score_info.get("structured_field_info") or {}
    structured_match = score_info.get("structured_field_match") is True

    if structured_match and structured_info.get("matched_field"):
        matched_field_name = str(structured_info.get("matched_field") or "").strip()
        matched_raw_value = str(structured_info.get("raw_value") or "").strip()
        return f"{matched_field_name}: {matched_raw_value}".strip()

    return best_chunk_text or score_info.get("searchable_text") or ""


def compute_scored_document(
    *,
    doc_id: int,
    row: dict[str, Any],
    fields: dict[str, Any],
    raw_ocr_text: str,
    parsed_query: dict[str, Any],
    query: str,
    document_frequency,
    semantic_hits_by_id: dict[int, dict[str, Any]],
    best_chunk_per_doc: dict[int, dict[str, Any]],
    raw_ocr_doc_ids: set[int],
    raw_ocr_first_page_by_doc: dict[int, int],
):
    chunk_data = get_best_chunk_data(
        doc_id=doc_id,
        semantic_hits_by_id=semantic_hits_by_id,
        best_chunk_per_doc=best_chunk_per_doc,
        raw_ocr_first_page_by_doc=raw_ocr_first_page_by_doc,
    )

    semantic_score = chunk_data["semantic_score"]
    best_chunk_text = chunk_data["best_chunk_text"]

    score_info = compute_document_score(
        query=query,
        parsed_query=parsed_query,
        row=row,
        fields=fields,
        semantic_score=semantic_score,
        best_chunk_text=best_chunk_text,
        raw_ocr_text=raw_ocr_text,
        document_frequency=document_frequency,
    )

    raw_ocr_hit = doc_id in raw_ocr_doc_ids

    if raw_ocr_hit and not score_info["persona_match"]:
        score_info["persona_match"] = True
        score_info["persona_token_matches_generic"] = max(
            score_info.get("persona_token_matches_generic", 0), 1
        )
        score_info["persona_token_matches"] = (
            (score_info.get("persona_token_matches_strong", 0) or 0)
            + (score_info.get("persona_token_matches_medium", 0) or 0)
            + (score_info.get("persona_token_matches_generic", 0) or 0)
        )

    return {
        "score_info": score_info,
        "semantic_score": semantic_score,
        "best_chunk_text": chunk_data["best_chunk_text"],
        "best_page_number": chunk_data["best_page_number"],
        "best_chunk_index": chunk_data["best_chunk_index"],
        "semantic_hit": chunk_data["semantic_hit"],
        "best_chunk_hit": chunk_data["best_chunk_hit"],
        "raw_ocr_hit": raw_ocr_hit,
    }