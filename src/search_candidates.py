from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select, or_

from src.db_setup import documents, document_fields
from src.search_ranking import tokenize_text, raw_ocr_person_match
from src.document_field_normalizer import normalize_document_type


async def collect_raw_ocr_matches(
    conn,
    persona_gpt: str | None,
    tipo_doc_gpt: str | None,
    build_tipo_documento_sql_conditions,
    is_generic_document_type,
) -> tuple[set[int], dict[int, int]]:
    raw_ocr_doc_ids: set[int] = set()
    raw_ocr_first_page_by_doc: dict[int, int] = {}

    if not persona_gpt:
        return raw_ocr_doc_ids, raw_ocr_first_page_by_doc

    raw_candidate_stmt = select(
        documents.c.id,
        documents.c.tipo_documento,
        documents.c.file_path,
        documents.c.ocr_text,
        documents.c.ocr_pages
    )

    if tipo_doc_gpt and not is_generic_document_type(tipo_doc_gpt):
        tipo_conditions = build_tipo_documento_sql_conditions(tipo_doc_gpt)
        if tipo_conditions:
            raw_candidate_stmt = raw_candidate_stmt.where(or_(*tipo_conditions))

    raw_candidate_rows = (await conn.execute(raw_candidate_stmt)).mappings().all()

    for raw_row in raw_candidate_rows:
        raw_text = str(raw_row.get("ocr_text") or "")
        raw_pages = raw_row.get("ocr_pages")
        doc_id = raw_row["id"]

        page_match_found = False
        parsed_pages = []

        if raw_pages:
            if isinstance(raw_pages, list):
                parsed_pages = raw_pages
            elif isinstance(raw_pages, str):
                try:
                    loaded_pages = json.loads(raw_pages)
                    if isinstance(loaded_pages, list):
                        parsed_pages = loaded_pages
                    else:
                        parsed_pages = [raw_pages]
                except Exception:
                    parsed_pages = [raw_pages]

        for page_idx, page_text in enumerate(parsed_pages, start=1):
            page_text_str = str(page_text or "")
            raw_text += " " + page_text_str

            if not page_match_found and raw_ocr_person_match(persona_gpt, page_text_str):
                raw_ocr_doc_ids.add(doc_id)
                raw_ocr_first_page_by_doc[doc_id] = page_idx
                page_match_found = True

        if not page_match_found and raw_ocr_person_match(persona_gpt, raw_text):
            raw_ocr_doc_ids.add(doc_id)

    return raw_ocr_doc_ids, raw_ocr_first_page_by_doc


async def collect_text_matches(
    conn,
    keywords: list[str],
    persona_gpt: str | None,
    tipo_doc_gpt: str | None,
    nome_file_gpt: str | None,
) -> set[int]:
    text_doc_ids: set[int] = set()

    if not keywords:
        return text_doc_ids

    text_stmt = select(documents.c.id).select_from(
        documents.join(
            document_fields,
            documents.c.id == document_fields.c.document_id
        )
    )

    general_conditions = []
    persona_conditions = []
    persona_tokens = []

    for kw in keywords:
        general_conditions.append(document_fields.c.valore.ilike(f"%{kw}%"))
        general_conditions.append(document_fields.c.campo.ilike(f"%{kw}%"))
        general_conditions.append(documents.c.tipo_documento.ilike(f"%{kw}%"))

    if tipo_doc_gpt:
        general_conditions.append(documents.c.tipo_documento.ilike(f"%{tipo_doc_gpt}%"))

    if nome_file_gpt:
        general_conditions.append(documents.c.file_path.ilike(f"%{nome_file_gpt}%"))

    if persona_gpt:
        persona_tokens = tokenize_text(persona_gpt)

    for token in persona_tokens:
        persona_conditions.append(document_fields.c.valore.ilike(f"%{token}%"))

    if general_conditions:
        text_stmt = text_stmt.where(or_(*general_conditions))

    if persona_conditions:
        text_stmt = text_stmt.where(or_(*persona_conditions))

    text_rows = (await conn.execute(text_stmt)).mappings().all()
    text_doc_ids = {row["id"] for row in text_rows}

    return text_doc_ids


async def collect_semantic_matches(
    query: str,
    persona_gpt: str | None,
    search,
    search_chunks,
) -> tuple[dict[int, dict[str, Any]], dict[int, dict[str, Any]], set[int]]:
    semantic_hits_by_id: dict[int, dict[str, Any]] = {}
    best_chunk_per_doc: dict[int, dict[str, Any]] = {}
    semantic_doc_ids: set[int] = set()

    semantic_top_k = 50
    chunk_top_k = 80

    if persona_gpt:
        semantic_top_k = 20
        chunk_top_k = 120

    semantic_hits = await __import__("asyncio").to_thread(search, query, top_k=semantic_top_k)
    chunk_hits = await __import__("asyncio").to_thread(search_chunks, query, top_k=chunk_top_k)

    for ch in chunk_hits:
        doc_id = ch["doc_id"]
        current = best_chunk_per_doc.get(doc_id)

        if current is None or (ch.get("score") or 0) > (current.get("score") or 0):
            best_chunk_per_doc[doc_id] = ch

    for hit in semantic_hits:
        doc_id = hit["doc_id"]
        current = semantic_hits_by_id.get(doc_id)

        if current is None or (hit.get("score") or 0) > (current.get("score") or 0):
            semantic_hits_by_id[doc_id] = {
                "doc_id": doc_id,
                "score": hit.get("score") or 0.0,
                "source": "doc",
                "chunk_text": None,
                "chunk_index": None,
                "page_number": None,
            }

    for hit in chunk_hits:
        doc_id = hit["doc_id"]
        chunk_score = hit.get("score") or 0.0
        current = semantic_hits_by_id.get(doc_id)

        if current is None or chunk_score > (current.get("score") or 0):
            semantic_hits_by_id[doc_id] = {
                "doc_id": doc_id,
                "score": chunk_score,
                "source": "chunk",
                "chunk_text": hit.get("text"),
                "chunk_index": hit.get("chunk_index"),
                "page_number": hit.get("page_number"),
            }

    semantic_doc_ids = set(semantic_hits_by_id.keys())
    return semantic_hits_by_id, best_chunk_per_doc, semantic_doc_ids


def build_search_keywords(
    query: str,
    keywords_gpt: list[str],
    persona_gpt: str | None,
    tipo_doc_gpt: str | None,
    nome_file_gpt: str | None,
) -> list[str]:
    keywords = [k.strip().lower() for k in keywords_gpt if len(str(k).strip()) > 1]

    if persona_gpt and persona_gpt.lower() not in keywords:
        keywords.append(persona_gpt.lower())

    if tipo_doc_gpt and tipo_doc_gpt.lower() not in keywords:
        keywords.append(tipo_doc_gpt.lower())

    if nome_file_gpt and nome_file_gpt.lower() not in keywords:
        keywords.append(nome_file_gpt.lower())

    if not keywords:
        keywords = [k.strip().lower() for k in query.split() if len(k.strip()) > 2]

    return keywords


async def collect_candidate_doc_ids(
    conn,
    query: str,
    keywords_gpt: list[str],
    persona_gpt: str | None,
    tipo_doc_gpt: str | None,
    nome_file_gpt: str | None,
    build_tipo_documento_sql_conditions,
    is_generic_document_type,
    search,
    search_chunks,
) -> dict[str, Any]:
    keywords = build_search_keywords(
        query=query,
        keywords_gpt=keywords_gpt,
        persona_gpt=persona_gpt,
        tipo_doc_gpt=tipo_doc_gpt,
        nome_file_gpt=nome_file_gpt,
    )

    raw_ocr_doc_ids, raw_ocr_first_page_by_doc = await collect_raw_ocr_matches(
        conn=conn,
        persona_gpt=persona_gpt,
        tipo_doc_gpt=tipo_doc_gpt,
        build_tipo_documento_sql_conditions=build_tipo_documento_sql_conditions,
        is_generic_document_type=is_generic_document_type,
    )

    text_doc_ids = await collect_text_matches(
        conn=conn,
        keywords=keywords,
        persona_gpt=persona_gpt,
        tipo_doc_gpt=tipo_doc_gpt,
        nome_file_gpt=nome_file_gpt,
    )

    semantic_hits_by_id, best_chunk_per_doc, semantic_doc_ids = await collect_semantic_matches(
        query=query,
        persona_gpt=persona_gpt,
        search=search,
        search_chunks=search_chunks,
    )

    all_doc_ids = list(text_doc_ids | semantic_doc_ids | raw_ocr_doc_ids)

    return {
        "keywords": keywords,
        "text_doc_ids": text_doc_ids,
        "semantic_hits_by_id": semantic_hits_by_id,
        "semantic_doc_ids": semantic_doc_ids,
        "raw_ocr_doc_ids": raw_ocr_doc_ids,
        "raw_ocr_first_page_by_doc": raw_ocr_first_page_by_doc,
        "best_chunk_per_doc": best_chunk_per_doc,
        "all_doc_ids": all_doc_ids,
    }