from sqlalchemy import select, or_
from src.search_ranking import tokenize_text

async def apply_fuzzy_person_recall(
    conn,
    *,
    parsed_query: dict,
    persona_gpt: str | None,
    tipo_doc_gpt: str | None,
    docs_map: dict,
    all_doc_ids: list,
    documents,
    document_fields,
    tokenize_text,
    has_fuzzy_person_match,
    apply_conditions_to_documents_stmt,
    build_tipo_documento_sql_conditions,
    is_generic_document_type,
):
    if not persona_gpt:
        return docs_map, all_doc_ids, 0

    person_tokens = tokenize_text(persona_gpt)

    fuzzy_stmt = select(
        documents.c.id,
        documents.c.tipo_documento,
        documents.c.file_path,
        documents.c.data_creazione
    )

    conditions = parsed_query.get("conditions") or []
    fuzzy_stmt = apply_conditions_to_documents_stmt(
        fuzzy_stmt,
        conditions,
        build_tipo_documento_sql_conditions,
    )

    if not conditions and tipo_doc_gpt and not is_generic_document_type(tipo_doc_gpt):
        tipo_conditions = build_tipo_documento_sql_conditions(tipo_doc_gpt)
        if tipo_conditions:
            fuzzy_stmt = fuzzy_stmt.where(or_(*tipo_conditions))

    fuzzy_doc_rows = (await conn.execute(fuzzy_stmt)).mappings().all()

    fuzzy_added = 0

    for fuzzy_row in fuzzy_doc_rows:
        fuzzy_doc_id = fuzzy_row["id"]

        if fuzzy_doc_id in docs_map:
            continue

        fields_stmt = select(
            document_fields.c.campo,
            document_fields.c.valore
        ).where(document_fields.c.document_id == fuzzy_doc_id)

        fields_rows = (await conn.execute(fields_stmt)).mappings().all()
        fields = {f["campo"]: f["valore"] for f in fields_rows}

        if has_fuzzy_person_match(fields, person_tokens):
            docs_map[fuzzy_doc_id] = fuzzy_row
            if fuzzy_doc_id not in all_doc_ids:
                all_doc_ids.append(fuzzy_doc_id)
            fuzzy_added += 1

    return docs_map, all_doc_ids, fuzzy_added