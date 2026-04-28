from datetime import date
from typing import Callable


async def filter_documents_by_query_date(
    *,
    conn,
    all_doc_ids: list[int],
    docs_map: dict,
    raw_ocr_doc_ids: set[int],
    has_positive_tipo: bool,
    data_da: date | None,
    data_a: date | None,
    document_matches_query_date: Callable,
    load_fields_for_doc: Callable,
) -> tuple[dict, list[int], list[dict]]:
    """
    Restituisce:
    - filtered_docs_map
    - filtered_all_doc_ids
    - date_filter_debug
    """

    if not data_da and not data_a:
        return docs_map, all_doc_ids, []

    filtered_docs_map = {}
    filtered_all_doc_ids = []
    date_filter_debug = []

    for current_doc_id in all_doc_ids:
        row = docs_map.get(current_doc_id)
        if not row:
            continue

        fields = await load_fields_for_doc(conn, current_doc_id)

        matches_date, matched_date, matched_date_source = document_matches_query_date(
            fields=fields,
            row=row,
            data_da=data_da,
            data_a=data_a,
            use_creation_fallback=True,
        )

        date_filter_debug.append({
            "doc_id": current_doc_id,
            "tipo_documento": row.get("tipo_documento"),
            "matched": matches_date,
            "matched_date": str(matched_date) if matched_date else None,
            "matched_date_source": matched_date_source,
        })

        if matches_date:
            filtered_docs_map[current_doc_id] = row
            filtered_all_doc_ids.append(current_doc_id)

    return filtered_docs_map, filtered_all_doc_ids, date_filter_debug