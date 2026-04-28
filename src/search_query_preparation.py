#src/search_query_preparation.py
from src.document_field_normalizer import normalize_document_type, normalize_text
from src.search_ranking import  has_meaningful_keywords, is_generic_document_type

def prepare_search_query_context(
    query: str,
    parsed_query: dict,
):
    parsed_query = dict(parsed_query or {})
    query_norm = normalize_text(query)

    if is_generic_document_type(parsed_query.get("tipo_documento")):
        parsed_query["tipo_documento"] = None

    exclude_tipi_norm = {
        normalize_document_type(x) or normalize_text(x)
        for x in (parsed_query.get("exclude_tipo_documento") or [])
        if x
    }

    tipo_doc_norm = (
        normalize_document_type(parsed_query.get("tipo_documento"))
        or normalize_text(parsed_query.get("tipo_documento"))
    )

    if tipo_doc_norm and tipo_doc_norm in exclude_tipi_norm:
        parsed_query["tipo_documento"] = None
        tipo_doc_norm = None

    theme_phrases = [
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
        "su",
    ]

    tail_text = None
    tail_theme_norm = None

    for phrase in theme_phrases:
        if phrase in query_norm:
            tail_text = query_norm.split(phrase, 1)[1].strip()
            if tail_text:
                tail_theme_norm = normalize_document_type(tail_text) or normalize_text(tail_text)
            break

    tipo_doc_norm = (
        normalize_document_type(parsed_query.get("tipo_documento"))
        or normalize_text(parsed_query.get("tipo_documento"))
    )

    if tipo_doc_norm and tail_theme_norm and tipo_doc_norm == tail_theme_norm:
        parsed_query["tipo_documento"] = None
        tipo_doc_norm = None

    existing_keywords_norm = {
        normalize_text(k)
        for k in (parsed_query.get("keywords") or [])
        if normalize_text(k)
    }

    if tail_text:
        tail_norm = normalize_text(tail_text)
        if tail_norm and tail_norm not in existing_keywords_norm:
            parsed_query.setdefault("keywords", []).append(tail_text)

    tipo_doc_gpt = parsed_query.get("tipo_documento")
    persona_gpt = parsed_query.get("persona")
    nome_file_gpt = parsed_query.get("nome_file")
    keywords_gpt = parsed_query.get("keywords", [])
    conditions_gpt = parsed_query.get("conditions") or []

    has_textual_theme = has_meaningful_keywords(
        keywords_gpt,
        tipo_documento=tipo_doc_gpt,
        persona=persona_gpt,
        nome_file=nome_file_gpt
    )

    return {
        "parsed_query": parsed_query,
        "query_norm": query_norm,
        "tipo_doc_gpt": tipo_doc_gpt,
        "persona_gpt": persona_gpt,
        "nome_file_gpt": nome_file_gpt,
        "keywords_gpt": keywords_gpt,
        "conditions_gpt": conditions_gpt,
        "has_textual_theme": has_textual_theme,
    }