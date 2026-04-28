from sqlalchemy import select, text, or_

from src.db_setup import documents, document_fields
from src.document_field_normalizer import (
    normalize_query_field_name,
    DOCUMENT_FIELD_ALIASES,
)
from src.search_ranking import normalize_text, is_generic_document_type


async def build_structured_results(conn, structured_rows, tipo_doc: str | None):
    final_results = []

    for row in structured_rows:
        doc_id = row["id"]

        fields_stmt = select(
            document_fields.c.campo,
            document_fields.c.valore
        ).where(document_fields.c.document_id == doc_id)

        fields_rows = (await conn.execute(fields_stmt)).mappings().all()
        fields = {f["campo"]: f["valore"] for f in fields_rows}

        final_results.append({
            "id": doc_id,
            "tipo_documento": row["tipo_documento"],
            "file_path": row["file_path"],
            "data_creazione": row["data_creazione"].strftime("%Y-%m-%d %H:%M:%S"),
            "campi": fields,
            "score": None,
            "boosted_score": None,
            "score_label": "Match strutturato",
            "score_percent": None,
            "text_match": True,
            "persona_match": False,
            "tipo_match": True if tipo_doc else False,
            "persona_token_matches": 0,
            "persona_token_matches_strong": 0,
            "persona_token_matches_medium": 0,
            "persona_token_matches_generic": 0,
            "persona_token_matches_chunk": 0,
            "persona_token_matches_filename": 0,
            "match_type": "structured",
            "semantic_source": None,
            "best_chunk_text": None,
            "best_chunk_preview": "",
            "best_chunk_preview_html": "",
            "best_chunk_index": None,
            "best_chunk_page": None,
            "structured_field_match": False,
            "structured_field_info": None,
        })

    deduped_results = []
    seen_ids = set()

    for r in final_results:
        current_doc_id = r.get("id")
        if current_doc_id in seen_ids:
            continue
        seen_ids.add(current_doc_id)
        deduped_results.append(r)

    return deduped_results


def apply_conditions_to_documents_stmt(docs_stmt, conditions, build_tipo_documento_sql_conditions):
    """
    Applica le condizioni strutturate al select sui documenti.
    Per ora gestisce solo target='tipo_documento'.
    """
    if not conditions:
        return docs_stmt

    for cond in conditions:
        if not isinstance(cond, dict):
            continue

        target_raw = str(cond.get("target") or "").strip().lower()
        field_raw = str(cond.get("field") or "").strip().lower()

        target_norm = normalize_text(target_raw)
        field_norm = normalize_text(field_raw)

        operator = (cond.get("operator") or "").strip().lower()
        value = cond.get("value")

        is_tipo_target = (
            target_raw == "tipo_documento" or target_norm == "tipo documento"
        )
        is_tipo_field = (
            field_raw == "tipo_documento" or field_norm == "tipo documento"
        )

        if is_tipo_target and is_tipo_field:
            if isinstance(value, list):
                norm_values = [normalize_text(v) for v in value if normalize_text(v)]
            else:
                single_value = normalize_text(value)
                norm_values = [single_value] if single_value else []

            if not norm_values:
                continue

            if operator in {"=", "==", "in"}:
                include_conditions = []

                for v in norm_values:
                    include_conditions.extend(build_tipo_documento_sql_conditions(v))

                if include_conditions:
                    docs_stmt = docs_stmt.where(or_(*include_conditions))

            elif operator in {"!=", "<>", "not_in"}:
                for v in norm_values:
                    exclude_conditions = build_tipo_documento_sql_conditions(v)

                    if exclude_conditions:
                        docs_stmt = docs_stmt.where(~or_(*exclude_conditions))

    return docs_stmt

def get_structured_field_aliases(tipo_doc: str | None, campo: str | None) -> list[str]:
    """
    Restituisce il campo canonico + tutti gli alias noti per quel tipo documento.
    Esempio:
    tipo_doc=contratto, campo=ral
    -> ["ral", "retribuzione_lorda_annuale", "contract.salary.annual_gross", ...]
    """
    if not campo:
        return []

    canonical_field = normalize_query_field_name(tipo_doc, campo) or campo
    aliases = [canonical_field]

    schema = DOCUMENT_FIELD_ALIASES.get(tipo_doc or "", {})
    canonical_map = schema.get("canonical_fields", {}) or {}

    if canonical_field in canonical_map:
        aliases.extend(canonical_map.get(canonical_field, []))

    # dedup mantenendo ordine
    seen = set()
    result = []

    for a in aliases:
        a_str = str(a).strip()
        if not a_str:
            continue
        a_norm = normalize_text(a_str)
        if a_norm in seen:
            continue
        seen.add(a_norm)
        result.append(a_str)

    return result
    
async def search_structured_documents(
    conn,
    parsed_query: dict,
    build_tipo_documento_sql_conditions,
):
    tipo_doc = parsed_query.get("tipo_documento")
    campo = parsed_query.get("campo_target")
    valore = parsed_query.get("valore_target")
    operatore = parsed_query.get("operatore")
    tipo_valore = parsed_query.get("tipo_valore")
    data_da = parsed_query.get("data_da")
    data_a = parsed_query.get("data_a")

    stmt = select(
        documents.c.id,
        documents.c.tipo_documento,
        documents.c.file_path,
        documents.c.data_creazione
    )

    joined_document_fields = False

    conditions = parsed_query.get("conditions") or []
    stmt = apply_conditions_to_documents_stmt(
        stmt,
        conditions,
        build_tipo_documento_sql_conditions,
    )

    primary_structured_field = normalize_query_field_name(tipo_doc, campo) if campo else None

    primary_date_filter_active = (
        primary_structured_field is not None
        and tipo_valore == "date"
        and (
            operatore == "between"
            or data_da is not None
            or data_a is not None
        )
    )

    if campo and (
        valore is not None
        or operatore == "between"
        or tipo_valore == "date"
        or data_da
        or data_a
    ):
        if not joined_document_fields:
            stmt = stmt.join(
                document_fields,
                documents.c.id == document_fields.c.document_id
            )
            joined_document_fields = True

        field_aliases = get_structured_field_aliases(tipo_doc, campo)
        print("[STRUCTURED FIELD ALIASES - MAIN]", field_aliases)

        if field_aliases:
            stmt = stmt.where(
                or_(*[
                    document_fields.c.campo.ilike(f"%{alias}%")
                    for alias in field_aliases
                ])
            )

        if tipo_valore == "number" and valore is not None:
            numero = None
            try:
                numero = float(str(valore).replace(",", ".").replace("h", "").strip())
            except ValueError:
                numero = None

            numeric_sql = """
            TRY_CAST(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(document_fields.valore, '€', ''),
                            ' ', ''),
                        '.', ''),
                    ',', '.'),
                'EUR', '')
            AS FLOAT)
            """

            if operatore == "between" and isinstance(valore, list) and len(valore) >= 2:
                try:
                    min_num = float(str(valore[0]).replace(",", ".").strip())
                    max_num = float(str(valore[1]).replace(",", ".").strip())

                    stmt = stmt.where(text(f"{numeric_sql} IS NOT NULL"))
                    stmt = stmt.where(text(f"{numeric_sql} >= :min_num")).params(min_num=min_num)
                    stmt = stmt.where(text(f"{numeric_sql} <= :max_num")).params(max_num=max_num)
                except Exception:
                    pass

            elif operatore in [">", "<", ">=", "<="] and numero is not None:
                stmt = stmt.where(text(f"{numeric_sql} IS NOT NULL"))

                if operatore == ">":
                    stmt = stmt.where(text(f"{numeric_sql} > :num")).params(num=numero)
                elif operatore == "<":
                    stmt = stmt.where(text(f"{numeric_sql} < :num")).params(num=numero)
                elif operatore == ">=":
                    stmt = stmt.where(text(f"{numeric_sql} >= :num")).params(num=numero)
                elif operatore == "<=":
                    stmt = stmt.where(text(f"{numeric_sql} <= :num")).params(num=numero)

            elif numero is not None:
                stmt = stmt.where(text(f"{numeric_sql} = :num")).params(num=numero)

        elif tipo_valore == "date":
            date_sql = """
            TRY_CONVERT(date, document_fields.valore, 23)
            """

            if data_da:
                stmt = stmt.where(text(f"{date_sql} IS NOT NULL"))
                stmt = stmt.where(text(f"{date_sql} >= :data_da")).params(data_da=str(data_da))

            if data_a:
                stmt = stmt.where(text(f"{date_sql} IS NOT NULL"))
                stmt = stmt.where(text(f"{date_sql} <= :data_a")).params(data_a=str(data_a))

            elif valore:
                stmt = stmt.where(document_fields.c.valore.ilike(f"%{valore}%"))

        else:
            if valore is not None:
                stmt = stmt.where(document_fields.c.valore.ilike(f"%{valore}%"))

    for cond in conditions:
        if not isinstance(cond, dict):
            continue

        target_norm = normalize_text(str(cond.get("target") or ""))
        value_type = str(cond.get("value_type") or "").strip().lower()
        operator = str(cond.get("operator") or "").strip().lower()
        field_name = cond.get("field")
        cond_value = cond.get("value")

        if target_norm not in {"business field", "business_field"}:
            continue

        if value_type != "date":
            continue

        normalized_field = normalize_query_field_name(tipo_doc, field_name) or field_name

        if primary_date_filter_active and normalized_field == primary_structured_field:
            continue

        if not joined_document_fields:
            stmt = stmt.join(
                document_fields,
                documents.c.id == document_fields.c.document_id
            )
            joined_document_fields = True

        field_aliases = get_structured_field_aliases(tipo_doc, normalized_field)
        print("[STRUCTURED FIELD ALIASES - CONDITIONS]", field_aliases)

        if field_aliases:
            stmt = stmt.where(
                or_(*[
                    document_fields.c.campo.ilike(f"%{alias}%")
                    for alias in field_aliases
                ])
            )

        date_sql = """
        TRY_CONVERT(date, document_fields.valore, 23)
        """

        if operator == "between" and isinstance(cond_value, list) and len(cond_value) >= 2:
            stmt = stmt.where(text(f"{date_sql} IS NOT NULL"))
            stmt = stmt.where(text(f"{date_sql} >= :cond_date_from")).params(cond_date_from=str(cond_value[0]))
            stmt = stmt.where(text(f"{date_sql} <= :cond_date_to")).params(cond_date_to=str(cond_value[1]))

        elif operator in {">", ">="} and cond_value:
            stmt = stmt.where(text(f"{date_sql} IS NOT NULL"))
            stmt = stmt.where(text(f"{date_sql} >= :cond_date_from")).params(cond_date_from=str(cond_value))

        elif operator in {"<", "<="} and cond_value:
            stmt = stmt.where(text(f"{date_sql} IS NOT NULL"))
            stmt = stmt.where(text(f"{date_sql} <= :cond_date_to")).params(cond_date_to=str(cond_value))

        elif cond_value:
            stmt = stmt.where(document_fields.c.valore.ilike(f"%{cond_value}%"))

    if not conditions and tipo_doc and not is_generic_document_type(tipo_doc):
        tipo_conditions = build_tipo_documento_sql_conditions(tipo_doc)
        if tipo_conditions:
            stmt = stmt.where(or_(*tipo_conditions))

    stmt = stmt.distinct()
    structured_rows = (await conn.execute(stmt)).mappings().all()

    print("[STRUCTURED DEBUG] RESULT IDS:", [r["id"] for r in structured_rows])
    print("[STRUCTURED DEBUG] RESULT TIPI:", [r["tipo_documento"] for r in structured_rows])
    print("[STRUCTURED DEBUG] COUNT:", len(structured_rows))
    print("[STRUCTURED DEBUG] ====================\n")

    return structured_rows