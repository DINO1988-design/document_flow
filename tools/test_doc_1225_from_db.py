import os
import sys
import json
import asyncio

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy import select
from src.db_setup import async_engine, documents
from src.document_field_flattener import flatten_document_fields
from src.main import (
    clean_ocr_text,
    parse_document_with_gpt,      # unified
    parse_data_with_gpt_bootstrap,
    parse_data_with_gpt,
)

DOC_ID = 1225
SEARCH_TERMS = [
    "slimani",
    "noureddine",
    "slimani noureddi",
    "slimani noureddine",
]


def contains_any(text: str, terms: list[str]) -> bool:
    if not text:
        return False
    text_lower = text.lower()
    return any(term.lower() in text_lower for term in terms)


def pretty(obj):
    try:
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return str(obj)


async def load_doc_from_db(doc_id: int):
    async with async_engine.connect() as conn:
        result = await conn.execute(
            select(
                documents.c.id,
                documents.c.tipo_documento,
                documents.c.file_path,
                documents.c.ocr_text,
                documents.c.ocr_pages,
            ).where(documents.c.id == doc_id)
        )
        row = result.mappings().first()
        return row


async def main():
    row = await load_doc_from_db(DOC_ID)

    if not row:
        print(f"[ERRORE] Documento {DOC_ID} non trovato")
        return

    file_path = row.get("file_path")
    db_tipo = row.get("tipo_documento")
    ocr_text = row.get("ocr_text") or ""

    print("\n" + "=" * 100)
    print("[DEBUG DB DOCUMENT]")
    print("DOC_ID:", row.get("id"))
    print("FILE:", file_path)
    print("TIPO DB:", db_tipo)
    print("OCR LEN:", len(ocr_text))
    print("=" * 100)

    cleaned = clean_ocr_text(ocr_text)

    print("\n" + "=" * 100)
    print("[DEBUG OCR CLEANED - FIRST 5000 CHARS]")
    print(cleaned[:5000])
    print("=" * 100)

    print("\n[CHECK OCR]")
    for term in SEARCH_TERMS:
        print(f"contains '{term}'? ->", term.lower() in cleaned.lower())

    # --------------------------------------------------
    # 1) GPT UNIFIED
    # --------------------------------------------------
    unified_tipo, unified_fields = parse_document_with_gpt(cleaned)
    unified_flat = flatten_document_fields(unified_fields or {})

    print("\n" + "=" * 100)
    print("[UNIFIED RESULT]")
    print("TIPO:", unified_tipo)
    print("FIELDS:")
    print(pretty(unified_fields))
    print("FLAT_FIELDS:")
    print(pretty(unified_flat))
    print("=" * 100)

    unified_text_blob = pretty(unified_fields) + "\n" + pretty(unified_flat)
    print("\n[CHECK UNIFIED]")
    for term in SEARCH_TERMS:
        print(f"contains '{term}'? ->", term.lower() in unified_text_blob.lower())

    # --------------------------------------------------
    # 2) GPT BOOTSTRAP
    # --------------------------------------------------
    bootstrap_tipo, bootstrap_fields = parse_data_with_gpt_bootstrap(cleaned)
    bootstrap_flat = flatten_document_fields(bootstrap_fields or {})

    print("\n" + "=" * 100)
    print("[BOOTSTRAP RESULT]")
    print("TIPO:", bootstrap_tipo)
    print("FIELDS:")
    print(pretty(bootstrap_fields))
    print("FLAT_FIELDS:")
    print(pretty(bootstrap_flat))
    print("=" * 100)

    bootstrap_text_blob = pretty(bootstrap_fields) + "\n" + pretty(bootstrap_flat)
    print("\n[CHECK BOOTSTRAP]")
    for term in SEARCH_TERMS:
        print(f"contains '{term}'? ->", term.lower() in bootstrap_text_blob.lower())

    # --------------------------------------------------
    # 3) GPT SCHEMA-DRIVEN
    # usa il tipo bootstrap, oppure fallback al tipo db
    # --------------------------------------------------
    schema_doc_type = bootstrap_tipo or db_tipo or "generico"
    schema_tipo, schema_fields = parse_data_with_gpt(cleaned, schema_doc_type)
    schema_flat = flatten_document_fields(schema_fields or {})

    print("\n" + "=" * 100)
    print("[SCHEMA RESULT]")
    print("INPUT DOC TYPE:", schema_doc_type)
    print("OUTPUT TIPO:", schema_tipo)
    print("FIELDS:")
    print(pretty(schema_fields))
    print("FLAT_FIELDS:")
    print(pretty(schema_flat))
    print("=" * 100)

    schema_text_blob = pretty(schema_fields) + "\n" + pretty(schema_flat)
    print("\n[CHECK SCHEMA]")
    for term in SEARCH_TERMS:
        print(f"contains '{term}'? ->", term.lower() in schema_text_blob.lower())

    print("\n" + "=" * 100)
    print("[SUMMARY]")
    print("OCR has target name?      ->", contains_any(cleaned, SEARCH_TERMS))
    print("UNIFIED has target name?  ->", contains_any(unified_text_blob, SEARCH_TERMS))
    print("BOOTSTRAP has target name?->", contains_any(bootstrap_text_blob, SEARCH_TERMS))
    print("SCHEMA has target name?   ->", contains_any(schema_text_blob, SEARCH_TERMS))
    print("=" * 100 + "\n")


if __name__ == "__main__":
    asyncio.run(main())