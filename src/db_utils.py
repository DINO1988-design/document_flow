# src/db_utils.py
from sqlalchemy import insert
from src.db_setup import async_engine, documents, document_fields
from datetime import datetime
import json


async def save_document_async(
    tipo_documento,
    file_path,
    fields,
    canonical_fields=None,
    ocr_text=None,
    ocr_pages=None
):
    canonical_fields = canonical_fields or {}

    async with async_engine.begin() as conn:
        result = await conn.execute(
            insert(documents).values(
                tipo_documento=tipo_documento,
                file_path=file_path,
                ocr_text=ocr_text,
                ocr_pages=ocr_pages,
                campi_canonici_json=json.dumps(canonical_fields, ensure_ascii=False),
                data_creazione=datetime.utcnow()
            )
        )
        document_id = result.inserted_primary_key[0]

        for campo, valore in fields.items():
            if isinstance(valore, (dict, list)):
                valore = json.dumps(valore, ensure_ascii=False)
            if isinstance(valore, str) and len(valore) > 4000:
                valore = valore[:4000]

            await conn.execute(
                insert(document_fields).values(
                    document_id=document_id,
                    campo=campo,
                    valore=valore
                )
            )

    return document_id