# indicizza_vecchi_documenti.py

from sqlalchemy import select
from src.db_setup import engine, documents, document_fields
from src.ai_search import index_document
from collections import defaultdict

print("Indicizzazione documenti esistenti...")

with engine.connect() as conn:

    docs = conn.execute(select(documents)).fetchall()

    for doc in docs:

        fields = conn.execute(
            select(document_fields).where(document_fields.c.document_id == doc.id)
        ).fetchall()

        text = doc.tipo_documento + " "

        for f in fields:
            text += f"{f.campo} {f.valore} "

        index_document(doc.id, text)

        print(f"Indicizzato documento {doc.id}")

print("Indicizzazione completata")