# src/process_document.py

import os
import asyncio
from pathlib import Path
from datetime import datetime
from src.ai_search import index_document

from .main import (
    extract_text,
    extract_text_from_pdf,
    parse_data_with_gpt,
    save_document,
    generate_word
)

UPLOADS_DIR = Path(__file__).parent / "uploads"


def process_document(file_path: str):

    print(f"[PROCESS] Elaborazione documento: {file_path}")

    # 1️⃣ Estrazione testo
    ext = os.path.splitext(file_path)[1].lower()

    if ext == ".pdf":
        text = extract_text_from_pdf(file_path)
    else:
        text = extract_text(file_path)

    print(f"[PROCESS] Testo estratto, lunghezza: {len(text)} caratteri")

    # 2️⃣ Parsing GPT
    doc_type, fields = parse_data_with_gpt(text)

    print(f"[PROCESS] Tipo documento: {doc_type}")
    print(f"[PROCESS] Campi estratti: {fields}")

    # 3️⃣ Salvataggio DB
    doc_id, _ = save_document(doc_type, file_path, fields)

    print(f"[PROCESS] Documento salvato con ID: {doc_id}")
    
    text_for_ai = doc_type + " "

    for k, v in fields.items():
        text_for_ai += f"{k} {v} "

    index_document(doc_id, text_for_ai)

    # 4️⃣ Generazione Word
    word_path = generate_word(fields, file_path)

    print(f"[PROCESS] File Word generato: {word_path}")

    # 5️⃣ WebSocket notify
    try:
        from .api_server import notify_clients

        doc_dict = {
            "status": "completed",
            "id": doc_id,
            "tipo_documento": doc_type,
            "file_path": file_path,
            "data_creazione": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "campi": fields
        }

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(notify_clients(doc_dict))
        except RuntimeError:
            asyncio.run(notify_clients(doc_dict))

        print("[PROCESS] WebSocket inviato")

    except Exception as e:
        print(f"[PROCESS] Errore WebSocket: {e}")

    return doc_id, word_path