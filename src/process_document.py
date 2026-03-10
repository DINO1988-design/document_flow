# src/process_document.py
import os
from pathlib import Path
from .main import extract_text, extract_text_from_pdf, parse_data_with_gpt, save_document, generate_word
from .ws_utils import notify_clients
from datetime import datetime

UPLOADS_DIR = Path(__file__).parent / "uploads"

def process_document(file_path: str):
    """
    Elabora un documento PDF o immagine:
    1. Estrae il testo
    2. Estrae i dati con GPT
    3. Salva il documento nel DB (document + document_fields)
    4. Genera un file Word
    5. Notifica i client via WebSocket
    """
    print(f"[PROCESS] Elaborazione documento: {file_path}")

    # 1️⃣ Estrazione testo
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".pdf":
        text = extract_text_from_pdf(file_path)
    else:
        text = extract_text(file_path)
    print(f"[PROCESS] Testo estratto, lunghezza: {len(text)} caratteri")

    # 2️⃣ Parsing dati con GPT
    doc_type, fields = parse_data_with_gpt(text)
    print(f"[PROCESS] Tipo documento: {doc_type}")
    print(f"[PROCESS] Campi estratti: {fields}")

    # 3️⃣ Salvataggio documento e campi nel DB
    doc_id = save_document(doc_type, file_path, fields)
    print(f"[PROCESS] Documento salvato con ID: {doc_id}")

    # 4️⃣ Generazione Word
    word_path = generate_word(fields, file_path)
    print(f"[PROCESS] File Word generato: {word_path}")

# 5️⃣ Notifica WebSocket
    try:
        from .api_server import notify_clients
        from datetime import datetime  # <-- aggiungi questa riga
        import asyncio

    # Prepara dict completo per il frontend
        doc_dict = {
            "id": doc_id,
            "tipo_documento": doc_type,
            "file_path": file_path,
            "data_creazione": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "campi": fields
        }
        asyncio.run(notify_clients(doc_dict))
        print(f"[PROCESS] Client notificati via WebSocket")
    except ImportError:
        print("[PROCESS] WebSocket non disponibile, skip notifiche")

    return doc_id, word_path