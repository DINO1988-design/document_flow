# src/ws_utils.py

import json
import os
from datetime import datetime

# Lista WebSocket attivi
# clients = []

# --------------------------
# Notifica i client WebSocket
# --------------------------
async def notify_clients(doc: dict):
    """
    Invia a tutti i client WebSocket attivi i dati del documento
    """
    to_remove = []
    for ws in clients:
        try:
            await ws.send_text(json.dumps({"event": "document_ready", "doc": doc}))
        except:
            to_remove.append(ws)
    for ws in to_remove:
        clients.remove(ws)

# --------------------------
# Estrazione testo da file
# --------------------------
def estrai_testo_da_file(file_path: str) -> str:
    """
    Estrae solo il testo da un file PDF o immagine.
    Restituisce il testo come stringa.
    Import locali per evitare circular import.
    """
    ext = os.path.splitext(file_path)[1].lower()

    # Import locale dentro la funzione per evitare circular import
    from .main import extract_text, extract_text_from_pdf

    if ext == ".pdf":
        return extract_text_from_pdf(file_path)
    else:
        return extract_text(file_path)