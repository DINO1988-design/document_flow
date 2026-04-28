# src/ai_search.py

from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
import pickle
import json
from pathlib import Path
import threading
import re
import os

from src.document_field_flattener import flatten_document_fields
# ===============================
# MODELLO EMBEDDING
# ===============================

model = SentenceTransformer("all-MiniLM-L6-v2")

# ===============================
# FILE INDICE
# ===============================
# ===============================
# MEMORIA
# ===============================

index = None
documents = []   # lista testi
doc_ids = []     # id documenti

index_lock = threading.Lock()

INDEX_FILE = Path("faiss_index.bin")
DOCSTORE_FILE = Path("faiss_docs.pkl")

# =========================
# INDICE CHUNK
# =========================
chunk_lock = threading.Lock()

chunk_index = None
chunk_store = {
    "chunks": [],
    "doc_ids": [],
    "chunk_indexes": [],
    "page_numbers": []
}
CHUNK_INDEX_FILE = Path("faiss_chunk_index.bin")
CHUNK_STORE_FILE = Path("faiss_chunk_store.pkl")


# ===============================
# CREA INDICE
# ===============================

def init_index():

    global index

    dim = 384  # dimensione embedding MiniLM
    index = faiss.IndexFlatIP(dim)


# ===============================
# CARICA INDICE DA DISCO
# ===============================

def load_index():

    global index, documents, doc_ids

    # Controlla che esistano entrambi i file
    if INDEX_FILE.exists() and DOCSTORE_FILE.exists():

        try:
            index = faiss.read_index(str(INDEX_FILE))

            with open(DOCSTORE_FILE, "rb") as f:
                data = pickle.load(f)

            documents = data.get("documents", [])
            doc_ids = data.get("doc_ids", [])

            print(f"[AI_SEARCH] Indice caricato: {len(documents)} documenti")

        except Exception as e:
            print(f"[AI_SEARCH] Errore caricamento indice: {e}")
            print("[AI_SEARCH] Ricreazione indice...")

            init_index()
            documents = []
            doc_ids = []

    else:
        print("[AI_SEARCH] Nessun indice trovato, inizializzazione nuovo indice")

        init_index()
        documents = []
        doc_ids = []


# ===============================
# SALVA INDICE
# ===============================

def save_index():
    global index
    if index is None:
        init_index()

    faiss.write_index(index, str(INDEX_FILE))
    with open(DOCSTORE_FILE, "wb") as f:
        pickle.dump(
            {
                "documents": documents,
                "doc_ids": doc_ids
            },
            f
        )

# ===============================
# AGGIUNGI DOCUMENTO
# ===============================
def add_document(doc_id: int, text: str):
    global index

    with index_lock:
        if index is None:
            init_index()

        if doc_id in doc_ids:
            print(f"[AI_SEARCH] Documento {doc_id} già indicizzato")
            return

        embedding = model.encode([text], normalize_embeddings=True)
        embedding = np.array(embedding).astype("float32")

        index.add(embedding)
        documents.append(text)
        doc_ids.append(doc_id)

        save_index()

# ===============================
# RICERCA SEMANTICA
# ===============================

def search(query: str, top_k=5):
    global index

    with index_lock:
        if index is None:
            init_index()
            return []

        if index.ntotal == 0:
            return []

        query_vec = model.encode([query], normalize_embeddings=True)
        query_vec = np.array(query_vec).astype("float32")

        distances, indices = index.search(query_vec, min(top_k, index.ntotal))
        results = []

        for rank, idx in enumerate(indices[0]):
            if idx == -1:
                continue

            if 0 <= idx < len(documents) and 0 <= idx < len(doc_ids):
                results.append({
                    "doc_id": doc_ids[idx],
                    "text": documents[idx],
                    "score": float(distances[0][rank])
                })
            else:
                print(
                    f"Attenzione: idx fuori range! "
                    f"idx={idx}, len(documents)={len(documents)}, len(doc_ids)={len(doc_ids)}"
                )

        return results
    
    # alias per compatibilità
index_document = add_document


# ===============================
# RICOSTRUZIONE INDICE DA DATI
# ===============================

def rebuild_index_from_items(items: list[dict]):
    """
    Ricrea completamente l'indice FAISS partendo da una lista di item:
    [
        {"doc_id": 1, "text": "..."},
        {"doc_id": 2, "text": "..."}
    ]
    """
    global index, documents, doc_ids

    with index_lock:
        init_index()
        documents = []
        doc_ids = []

        if not items:
            save_index()
            print("[AI_SEARCH] Indice ricostruito: 0 documenti")
            return

        texts = [item["text"] for item in items]
        ids = [item["doc_id"] for item in items]

        embeddings = model.encode(texts, normalize_embeddings=True)
        embeddings = np.array(embeddings).astype("float32")

        index.add(embeddings)
        documents = texts
        doc_ids = ids

        save_index()


def reset_index():
    """
    Svuota completamente l'indice.
    """
    global index, documents, doc_ids

    with index_lock:
        init_index()
        documents = []
        doc_ids = []
        save_index()

    print("[AI_SEARCH] Indice azzerato")
    
def get_index_count() -> int:
    global index
    if index is None:
        return 0
    return int(index.ntotal)
    
def normalize_text_for_index(value: str) -> str:
    if not value:
        return ""
    value = str(value)
    value = value.replace("\r", " ").replace("\n", " ")
    value = re.sub(r"\s+", " ", value).strip()
    return value
    
def build_text_for_ai(
    tipo_documento: str,
    file_path: str,
    fields: dict,
    canonical_fields: dict | None = None,
    ocr_text: str | None = None
) -> str:
    parts = []

    tipo_documento = (tipo_documento or "documento").strip()
    canonical_fields = canonical_fields or {}
    fields = fields or {}

    parts.append(f"Documento di tipo {tipo_documento}.")

    if file_path:
        file_name = os.path.basename(file_path)
        parts.append(f"Nome file: {file_name}.")

    preferred_keys = [
        "nome", "cognome", "intestatario", "passenger_name",
        "numero_passaporto", "booking_reference", "ticket_number",
        "nazionalità", "nazionalita", "data_di_nascita",
        "luogo_di_nascita", "data_di_scadenza",
        "compagnia", "flight", "agenzia", "email",
        "codice_fiscale", "ral", "data_inizio", "data_fine",
        "beneficiario", "contraente", "employee_name", "full_name"
    ]

    used_keys = set()

    for key in preferred_keys:
        value = canonical_fields.get(key)
        if value in [None, "", []]:
            continue
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        parts.append(f"{key.replace('_', ' ')}: {value}.")
        used_keys.add(key)

    for key, value in canonical_fields.items():
        if key in used_keys or value in [None, "", []]:
            continue
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        parts.append(f"{key.replace('_', ' ')}: {value}.")
        used_keys.add(key)

    flat_fields = flatten_document_fields(fields)

    for key in preferred_keys:
        if key in used_keys:
            continue
        value = flat_fields.get(key)
        if value in [None, "", []]:
            continue
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        parts.append(f"{key.replace('_', ' ')}: {value}.")
        used_keys.add(key)

    for key, value in flat_fields.items():
        if key in used_keys or value in [None, "", []]:
            continue
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)
        parts.append(f"{key.replace('_', ' ')}: {value}.")
        used_keys.add(key)

    cleaned_ocr = normalize_text_for_index(ocr_text or "")
    if cleaned_ocr:
        parts.append(f"Testo OCR: {cleaned_ocr}.")

    return " ".join(parts)
# =========================
# INDICE CHUNK
# =========================

def normalize_chunk_text(text: str) -> str:
    if not text:
        return ""
    text = str(text).replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def split_text_into_chunks(text: str, chunk_size: int = 220, overlap: int = 40) -> list[str]:
    """
    Divide il testo in chunk per numero di parole.
    chunk_size = numero parole per chunk
    overlap = parole condivise col chunk successivo
    """
    text = normalize_chunk_text(text)
    if not text:
        return []

    words = text.split()
    if not words:
        return []

    if len(words) <= chunk_size:
        return [" ".join(words)]

    chunks = []
    start = 0
    step = max(chunk_size - overlap, 1)

    while start < len(words):
        end = start + chunk_size
        chunk_words = words[start:end]

        if chunk_words:
            chunks.append(" ".join(chunk_words))

        if end >= len(words):
            break

        start += step

    return chunks


def load_chunk_index():
    global chunk_index, chunk_store

    if CHUNK_INDEX_FILE.exists() and CHUNK_STORE_FILE.exists():
        try:
            with chunk_lock:
                chunk_index = faiss.read_index(str(CHUNK_INDEX_FILE))
                with open(CHUNK_STORE_FILE, "rb") as f:
                    chunk_store = pickle.load(f)

            if not isinstance(chunk_store, dict):
                raise ValueError("chunk_store non valido")

            chunk_store.setdefault("chunks", [])
            chunk_store.setdefault("doc_ids", [])
            chunk_store.setdefault("chunk_indexes", [])
            chunk_store.setdefault("page_numbers", [])

            # riallinea page_numbers se il vecchio store è incompleto
            total_chunks = len(chunk_store["chunks"])
            total_doc_ids = len(chunk_store["doc_ids"])
            total_chunk_indexes = len(chunk_store["chunk_indexes"])
            total_page_numbers = len(chunk_store["page_numbers"])

            if not (total_chunks == total_doc_ids == total_chunk_indexes):
                raise ValueError(
                    f"Chunk store disallineato: "
                    f"chunks={total_chunks}, doc_ids={total_doc_ids}, chunk_indexes={total_chunk_indexes}"
                )

            if total_page_numbers < total_chunks:
                missing = total_chunks - total_page_numbers
                chunk_store["page_numbers"].extend([None] * missing)

            elif total_page_numbers > total_chunks:
                chunk_store["page_numbers"] = chunk_store["page_numbers"][:total_chunks]

            print(f"[AI_SEARCH] Chunk index caricato: {len(chunk_store['doc_ids'])} chunk")
            return

        except Exception as e:
            print(f"[AI_SEARCH] Errore caricamento chunk index, reset: {e}")

    reset_chunk_index()


def reset_chunk_index():
    global chunk_index, chunk_store

    with chunk_lock:
        dim = 384
        chunk_index = faiss.IndexFlatIP(dim)
        chunk_store = {
            "chunks": [],
            "doc_ids": [],
            "chunk_indexes": [],
            "page_numbers": []
        }

def save_chunk_index():
    global chunk_index, chunk_store

    if chunk_index is None:
        reset_chunk_index()

    with chunk_lock:
        faiss.write_index(chunk_index, str(CHUNK_INDEX_FILE))
        with open(CHUNK_STORE_FILE, "wb") as f:
            pickle.dump(chunk_store, f)


def add_document_chunks(doc_id: int, text: str, context_prefix: str = ""):
    global chunk_index, chunk_store

    if chunk_index is None:
        reset_chunk_index()

    chunks = split_text_into_chunks(text, chunk_size=220, overlap=40)
    if not chunks:
        return

    if context_prefix:
        chunks = [f"{context_prefix} {chunk}".strip() for chunk in chunks]

    embeddings = model.encode(chunks, normalize_embeddings=True)
    embeddings = np.array(embeddings).astype("float32")

    with chunk_lock:
        chunk_index.add(embeddings)

        current_count = len(chunk_store["chunk_indexes"])

        for i, chunk in enumerate(chunks):
            chunk_store["chunks"].append(chunk)
            chunk_store["doc_ids"].append(doc_id)
            chunk_store["chunk_indexes"].append(current_count + i)
            chunk_store["page_numbers"].append(None)   # <-- AGGIUNGI QUESTO

    save_chunk_index()
    
def add_document_page_chunks(doc_id: int, page_texts: list[str], context_prefix: str = ""):
    global chunk_index, chunk_store

    if chunk_index is None:
        reset_chunk_index()

    all_chunks = []
    all_pages = []

    for page_no, page_text in enumerate(page_texts, start=1):
        page_chunks = split_text_into_chunks(page_text, chunk_size=220, overlap=40)

        for chunk in page_chunks:
            full_chunk = f"{context_prefix} {chunk}".strip() if context_prefix else chunk
            all_chunks.append(full_chunk)
            all_pages.append(page_no)

    if not all_chunks:
        return

    embeddings = model.encode(all_chunks, normalize_embeddings=True)
    embeddings = np.array(embeddings).astype("float32")

    with chunk_lock:
        chunk_index.add(embeddings)

        current_count = len(chunk_store["chunk_indexes"])

        for i, chunk in enumerate(all_chunks):
            chunk_store["chunks"].append(chunk)
            chunk_store["doc_ids"].append(doc_id)
            chunk_store["chunk_indexes"].append(current_count + i)
            chunk_store["page_numbers"].append(all_pages[i])

    save_chunk_index()

def search_chunks(query: str, top_k: int = 20):
    global chunk_index, chunk_store

    with chunk_lock:
        if not query or chunk_index is None or chunk_index.ntotal == 0:
            return []

        query_embedding = model.encode([query], normalize_embeddings=True)
        query_embedding = np.array(query_embedding).astype("float32")

        k = min(top_k, chunk_index.ntotal)
        scores, indices = chunk_index.search(query_embedding, k)

        results = []

        for score, idx in zip(scores[0], indices[0]):
            if (
                idx < 0
                or idx >= len(chunk_store["doc_ids"])
                or idx >= len(chunk_store["chunks"])
                or idx >= len(chunk_store["chunk_indexes"])
            ):
                continue

            page_number = None
            if idx < len(chunk_store.get("page_numbers", [])):
                page_number = chunk_store["page_numbers"][idx]

            results.append({
                "doc_id": chunk_store["doc_ids"][idx],
                "chunk_index": chunk_store["chunk_indexes"][idx],
                "text": chunk_store["chunks"][idx],
                "score": float(score),
                "page_number": page_number
            })

        return results

def get_chunk_index_count() -> int:
    global chunk_index
    if chunk_index is None:
        return 0
    return int(chunk_index.ntotal)

def get_chunk_doc_ids() -> set[int]:
    global chunk_store
    if not chunk_store or "doc_ids" not in chunk_store:
        return set()
    return set(chunk_store.get("doc_ids", []))
    
def get_chunk_store_count() -> int:
    global chunk_store
    if not chunk_store:
        return 0
    return len(chunk_store.get("doc_ids", []))

def debug_chunk_store_lengths():
    global chunk_store

    print("[CHUNK STORE DEBUG]")
    print("chunks =", len(chunk_store.get("chunks", [])))
    print("doc_ids =", len(chunk_store.get("doc_ids", [])))
    print("chunk_indexes =", len(chunk_store.get("chunk_indexes", [])))
    print("page_numbers =", len(chunk_store.get("page_numbers", [])))   
   
    
def build_chunk_context_prefix(
    tipo_documento: str,
    file_path: str,
    fields: dict,
    canonical_fields: dict | None = None
) -> str:
    parts = []

    tipo_documento = (tipo_documento or "documento").strip()
    canonical_fields = canonical_fields or {}
    fields = fields or {}

    parts.append(f"Documento di tipo {tipo_documento}.")

    if file_path:
        file_name = os.path.basename(file_path)
        parts.append(f"Nome file: {file_name}.")

    preferred_keys = [
        "nome", "cognome", "intestatario", "passenger_name",
        "full_name", "employee_name", "beneficiario", "contraente",
        "codice_fiscale", "numero_passaporto",
        "booking_reference", "ticket_number",
        "ral", "data_inizio", "data_fine",
        "data_di_nascita", "data_di_scadenza",
        "compagnia", "flight", "agenzia", "email"
    ]

    flat_fields = flatten_document_fields(fields)
    used_keys = set()

    for key in preferred_keys:
        value = canonical_fields.get(key)
        if value in [None, "", []]:
            value = flat_fields.get(key)

        if value in [None, "", []]:
            continue

        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)

        parts.append(f"{key.replace('_', ' ')}: {value}.")
        used_keys.add(key)

    return " ".join(parts).strip()