# src/api_server.py

from fastapi import FastAPI, Request, File, UploadFile, Query, WebSocket, BackgroundTasks
from typing import List
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import os, json, zipfile
from io import BytesIO
from src.db_setup import async_engine, async_session, documents, document_fields, document_jobs, clients
from src.process_document import process_document
from src.main import interpret_search_query
from sqlalchemy import select, cast, Float, insert, delete, text, or_
from fastapi.responses import JSONResponse
from src.ai_search import (
    load_index,
    search,
    rebuild_index_from_items,
    reset_index,
    get_index_count,
    build_text_for_ai,
    build_chunk_context_prefix,
    load_chunk_index,
    reset_chunk_index,
    save_chunk_index,
    add_document_chunks,
    add_document_page_chunks,
    search_chunks,
    get_chunk_index_count,
    get_chunk_doc_ids,
    get_chunk_store_count,
    debug_chunk_store_lengths   # <-- AGGIUNGI QUESTO
)
import asyncio
from sqlalchemy.exc import DBAPIError, OperationalError
from fastapi import status
import re
import unicodedata
from difflib import SequenceMatcher



faiss_rebuild_lock = asyncio.Lock()
templates = Jinja2Templates(directory="src/templates")

def normalize_text(value: str) -> str:
    if value is None:
        return ""

    value = str(value).strip().lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(c for c in value if not unicodedata.combining(c))
    value = re.sub(r"[_\-/\\]", " ", value)
    value = re.sub(r"[^\w\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value
def build_chunk_preview(text: str, max_len: int = 280) -> str:
    if not text:
        return ""

    text = str(text).strip()
    text = re.sub(r"\s+", " ", text)

    if len(text) <= max_len:
        return text

    return text[:max_len].rstrip() + "..."
def build_match_excerpt(text: str, query: str, window: int = 120) -> str:
    if not text or not query:
        return ""

    clean_text = re.sub(r"\s+", " ", str(text)).strip()
    tokens = [t for t in tokenize_text(query) if len(t) >= 3]

    if not tokens:
        return clean_text[:240] + ("..." if len(clean_text) > 240 else "")

    best_pos = -1

    # trova il primo match significativo
    for token in sorted(tokens, key=len, reverse=True):
        m = re.search(re.escape(token), clean_text, re.IGNORECASE)
        if m:
            best_pos = m.start()
            break

    # fallback: nessun match trovato
    if best_pos < 0:
        return clean_text[:240] + ("..." if len(clean_text) > 240 else "")

    start = max(0, best_pos - window)
    end = min(len(clean_text), best_pos + window)

    excerpt = clean_text[start:end].strip()

    if start > 0:
        excerpt = "... " + excerpt
    if end < len(clean_text):
        excerpt = excerpt + " ..."

    return excerpt

def highlight_query_terms(text: str, query: str) -> str:
    if not text or not query:
        return text or ""

    stopwords = {
        "di", "del", "della", "dello", "dei", "degli", "delle",
        "il", "lo", "la", "i", "gli", "le",
        "un", "uno", "una",
        "e", "ed", "o",
        "a", "da", "in", "con", "su", "per", "tra", "fra"
    }

    result = text
    query_tokens = []
    seen = set()

    for token in tokenize_text(query):
        if token in stopwords:
            continue
        if len(token) < 3:
            continue
        if token not in seen:
            seen.add(token)
            query_tokens.append(token)

    query_tokens.sort(key=len, reverse=True)

    for token in query_tokens:
        pattern = re.compile(rf"({re.escape(token)})", re.IGNORECASE)
        result = pattern.sub(r"<mark>\1</mark>", result)

    return result


def tokenize_text(value: str) -> list[str]:
    norm = normalize_text(value)
    if not norm:
        return []
    return [token for token in norm.split() if len(token) > 1]
    
def text_similarity(a: str, b: str) -> float:
    a_norm = normalize_text(a)
    b_norm = normalize_text(b)

    if not a_norm or not b_norm:
        return 0.0

    return SequenceMatcher(None, a_norm, b_norm).ratio()


def token_fuzzy_match(token: str, text: str, min_ratio: float = 0.82) -> bool:
    token_norm = normalize_text(token)
    text_norm = normalize_text(text)

    if not token_norm or not text_norm:
        return False

    words = text_norm.split()

    for w in words:
        if w == token_norm:
            return True

        if SequenceMatcher(None, token_norm, w).ratio() >= min_ratio:
            return True

    return False
    
def are_similar_tokens(a: str, b: str, threshold: float = 0.84) -> bool:
    a = normalize_text(a)
    b = normalize_text(b)

    if not a or not b:
        return False

    if a == b:
        return True

    return SequenceMatcher(None, a, b).ratio() >= threshold

def count_similar_token_matches(query_tokens: list[str], target_text: str, threshold: float = 0.84) -> int:
    if not query_tokens or not target_text:
        return 0

    target_tokens = tokenize_text(target_text)
    if not target_tokens:
        return 0

    matched = 0
    used_targets = set()

    for q in query_tokens:
        for i, t in enumerate(target_tokens):
            if i in used_targets:
                continue

            if are_similar_tokens(q, t, threshold=threshold):
                matched += 1
                used_targets.add(i)
                break

    return matched
def raw_ocr_person_match(persona: str, raw_text: str) -> bool:
    persona_norm = normalize_text(persona)
    raw_norm = normalize_text(raw_text)

    if not persona_norm or not raw_norm:
        return False

    if persona_norm in raw_norm:
        return True

    persona_tokens = tokenize_text(persona_norm)
    if not persona_tokens:
        return False

    matched = count_similar_token_matches(persona_tokens, raw_norm, threshold=0.82)

    if len(persona_tokens) == 1:
        return matched >= 1

    return matched >= min(2, len(persona_tokens))
def has_fuzzy_person_match(fields: dict, person_tokens: list[str]) -> bool:
    if not fields or not person_tokens:
        return False

    field_groups = extract_field_groups(fields)

    strong_text = field_groups["person_strong_text"]
    medium_text = field_groups["person_medium_text"]
    generic_text = field_groups["generic_text"]

    strong_matches = count_similar_token_matches(person_tokens, strong_text, threshold=0.84)
    medium_matches = count_similar_token_matches(person_tokens, medium_text, threshold=0.84)
    generic_matches = count_similar_token_matches(person_tokens, generic_text, threshold=0.88)

    total_tokens = len(person_tokens)

    if total_tokens == 1:
        return (strong_matches + medium_matches + generic_matches) > 0

    return (
        strong_matches >= 1
        or medium_matches >= 2
        or (strong_matches + medium_matches) >= 2
        or (strong_matches + medium_matches + generic_matches) >= 2
    )
    
def build_searchable_text(tipo_documento: str, fields: dict) -> str:
    parts = []

    if tipo_documento:
        parts.append(str(tipo_documento))

    for k, v in fields.items():
        parts.append(str(k))

        if v is None:
            continue

        if isinstance(v, dict):
            for sk, sv in v.items():
                parts.append(f"{sk} {sv}")

        elif isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    for ik, iv in item.items():
                        parts.append(f"{ik} {iv}")
                else:
                    parts.append(str(item))
        else:
            parts.append(str(v))

    return normalize_text(" ".join(parts))
    
def get_normalized_filename(file_path: str) -> str:
    if not file_path:
        return ""
    return normalize_text(os.path.basename(file_path))
    
def extract_field_groups(fields: dict) -> dict:
    strong_person_field_names = {
        normalize_text(x) for x in {
            "passenger_name",
            "nome_passeggero",
            "nome_passaggero",
            "nome",
            "cognome",
            "intestatario",
            "full_name",
            "cliente",
            "persona",
            "nome_cliente",
            "beneficiario",
            "holder_name",
            "applicant_name",
            "employee_name",
            "collaboratore",
            "contraente",
            "signed_by",
            "traveller_name",
            "guest",
            "nome_completo"
        }
    }

    medium_person_field_names = {
        normalize_text(x) for x in {
            "traveler",
            "passeggero",
            "guest_name",
            "customer_name",
            "nome1",
            "nome2",
            "first_name",
            "last_name",
            "surname",
            "given_name",
            "recipient",
            "destinatario"
        }
    }

    person_values_strong = []
    person_values_medium = []
    generic_values = []

    for k, v in fields.items():
        key_norm = normalize_text(k)

        if v in [None, "", []]:
            continue

        value_str = str(v)

        if key_norm in strong_person_field_names:
            person_values_strong.append(value_str)
        elif key_norm in medium_person_field_names:
            person_values_medium.append(value_str)

        generic_values.append(f"{k} {value_str}")

    return {
        "person_strong_text": normalize_text(" ".join(person_values_strong)),
        "person_medium_text": normalize_text(" ".join(person_values_medium)),
        "generic_text": normalize_text(" ".join(generic_values))
    }

def is_generic_document_type(value: str) -> bool:
    v = normalize_text(value)
    return v in {"", "generico", "documento", "doc", "file", "altro"}


def document_type_matches(query_tipo: str, row_tipo: str) -> bool:
    q = normalize_text(query_tipo)
    r = normalize_text(row_tipo)

    if not q or not r:
        return False

    ticket_aliases = {
        "ticket", "biglietto", "biglietti", "biglietto aereo",
        "e ticket", "e-ticket", "eticket", "volo", "voli",
        "boarding pass", "biglietto traghetto", "traghetto"
    }

    contract_aliases = {
        "contratto", "contract", "contratti"
    }

    invoice_aliases = {
        "fattura", "invoice", "fatture"
    }

    passport_aliases = {
        "passaporto", "passport", "passeport"
    }

    def belongs(value: str, aliases: set[str]) -> bool:
        return value in aliases or any(alias in value for alias in aliases)

    if belongs(q, ticket_aliases) and belongs(r, ticket_aliases):
        return True

    if belongs(q, contract_aliases) and belongs(r, contract_aliases):
        return True

    if belongs(q, invoice_aliases) and belongs(r, invoice_aliases):
        return True

    if belongs(q, passport_aliases) and belongs(r, passport_aliases):
        return True

    return q in r or r in q
def compute_document_score(
    query: str,
    parsed_query: dict,
    row: dict,
    fields: dict,
    semantic_score: float | None = None,
    best_chunk_text: str | None = None,
    raw_ocr_text: str | None = None
) -> dict:
    query_norm = normalize_text(query)
    tipo_doc_gpt = normalize_text(parsed_query.get("tipo_documento"))
    persona_gpt = normalize_text(parsed_query.get("persona"))
    nome_file_gpt = normalize_text(parsed_query.get("nome_file"))

    tipo_documento = row.get("tipo_documento") or ""
    file_path = row.get("file_path") or ""

    tipo_norm = normalize_text(tipo_documento)
    file_name_norm = get_normalized_filename(file_path)

    searchable_text = build_searchable_text(tipo_documento, fields)
    field_groups = extract_field_groups(fields)

    strong_person_text = field_groups["person_strong_text"]
    medium_person_text = field_groups["person_medium_text"]
    generic_text = field_groups["generic_text"]

    person_tokens = tokenize_text(persona_gpt) if persona_gpt else []

    semantic_score = semantic_score or 0.0
    score = 0.0

    matched_person_tokens_strong = 0
    matched_person_tokens_medium = 0
    matched_person_tokens_generic = 0
    matched_person_tokens_filename = 0

    matched_person_tokens_strong = count_similar_token_matches(person_tokens, strong_person_text, threshold=0.84)
    matched_person_tokens_medium = count_similar_token_matches(person_tokens, medium_person_text, threshold=0.84)
    matched_person_tokens_generic = count_similar_token_matches(person_tokens, generic_text, threshold=0.88)
    matched_person_tokens_filename = count_similar_token_matches(person_tokens, file_name_norm, threshold=0.90)
    matched_person_tokens_chunk = count_similar_token_matches(person_tokens, best_chunk_text or "", threshold=0.84)
    raw_ocr_match = raw_ocr_person_match(persona_gpt, raw_ocr_text or "") if persona_gpt else False    
    
    total_person_tokens = len(person_tokens)

    person_coverage_strong = (
        matched_person_tokens_strong / total_person_tokens
        if total_person_tokens else 0.0
    )
    person_coverage_medium = (
        matched_person_tokens_medium / total_person_tokens
        if total_person_tokens else 0.0
    )
    person_coverage_generic = (
        matched_person_tokens_generic / total_person_tokens
        if total_person_tokens else 0.0
    )
    person_coverage_filename = (
        matched_person_tokens_filename / total_person_tokens
        if total_person_tokens else 0.0
    )
    
    person_coverage_chunk = (
        matched_person_tokens_chunk / total_person_tokens
        if total_person_tokens else 0.0
    )

    persona_match = (
        matched_person_tokens_strong > 0
        or matched_person_tokens_medium > 0
        or matched_person_tokens_generic > 0
        or matched_person_tokens_chunk > 0
        or raw_ocr_match
    )
    filename_only_match = (
        matched_person_tokens_filename > 0 and not persona_match
    )

    # -------------------------
    # BASE SEMANTICA
    # -------------------------
    # La semantica contribuisce, ma non basta da sola per persona
    score += semantic_score * 0.45

    # -------------------------
    # MATCH PERSONA
    # -------------------------
    # Forte: campi dedicati al nome della persona
    if matched_person_tokens_strong > 0:
        score += 0.55
        score += person_coverage_strong * 0.25

    # Medio: campi persona secondari
    if matched_person_tokens_medium > 0:
        score += 0.30
        score += person_coverage_medium * 0.15

    # Generico: nome trovato in altri campi testuali
    if matched_person_tokens_generic > 0:
        score += 0.18
        score += person_coverage_generic * 0.10
        
    if matched_person_tokens_chunk > 0:
        score += 0.22
        score += person_coverage_chunk * 0.12
    if raw_ocr_match:
        score += 0.28
    
    # Filename: solo un aiutino, mai dominante
    if matched_person_tokens_filename > 0:
        score += min(0.04 * matched_person_tokens_filename, 0.08)

    # Frase completa persona
    if persona_gpt:
        if persona_gpt in strong_person_text or text_similarity(persona_gpt, strong_person_text) >= 0.84:
            score += 0.20
        elif persona_gpt in medium_person_text or text_similarity(persona_gpt, medium_person_text) >= 0.84:
            score += 0.10
        elif persona_gpt in generic_text or text_similarity(persona_gpt, generic_text) >= 0.88:
            score += 0.06
        elif persona_gpt in file_name_norm or text_similarity(persona_gpt, file_name_norm) >= 0.90:
            score += 0.03

    # Penalità se la query chiede una persona ma il documento non la contiene davvero
    if person_tokens and not persona_match:
        score -= 0.45

    # Penalità extra se compare solo nel filename
    if filename_only_match:
        score -= 0.15

    # -------------------------
    # MATCH TIPO DOCUMENTO
    # -------------------------
    tipo_match = False

    if tipo_doc_gpt:
        if document_type_matches(tipo_doc_gpt, tipo_documento):
            tipo_match = True
            score += 0.22
        elif is_generic_document_type(tipo_documento):
            # tipo poco utile: nessun bonus, nessun malus
            pass
        else:
            # tipo chiaramente incompatibile
            score -= 0.25

    # -------------------------
    # MATCH TESTUALE GENERALE
    # -------------------------
    text_match = False

    if query_norm and query_norm in searchable_text:
        text_match = True
        score += 0.12

    # Match keyword sparsi nel testo
    query_tokens = tokenize_text(query_norm)
    matched_query_tokens = sum(1 for t in query_tokens if t in searchable_text)
    query_token_coverage = (matched_query_tokens / len(query_tokens)) if query_tokens else 0.0

    if matched_query_tokens > 0:
        score += min(query_token_coverage * 0.12, 0.12)

    # -------------------------
    # MATCH NOME FILE ESPLICITO
    # -------------------------
    # Solo se GPT ha veramente identificato un nome file
    if nome_file_gpt and nome_file_gpt in file_name_norm:
        score += 0.10

    # -------------------------
    # REGOLE BLOCCANTI
    # -------------------------
    # Se c'è una persona nella query, un doc con solo filename non deve emergere
    if person_tokens and not persona_match and semantic_score < 0.35:
        score -= 0.20

    # Se non c'è alcun segnale reale
    if (
        semantic_score <= 0.0
        and not persona_match
        and not text_match
        and not tipo_match
    ):
        score -= 0.30

    score = max(0.0, score)

    return {
        "score": round(score, 4),
        "persona_match": persona_match,
        "raw_ocr_person_match": raw_ocr_match,
        "filename_only_match": filename_only_match,
        "persona_token_matches": (
            matched_person_tokens_strong
            + matched_person_tokens_medium
            + matched_person_tokens_generic
        ),
        "persona_token_matches_strong": matched_person_tokens_strong,
        "persona_token_matches_medium": matched_person_tokens_medium,
        "persona_token_matches_generic": matched_person_tokens_generic,
        "persona_token_matches_filename": matched_person_tokens_filename,
        "tipo_match": tipo_match,
        "text_match": text_match,
        "searchable_text": searchable_text,
        "persona_token_matches_chunk": matched_person_tokens_chunk,
        
    }
def map_exception_to_user_error(ex: Exception) -> dict:
    raw = str(ex).lower()

    # Timeout / login / connessione DB SQL Server
    if (
        "08001" in raw
        or "login timeout expired" in raw
        or "shared memory provider: timeout error" in raw
        or "unable to complete login process due to delay in login response" in raw
        or "sqlserver does not exist or access denied" in raw
        or "cannot open server" in raw
    ):
        return {
            "error_code": "DB_TEMPORARY_CONNECTION_ERROR",
            "user_message": "Problema temporaneo di connessione al database. Riprova tra qualche secondo.",
            "status_code": 503,
            "retryable": True
        }

    # Deadlock SQL Server
    if "deadlock" in raw or "1205" in raw:
        return {
            "error_code": "DB_DEADLOCK",
            "user_message": "Conflitto temporaneo sul database. Riprova tra qualche secondo.",
            "status_code": 409,
            "retryable": True
        }

    # Timeout generico
    if "timeout" in raw:
        return {
            "error_code": "TIMEOUT",
            "user_message": "Operazione scaduta. Riprova tra qualche secondo.",
            "status_code": 504,
            "retryable": True
        }

    # File non trovato
    if "file not found" in raw or "file non trovato" in raw:
        return {
            "error_code": "FILE_NOT_FOUND",
            "user_message": "File non trovato.",
            "status_code": 404,
            "retryable": False
        }

    # Default
    return {
        "error_code": "INTERNAL_ERROR",
        "user_message": "Si è verificato un errore temporaneo durante l'operazione.",
        "status_code": 500,
        "retryable": False
    }


def log_technical_error(context: str, ex: Exception):
    print(f"[ERROR][{context}] type={type(ex).__name__} detail={ex}")


def json_error_response(ex: Exception, context: str = "GENERIC"):
    err = map_exception_to_user_error(ex)
    log_technical_error(context, ex)

    return JSONResponse(
        status_code=err["status_code"],
        content={
            "success": False,
            "error_code": err["error_code"],
            "error": err["user_message"],
            "retryable": err["retryable"]
        }
    )
def get_semantic_label(score: float | None) -> str:
    if score is None:
        return "N/D"
    if score >= 0.80:
        return "Molto pertinente"
    if score >= 0.60:
        return "Buona corrispondenza"
    if score >= 0.45:
        return "Corrispondenza debole"
    return "Bassa pertinenza"  
    
def get_result_label(score: float | None, text_match: bool) -> str:
    if text_match and score is not None and score >= 0.45:
        return "Match testuale + semantico"
    if text_match:
        return "Match testuale"
    return get_semantic_label(score)
    
async def update_job_row(job_id: int, **values):
    from sqlalchemy import update
    from datetime import datetime

    values["updated_at"] = datetime.utcnow()

    async with async_engine.begin() as conn:
        await conn.execute(
            update(document_jobs)
            .where(document_jobs.c.id == job_id)
            .values(**values)
        )

async def is_job_cancelled(job_id: int) -> bool:
    async with async_engine.connect() as conn:
        result = await conn.execute(
            select(document_jobs.c.cancelled).where(document_jobs.c.id == job_id)
        )
        row = result.first()
        return bool(row and row[0])


async def cancel_jobs_for_document(doc_id: int | None = None, file_path: str | None = None):
    from sqlalchemy import update, or_
    from datetime import datetime

    conditions = []

    if doc_id is not None:
        conditions.append(document_jobs.c.document_id == doc_id)

    if file_path:
        conditions.append(document_jobs.c.file_path == file_path)

    if not conditions:
        return

    async with async_engine.begin() as conn:
        # 1) marca SEMPRE cancelled=1 per bloccare eventuali eventi tardivi
        await conn.execute(
            update(document_jobs)
            .where(or_(*conditions))
            .values(
                cancelled=1,
                updated_at=datetime.utcnow()
            )
        )

        # 2) solo per job non terminali aggiorna anche stato/fase
        await conn.execute(
            update(document_jobs)
            .where(
                or_(*conditions),
                document_jobs.c.status.in_(["queued", "processing"])
            )
            .values(
                status="cancelled",
                phase="cancelled",
                updated_at=datetime.utcnow()
            )
        )
        
async def delete_document_with_retry(doc_id: int, max_retries: int = 3):
   
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            async with async_engine.begin() as conn:
                doc_result = await conn.execute(
                    select(documents).where(documents.c.id == doc_id)
                )
                doc_row = doc_result.first()

                if not doc_row:
                    return None

                file_path = doc_row.file_path
                base_name = os.path.basename(file_path)

                await conn.execute(
                    delete(document_fields).where(document_fields.c.document_id == doc_id)
                )

                await conn.execute(
                    delete(document_jobs).where(
                        or_(
                            document_jobs.c.document_id == doc_id,
                            document_jobs.c.file_path == file_path
                        )
                    )
                )

                await conn.execute(
                    delete(documents).where(documents.c.id == doc_id)
                )

                return {
                    "file_path": file_path,
                    "base_name": base_name
                }

        except DBAPIError as e:
            last_error = e
            err_text = str(e).lower()

            if "deadlock" in err_text or "1205" in err_text:
                print(f"[DELETE] Deadlock su doc_id={doc_id}, tentativo {attempt}/{max_retries}")
                await asyncio.sleep(0.2 * attempt)
                continue

            raise

    raise last_error
  
async def try_acquire_job(job_id: int) -> bool:
    from sqlalchemy import update
    from datetime import datetime

    async with async_engine.begin() as conn:
        result = await conn.execute(
            update(document_jobs)
            .where(
                document_jobs.c.id == job_id,
                document_jobs.c.status == "queued",
                document_jobs.c.cancelled == 0
            )
            .values(
                status="processing",
                phase="start",
                progress=0,
                started_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
        )

    return result.rowcount == 1
        
async def process_document_job(job_id: int, file_path: str):
    import os
    import json
    from datetime import datetime
    from sqlalchemy import select
    from src.db_setup import async_engine, documents, document_fields, clients

    file_name = os.path.basename(file_path)
    doc_id = None
    word_path = None
    
    async def cleanup_cancelled_job():
        try:
            
            if doc_id is not None:
                async with async_engine.begin() as conn:
                    await conn.execute(
                        delete(document_fields).where(document_fields.c.document_id == doc_id)
                    )
                    await conn.execute(
                        delete(documents).where(documents.c.id == doc_id)
                    )

            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"[JOB] Errore eliminazione file annullato {file_path}: {e}")

            output_docx = os.path.join(OUTPUT_FOLDER, os.path.basename(file_path) + ".docx")
            if os.path.exists(output_docx):
                try:
                    os.remove(output_docx)
                except Exception as e:
                    print(f"[JOB] Errore eliminazione output annullato {output_docx}: {e}")

        except Exception as e:
            print(f"[JOB] Errore cleanup job annullato {job_id}: {e}")
            
    async def notify_progress(
        progress: float,
        message: str,
        error: bool = False,
        phase: str | None = None,
        status: str | None = None
    ):
        to_remove = []
        for ws in clients:
            try:
                await ws.send_text(json.dumps({
                    "event": "job_progress",
                    "job_id": job_id,
                    "progress": progress,
                    "message": message,
                    "file_name": file_name,
                    "phase": phase,
                    "status": status,
                    "error": error
                }))
            except:
                to_remove.append(ws)

        for ws in to_remove:
            if ws in clients:
                clients.remove(ws)

    phases = [
        ("Preparazione documento", 5, 35, 0.4),
        ("Estrazione dati (OCR)", 35, 65, 0.25),
        ("Generazione output", 65, 90, 0.2),
        ("Finalizzazione", 90, 100, 0.15)
    ]

    try:
        acquired = await try_acquire_job(job_id)
        if not acquired:
            print(f"[JOB] Job {job_id} già acquisito o annullato")
            return

        if await is_job_cancelled(job_id):
            print(f"[JOB] Job {job_id} annullato prima dell'avvio")
            await update_job_row(job_id, status="cancelled", phase="cancelled")
            await cleanup_cancelled_job()
            return

        await notify_progress(0, "Job creato", phase="start", status="processing")
        await notify_progress(5, "Job avviato", phase="start", status="processing")



        ocr_progress_stop = asyncio.Event()

        async def run_ocr_progress(start_pct: int = 35, end_pct: int = 65):
            current = start_pct
            while not ocr_progress_stop.is_set() and current < end_pct:
                if await is_job_cancelled(job_id):
                    return

                await update_job_row(
                    job_id,
                    progress=current,
                    phase="Estrazione dati (OCR)"
                )

                await notify_progress(
                    current,
                    "Estrazione dati (OCR)",
                    phase="Estrazione dati (OCR)",
                    status="processing"
                )

                current += 1
                await asyncio.sleep(0.4)

        for phase_name, start_pct, end_pct, step_delay in phases:

            if await is_job_cancelled(job_id):
                print(f"[JOB] Job {job_id} annullato durante la fase {phase_name}")
                await update_job_row(job_id, status="cancelled", phase="cancelled")
                await cleanup_cancelled_job()
                return

            if phase_name == "Estrazione dati (OCR)":
                ocr_progress_stop.clear()
                ocr_task = asyncio.create_task(run_ocr_progress(start_pct, end_pct))

                try:
                   
                    doc_id, word_path = await process_document(file_path)

                    if doc_id is not None:
                        await update_job_row(job_id, document_id=doc_id)

                finally:
                    ocr_progress_stop.set()
                    await ocr_task

                if await is_job_cancelled(job_id):
                    print(f"[JOB] Job {job_id} annullato dopo process_document")
                    await update_job_row(job_id, status="cancelled", phase="cancelled")
                    await cleanup_cancelled_job()
                    return

                await update_job_row(
                    job_id,
                    progress=end_pct,
                    phase=phase_name
                )

                await notify_progress(
                    end_pct,
                    phase_name,
                    phase=phase_name,
                    status="processing"
                )
            else:
                steps = max(int((end_pct - start_pct) / 2), 1)

                for i in range(1, steps + 1):
                    if await is_job_cancelled(job_id):
                        print(f"[JOB] Job {job_id} annullato durante step di {phase_name}")
                        await update_job_row(job_id, status="cancelled", phase="cancelled")
                        await cleanup_cancelled_job()
                        return

                    pct = start_pct + (end_pct - start_pct) * i / steps

                    await update_job_row(
                        job_id,
                        progress=int(pct),
                        phase=phase_name
                    )

                    await notify_progress(
                        round(pct, 1),
                        phase_name,
                        phase=phase_name,
                        status="processing"
                    )

                    await asyncio.sleep(step_delay / steps)

        if await is_job_cancelled(job_id):
            print(f"[JOB] Job {job_id} annullato prima della finalizzazione")
            await update_job_row(job_id, status="cancelled", phase="cancelled")
            await cleanup_cancelled_job()
            return

        if doc_id is None:
            raise Exception("Documento non elaborato correttamente")

        if await is_job_cancelled(job_id):
            print(f"[JOB] Job {job_id} annullato prima di document_ready_fast")
            await update_job_row(job_id, status="cancelled", phase="cancelled")
            await cleanup_cancelled_job()
            return

        await notify_clients({
            "event": "document_ready_fast",
            "doc": {
                "id": doc_id,
                "file_path": file_path
            }
        })
        
        await update_job_row(
            job_id,
            status="completed",
            progress=100,
            phase="completed",
            completed_at=datetime.utcnow()
        )

        if await is_job_cancelled(job_id):
            print(f"[JOB] Job {job_id} annullato prima di document_ready_full")
            await update_job_row(job_id, status="cancelled", phase="cancelled")
            await cleanup_cancelled_job()
            return

        async with async_engine.connect() as conn:
            doc_record = (
                await conn.execute(
                    select(documents).where(documents.c.id == doc_id)
                )
            ).first()

            if not doc_record:
                print(f"[JOB] Documento {doc_id} non più presente, skip notifica finale")
                return

            doc_fields = (
                await conn.execute(
                    select(document_fields).where(document_fields.c.document_id == doc_id)
                )
            ).all()

        fields_dict = {f.campo: f.valore for f in doc_fields}

        await notify_clients({
            "event": "document_ready_full",
            "doc": {
                "id": doc_record.id,
                "tipo_documento": doc_record.tipo_documento,
                "file_path": doc_record.file_path,
                "data_creazione": doc_record.data_creazione.strftime("%Y-%m-%d %H:%M:%S"),
                "campi": fields_dict
            }
        })

    except Exception as e:
        err = map_exception_to_user_error(e)
        log_technical_error(f"JOB {job_id}", e)

        await update_job_row(
            job_id,
            status="error",
            phase="error",
            error_message=err["user_message"],
            completed_at=datetime.utcnow()
        )

        await notify_progress(
            100,
            f"❌ {err['user_message']}",
            error=True,
            phase="error",
            status="error"
        )

app = FastAPI()

# -------------------------------
# Cartelle
# -------------------------------
UPLOAD_FOLDER = "src/uploads/"
OUTPUT_FOLDER = "output/"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------------------
# JOB RECOVERY
# -------------------------------

async def recover_pending_jobs():

    from sqlalchemy import select, update
    from datetime import datetime

    async with async_engine.begin() as conn:

        result = await conn.execute(
            select(
                document_jobs.c.id,
                document_jobs.c.file_name,
                document_jobs.c.file_path,
                document_jobs.c.status,
                document_jobs.c.cancelled,
                document_jobs.c.document_id
            ).where(
                document_jobs.c.status.in_(["queued", "processing"])
            )
        )

        jobs = result.mappings().all()

        for job in jobs:
            await conn.execute(
                update(document_jobs)
                .where(document_jobs.c.id == job["id"])
                .values(
                    status="queued",
                    phase="recovered",
                    progress=0,
                    error_message=None,
                    completed_at=None,
                    updated_at=datetime.utcnow()
                )
            )

    for job in jobs:

        if job.get("cancelled"):
            continue

        if job.get("document_id"):
            continue

        file_path = job.get("file_path") or os.path.join(UPLOAD_FOLDER, job["file_name"])

        if not os.path.exists(file_path):

            async with async_engine.begin() as conn:
                await conn.execute(
                    update(document_jobs)
                    .where(document_jobs.c.id == job["id"])
                    .values(
                        status="error",
                        phase="error",
                        error_message="File non trovato durante recovery",
                        updated_at=datetime.utcnow()
                    )
                )

            continue

        asyncio.create_task(process_document_job(job["id"], file_path))
        
@app.on_event("startup")
async def startup_event():
    print("Caricamento indice AI...")
    load_index()
    load_chunk_index()
    debug_chunk_store_lengths()

    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(select(documents.c.id))
            db_doc_ids = [row[0] for row in result.fetchall()]

        db_count = len(db_doc_ids)
        ai_count = get_index_count()
        chunk_count = get_chunk_index_count()
        chunk_doc_ids = get_chunk_doc_ids()

        db_doc_ids_set = set(db_doc_ids)
        missing_in_chunks = db_doc_ids_set - chunk_doc_ids

        print(f"[STARTUP] Documenti nel DB: {db_count}")
        print(f"[STARTUP] Documenti nell'indice AI: {ai_count}")
        print(f"[STARTUP] Chunk nell'indice AI: {chunk_count}")
        print(f"[STARTUP] Doc mancanti nel chunk index: {len(missing_in_chunks)}")
        print(f"[STARTUP] Chunk metadata salvati: {get_chunk_store_count()}")

        if ai_count != db_count or missing_in_chunks:
            print("[STARTUP] Indice o chunk NON allineati → rebuild")
            await rebuild_faiss_from_db()
        else:
            print("[STARTUP] Indici già allineati")

    except Exception as e:
        print(f"[STARTUP] Errore controllo indice AI: {e}")

    print("Recovery job pendenti...")
    try:
        await recover_pending_jobs()
    except Exception as e:
        print(f"[STARTUP] Errore recovery job pendenti: {e}")
@app.post("/rebuild_ai_index/")
async def rebuild_ai_index():
    try:
        await rebuild_faiss_from_db()
        return JSONResponse(
            content={
                "success": True,
                "message": "Indice AI ricostruito"
            }
        )
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": f"Errore rebuild indice AI: {str(e)}"
            }
        )



@app.post("/upload_multiple/")
async def upload_multiple(files: List[UploadFile] = File(...)):

    try:
        uploaded_files = []

        async with async_session() as session:
            for file in files:
                dest_path = os.path.join(UPLOAD_FOLDER, file.filename)

                import aiofiles
                async with aiofiles.open(dest_path, "wb") as f:
                    while True:
                        chunk = await file.read(1024 * 1024)
                        if not chunk:
                            break
                        await f.write(chunk)

                from datetime import datetime

                stmt = insert(document_jobs).values(
                    file_name=file.filename,
                    file_path=dest_path,
                    status="queued",
                    phase="queued",
                    progress=0,
                    upload_date=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                result = await session.execute(stmt)
                await session.commit()

                job_id = result.inserted_primary_key[0]
                uploaded_files.append({
                    "job_id": job_id,
                    "file_name": file.filename
                })

                await notify_clients({
                    "event": "job_queued",
                    "job_id": job_id,
                    "file_name": file.filename,
                    "progress": 0,
                    "phase": "queued",
                    "status": "queued",
                    "message": "📥 In coda..."
                })

                asyncio.create_task(process_document_job(job_id, dest_path))

        return {"success": True, "uploaded": uploaded_files}

    except Exception as e:
        return json_error_response(e, context="UPLOAD_MULTIPLE")
        
@app.get("/jobs_status/")
async def get_jobs_status():
    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(
                select(
                    document_jobs.c.id,
                    document_jobs.c.file_name,
                    document_jobs.c.file_path,
                    document_jobs.c.status,
                    document_jobs.c.phase,
                    document_jobs.c.progress,
                    document_jobs.c.started_at,
                    document_jobs.c.completed_at,
                    document_jobs.c.updated_at,
                    document_jobs.c.document_id,
                    document_jobs.c.error_message
                ).order_by(document_jobs.c.id.desc())
            )

            jobs = [dict(row._mapping) for row in result.fetchall()]

        return {"success": True, "jobs": jobs}

    except Exception as e:
        return json_error_response(e, context="JOBS_STATUS")
   
# Serviamo i file caricati per anteprime
app.mount("/uploads", StaticFiles(directory=UPLOAD_FOLDER), name="uploads")

# -------------------------------
# WebSocket realtime
# -------------------------------


@app.websocket("/ws/docs")
async def websocket_docs(ws: WebSocket):
    await ws.accept()
    clients.append(ws)
    try:
        while True:
            await ws.receive_text()  # Mantieni aperto
    except:
        clients.remove(ws)

# Funzione per notificare nuovi documenti
async def notify_clients(payload: dict):
    to_remove = []
    for ws in clients:
        try:
            await ws.send_text(json.dumps(payload))
        except:
            to_remove.append(ws)

    for ws in to_remove:
        clients.remove(ws)
        
async def rebuild_faiss_from_db():
    async with faiss_rebuild_lock:
        items = []
        chunk_items = []

        async with async_engine.connect() as conn:
            docs_result = await conn.execute(
                select(
                    documents.c.id,
                    documents.c.tipo_documento,
                    documents.c.file_path,
                    documents.c.ocr_text,
                    documents.c.ocr_pages
                )
            )
            docs_rows = docs_result.mappings().all()

            for row in docs_rows:
                doc_id = row["id"]

                fields_result = await conn.execute(
                    select(
                        document_fields.c.campo,
                        document_fields.c.valore
                    ).where(document_fields.c.document_id == doc_id)
                )
                fields_rows = fields_result.mappings().all()

                fields_dict = {f["campo"]: f["valore"] for f in fields_rows}

                doc_text_for_ai = build_text_for_ai(
                    row["tipo_documento"],
                    row["file_path"],
                    fields_dict
                )

                chunk_source_text = row.get("ocr_text") or doc_text_for_ai

                raw_ocr_pages = row.get("ocr_pages")
                parsed_page_texts = []

                if raw_ocr_pages:
                    if isinstance(raw_ocr_pages, list):
                        parsed_page_texts = [str(p) for p in raw_ocr_pages if p]
                    elif isinstance(raw_ocr_pages, str):
                        try:
                            loaded = json.loads(raw_ocr_pages)
                            if isinstance(loaded, list):
                                parsed_page_texts = [str(p) for p in loaded if p]
                        except Exception as e:
                            print(f"[AI_SEARCH] Errore parsing ocr_pages doc_id={doc_id}: {e}")
                            parsed_page_texts = []

                items.append({
                    "doc_id": doc_id,
                    "text": doc_text_for_ai
                })

                chunk_context_prefix = build_chunk_context_prefix(
                    row["tipo_documento"],
                    row["file_path"],
                    fields_dict
                )

                chunk_items.append({
                    "doc_id": doc_id,
                    "text": chunk_source_text,
                    "page_texts": parsed_page_texts,
                    "context_prefix": chunk_context_prefix
                })

        if items:
            await asyncio.to_thread(rebuild_index_from_items, items)
        else:
            await asyncio.to_thread(reset_index)

        await asyncio.to_thread(reset_chunk_index)

        for item in chunk_items:
            page_texts = item.get("page_texts") or []
            context_prefix = item.get("context_prefix", "")

            if isinstance(page_texts, list) and len(page_texts) > 0:
                await asyncio.to_thread(
                    add_document_page_chunks,
                    item["doc_id"],
                    page_texts,
                    context_prefix
                )
            else:
                await asyncio.to_thread(
                    add_document_chunks,
                    item["doc_id"],
                    item["text"],
                    context_prefix
                )

        print(f"[AI_SEARCH] Indice ricostruito: {len(items)} documenti")
        print(f"[AI_SEARCH] Chunk index ricostruito: {len(chunk_items)} documenti")
# -------------------------------
# HTML principale
# -------------------------------



@app.get("/", response_class=HTMLResponse)
async def home_page(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})



# -------------------------------
# Lista documenti
# -------------------------------
@app.get("/documents/")
async def list_documents():
    async with async_engine.connect() as conn:
        result = await conn.execute(select(documents))
        rows = result.fetchall()

        doc_ids = [r.id for r in rows]
        fields_map = {}

        if doc_ids:
            fields_result = await conn.execute(
                select(
                    document_fields.c.document_id,
                    document_fields.c.campo,
                    document_fields.c.valore
                ).where(document_fields.c.document_id.in_(doc_ids))
            )

            for f in fields_result.fetchall():
                fields_map.setdefault(f.document_id, {})[f.campo] = f.valore

        docs = []
        for r in rows:
            docs.append({
                "id": r.id,
                "tipo_documento": r.tipo_documento,
                "file_path": r.file_path,
                "data_creazione": r.data_creazione.strftime("%Y-%m-%d %H:%M:%S"),
                "campi": fields_map.get(r.id, {})
            })

    return JSONResponse(
        content=docs,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )
@app.get("/documents/{doc_id}")
async def get_document(doc_id: int):
    async with async_engine.connect() as conn:
        result = await conn.execute(
            select(documents).where(documents.c.id == doc_id)
        )
        r = result.first()

        if not r:
            return JSONResponse(
                status_code=404,
                content={"success": False, "error": "Documento non trovato"}
            )

        fields_result = await conn.execute(
            select(document_fields).where(
                document_fields.c.document_id == doc_id
            )
        )

        fields = {
            f.campo: f.valore
            for f in fields_result.fetchall()
        }

        doc = {
            "id": r.id,
            "tipo_documento": r.tipo_documento,
            "file_path": r.file_path,
            "data_creazione": r.data_creazione.strftime("%Y-%m-%d %H:%M:%S"),
            "campi": fields
        }

    return JSONResponse(
        content=doc,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )
# -------------------------------
# Statistiche per grafici
# -------------------------------
@app.get("/stats/")
async def stats():
    async with async_engine.connect() as conn:
        result = await conn.execute(select(documents))
        rows = result.fetchall()

    tipo_count = {}
    date_count = {}

    for r in rows:
        tipo_count[r.tipo_documento] = tipo_count.get(r.tipo_documento, 0) + 1
        # elimina categorie con pochi elementi
        tipo_count = {
            k: v for k, v in tipo_count.items()
            if v >= 1   # soglia (puoi cambiare)
        }
        day_str = r.data_creazione.strftime("%Y-%m-%d")
        date_count[day_str] = date_count.get(day_str, 0) + 1

    if rows:
        last_upload = max(rows, key=lambda r: r.data_creazione).data_creazione.strftime("%d/%m/%Y, %H:%M:%S")
    else:
        last_upload = "-"

    return JSONResponse(
        content={
            "tipo_count": tipo_count,
            "date_count": date_count,
            "last_upload": last_upload
        },
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )


@app.delete("/documents/{doc_id}")
async def delete_document(doc_id: int):
    try:
        print(f"[DELETE] Richiesta eliminazione doc_id={doc_id}")

        async with async_engine.connect() as conn:
            doc_result = await conn.execute(
                select(documents).where(documents.c.id == doc_id)
            )
            doc_row = doc_result.first()

        if not doc_row:
            return JSONResponse(
                status_code=404,
                content={
                    "success": False,
                    "error": "Documento non trovato"
                }
            )

        file_path = doc_row.file_path
        base_name = os.path.basename(file_path)

        await cancel_jobs_for_document(doc_id=doc_id, file_path=file_path)
        print(f"[DELETE] Job collegati al documento {doc_id} marcati come cancelled")

        deleted_info = await delete_document_with_retry(doc_id)

        if not deleted_info:
            return JSONResponse(
                status_code=404,
                content={"success": False, "error": "Documento non trovato"}
            )

        print(f"[DELETE] Eliminazione DB completata doc_id={doc_id}")

        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"[DELETE] Errore eliminazione file originale {file_path}: {e}")

        output_docx = os.path.join(OUTPUT_FOLDER, base_name + ".docx")
        if os.path.exists(output_docx):
            try:
                os.remove(output_docx)
            except Exception as e:
                print(f"[DELETE] Errore eliminazione output {output_docx}: {e}")

        # notifica solo informativa
        asyncio.create_task(notify_clients({
            "event": "document_deleted",
            "doc_id": doc_id,
            "file_name": base_name
        }))

        return JSONResponse(
            content={
                "success": True,
                "doc_id": doc_id,
                "file_name": base_name
            },
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )

    except DBAPIError as e:
        err_text = str(e).lower()
        if "deadlock" in err_text or "1205" in err_text:
            return JSONResponse(
                status_code=409,
                content={
                    "success": False,
                    "error": "Conflitto temporaneo sul database durante l'eliminazione. Riprova."
                }
            )

        print(f"[DELETE] Errore DB eliminazione documento {doc_id}: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "Errore database durante l'eliminazione"
            }
        )

    except Exception as e:
        print(f"[DELETE] Errore generico eliminazione documento {doc_id}: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "Errore interno durante l'eliminazione"
            }
        )
# -------------------------------
# Download multiplo ZIP
# -------------------------------
@app.get("/download_zip/")
def download_zip(filenames: list[str] = Query(...)):
    memory_file = BytesIO()
    with zipfile.ZipFile(memory_file, 'w') as zf:
        for fname in filenames:
            path = os.path.join(OUTPUT_FOLDER, fname + ".docx")
            if os.path.exists(path):
                zf.write(path, arcname=fname + ".docx")
    memory_file.seek(0)
    return StreamingResponse(memory_file, media_type='application/zip', headers={
        "Content-Disposition": "attachment; filename=documenti_selezionati.zip"
    })
    
@app.get("/download/{filename}")
def download_word(filename: str):
    docx_path = os.path.join(OUTPUT_FOLDER, filename + ".docx")

    if not os.path.exists(docx_path):
        return JSONResponse(
            status_code=404,
            content={"success": False, "error": "File Word non trovato"}
        )

    return FileResponse(
        path=docx_path,
        filename=os.path.basename(docx_path),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )   
# -------------------------------
# AI Search nei documenti
# -------------------------------
# -------------------------------
# AI Search nei documenti
# -------------------------------



@app.get("/search_ai/")
async def search_ai(request: Request):
    query = request.query_params.get("query", "").strip()
    if not query:
        return JSONResponse({"results": []})

    # Interpreta la query
    filters = interpret_search_query(query)
    tipo_doc = filters.get("tipo_documento")
    campo = filters.get("campo")
    valore = filters.get("valore")
    operatore = filters.get("operatore", None)

    async with async_engine.connect() as conn:
        stmt = select(
            documents.c.id, 
            documents.c.tipo_documento, 
            documents.c.file_path, 
            documents.c.data_creazione
        )

        if campo and valore is not None:
            stmt = stmt.join(
                document_fields,
                documents.c.id == document_fields.c.document_id
            )

            stmt = stmt.where(document_fields.c.campo.ilike(campo))

            numero = None
            try:
                numero = float(str(valore).replace(",", ".").replace("h", "").strip())
            except ValueError:
                numero = None

            if operatore in [">", "<", ">=", "<="] and numero is not None:
                stmt = stmt.where(text("TRY_CAST(document_fields.valore AS FLOAT) IS NOT NULL"))

                if operatore == ">":
                    stmt = stmt.where(text("TRY_CAST(document_fields.valore AS FLOAT) > :num")).params(num=numero)
                elif operatore == "<":
                    stmt = stmt.where(text("TRY_CAST(document_fields.valore AS FLOAT) < :num")).params(num=numero)
                elif operatore == ">=":
                    stmt = stmt.where(text("TRY_CAST(document_fields.valore AS FLOAT) >= :num")).params(num=numero)
                elif operatore == "<=":
                    stmt = stmt.where(text("TRY_CAST(document_fields.valore AS FLOAT) <= :num")).params(num=numero)
            else:
                stmt = stmt.where(document_fields.c.valore.ilike(f"%{valore}%"))

        if tipo_doc:
            stmt = stmt.where(documents.c.tipo_documento.ilike(f"%{tipo_doc}%"))

        results = await conn.execute(stmt)
        rows = results.mappings().all()

        final_results = []
        for row in rows:
            doc_id = row["id"]
            fields_stmt = select(document_fields.c.campo, document_fields.c.valore).where(document_fields.c.document_id == doc_id)
            fields_rows = await conn.execute(fields_stmt)
            fields = {f["campo"]: f["valore"] for f in fields_rows.mappings().all()}

            final_results.append({
                "id": doc_id,
                "tipo_documento": row["tipo_documento"],
                "file_path": row["file_path"],
                "data_creazione": row["data_creazione"].isoformat(),
                "campi": fields
            })

    return JSONResponse({"results": final_results})
    
@app.get("/semantic_search/")
async def semantic_search(query: str = Query(..., description="Query per ricerca semantica AI")):
    semantic_hits = await asyncio.to_thread(search, query, top_k=50)

    if not semantic_hits:
        return {"query": query, "results": []}

    semantic_doc_ids = [hit["doc_id"] for hit in semantic_hits]

    async with async_engine.connect() as conn:
        docs_stmt = select(
            documents.c.id,
            documents.c.tipo_documento,
            documents.c.file_path,
            documents.c.data_creazione
        ).where(documents.c.id.in_(semantic_doc_ids))

        docs_rows = (await conn.execute(docs_stmt)).mappings().all()
        docs_map = {row["id"]: row for row in docs_rows}

        final_results = []

        for hit in semantic_hits:
            doc_id = hit["doc_id"]
            row = docs_map.get(doc_id)

            if not row:
                continue

            fields_stmt = select(
                document_fields.c.campo,
                document_fields.c.valore
            ).where(document_fields.c.document_id == doc_id)

            fields_rows = (await conn.execute(fields_stmt)).mappings().all()
            fields = {f["campo"]: f["valore"] for f in fields_rows}

            score = hit.get("score")
            file_name = (row["file_path"] or "").lower()

            if query.lower() in file_name:
                score = min(1.0, (score or 0) + 0.3)

            final_results.append({
                "id": doc_id,
                "tipo_documento": row["tipo_documento"],
                "file_path": row["file_path"],
                "data_creazione": row["data_creazione"].strftime("%Y-%m-%d %H:%M:%S"),
                "campi": fields,
                "score": score,
                "score_label": get_semantic_label(score),
                "score_percent": round(score * 100, 1) if score is not None else None
            })

    final_results = [r for r in final_results if (r.get("score") or 0) >= 0.65]
    final_results.sort(key=lambda x: (x.get("score") or 0), reverse=True)
    final_results = final_results[:100]

    return {
        "query": query,
        "results": final_results
    }

@app.get("/search_documents/")
async def search_documents(request: Request, debug: bool = False):

    try:
        from src.query_understanding_gpt import parse_search_query_with_gpt

        query = request.query_params.get("query", "").strip()
        parsed_query = await asyncio.to_thread(parse_search_query_with_gpt, query)
        
        tipo_doc_gpt = parsed_query.get("tipo_documento")
        persona_gpt = parsed_query.get("persona")
        nome_file_gpt = parsed_query.get("nome_file")
        keywords_gpt = parsed_query.get("keywords", [])
        data_da_gpt = parsed_query.get("data_da")
        data_a_gpt = parsed_query.get("data_a")
       
        raw_ocr_match_ids = []

        if persona_gpt:
            async with async_engine.connect() as conn_debug:
                raw_stmt = select(
                    documents.c.id,
                    documents.c.tipo_documento,
                    documents.c.ocr_text,
                    documents.c.ocr_pages
                )

                tipo_doc_norm = normalize_text(tipo_doc_gpt or "")
                if tipo_doc_norm in {"e ticket", "e-ticket", "ticket"}:
                    raw_stmt = raw_stmt.where(
                        or_(
                            documents.c.tipo_documento.ilike("%ticket%"),
                            documents.c.tipo_documento.ilike("%biglietto%"),
                            documents.c.tipo_documento.ilike("%biglietto aereo%"),
                            documents.c.tipo_documento.ilike("%biglietto traghetto%")
                        )
                    )
                elif tipo_doc_gpt:
                    raw_stmt = raw_stmt.where(
                        documents.c.tipo_documento.ilike(f"%{tipo_doc_gpt}%")
                    )

                raw_rows = (await conn_debug.execute(raw_stmt)).mappings().all()

                for rr in raw_rows:
                    raw_text = str(rr.get("ocr_text") or "")
                    raw_pages = rr.get("ocr_pages") or ""

                    if raw_pages:
                        if isinstance(raw_pages, str):
                            raw_text += " " + raw_pages
                        else:
                            raw_text += " " + str(raw_pages)

                    if raw_ocr_person_match(persona_gpt, raw_text):
                        raw_ocr_match_ids.append(rr["id"])

        print("[RAW_OCR_MATCH_IDS]", raw_ocr_match_ids)
        print("[RAW_OCR_MATCH_COUNT]", len(raw_ocr_match_ids))

 

        print("\n================ SEARCH DEBUG START ================")
        print("[QUERY]", query)
        print("[SEARCH GPT PARSED]", parsed_query)

        if not query:
            return JSONResponse({"success": True, "mode": "empty", "results": []})
            
        async with async_engine.connect() as conn:
            # =========================
            # 1. TENTATIVO RICERCA STRUTTURATA
            # =========================
            filters = interpret_search_query(query)
            tipo_doc = filters.get("tipo_documento")
            campo = filters.get("campo")
            valore = filters.get("valore")
            operatore = filters.get("operatore", None)

            stmt = select(
                documents.c.id,
                documents.c.tipo_documento,
                documents.c.file_path,
                documents.c.data_creazione
            )

            if campo and valore is not None:
                stmt = stmt.join(
                    document_fields,
                    documents.c.id == document_fields.c.document_id
                )

                stmt = stmt.where(document_fields.c.campo.ilike(campo))

                numero = None
                try:
                    numero = float(str(valore).replace(",", ".").replace("h", "").strip())
                except ValueError:
                    numero = None

                if operatore in [">", "<", ">=", "<="] and numero is not None:
                    stmt = stmt.where(text("TRY_CAST(document_fields.valore AS FLOAT) IS NOT NULL"))

                    if operatore == ">":
                        stmt = stmt.where(text("TRY_CAST(document_fields.valore AS FLOAT) > :num")).params(num=numero)
                    elif operatore == "<":
                        stmt = stmt.where(text("TRY_CAST(document_fields.valore AS FLOAT) < :num")).params(num=numero)
                    elif operatore == ">=":
                        stmt = stmt.where(text("TRY_CAST(document_fields.valore AS FLOAT) >= :num")).params(num=numero)
                    elif operatore == "<=":
                        stmt = stmt.where(text("TRY_CAST(document_fields.valore AS FLOAT) <= :num")).params(num=numero)
                else:
                    stmt = stmt.where(document_fields.c.valore.ilike(f"%{valore}%"))

            if tipo_doc:
                stmt = stmt.where(documents.c.tipo_documento.ilike(f"%{tipo_doc}%"))

            structured_rows = (await conn.execute(stmt)).mappings().all()

            if structured_rows:
                if not persona_gpt:
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
                        })

                    deduped_results = []
                    seen_ids = set()

                    for r in final_results:
                        current_doc_id = r.get("id")
                        if current_doc_id in seen_ids:
                            continue
                        seen_ids.add(current_doc_id)
                        deduped_results.append(r)

                    return JSONResponse({
                        "success": True,
                        "mode": "structured",
                        "results": deduped_results
                    })         
  
            # =========================
            # 2. FALLBACK IBRIDO: TESTUALE + SEMANTICO
            # =========================
         

            keywords = [k.strip().lower() for k in keywords_gpt if len(str(k).strip()) > 1]

            if persona_gpt and persona_gpt.lower() not in keywords:
                keywords.append(persona_gpt.lower())

            if tipo_doc_gpt and tipo_doc_gpt.lower() not in keywords:
                keywords.append(tipo_doc_gpt.lower())

            if nome_file_gpt and nome_file_gpt.lower() not in keywords:
                keywords.append(nome_file_gpt.lower())

            if not keywords:
                keywords = [k.strip().lower() for k in query.split() if len(k.strip()) > 2]

            text_doc_ids = set()

            semantic_hits_by_id = {}
            semantic_doc_ids = set()
            raw_ocr_doc_ids = set()
            raw_ocr_first_page_by_doc = {}
            best_chunk_per_doc = {}

            if persona_gpt:
                raw_candidate_stmt = select(
                    documents.c.id,
                    documents.c.tipo_documento,
                    documents.c.file_path,
                    documents.c.ocr_text,
                    documents.c.ocr_pages
                )

                if tipo_doc_gpt:
                    tipo_doc_norm = normalize_text(tipo_doc_gpt)

                    if tipo_doc_norm in {"e ticket", "e-ticket", "ticket"}:
                        raw_candidate_stmt = raw_candidate_stmt.where(
                            or_(
                                documents.c.tipo_documento.ilike("%ticket%"),
                                documents.c.tipo_documento.ilike("%biglietto%"),
                                documents.c.tipo_documento.ilike("%biglietto aereo%"),
                                documents.c.tipo_documento.ilike("%biglietto traghetto%")
                            )
                        )
                    else:
                        raw_candidate_stmt = raw_candidate_stmt.where(
                            documents.c.tipo_documento.ilike(f"%{tipo_doc_gpt}%")
                        )

                raw_candidate_rows = (await conn.execute(raw_candidate_stmt)).mappings().all()

                for raw_row in raw_candidate_rows:
                    raw_text = str(raw_row.get("ocr_text") or "")
                    raw_pages = raw_row.get("ocr_pages")
                    doc_id = raw_row["id"]

                    page_match_found = False
                    parsed_pages = []

                    if raw_pages:
                        if isinstance(raw_pages, list):
                            parsed_pages = raw_pages
                        elif isinstance(raw_pages, str):
                            try:
                                loaded_pages = json.loads(raw_pages)
                                if isinstance(loaded_pages, list):
                                    parsed_pages = loaded_pages
                                else:
                                    parsed_pages = [raw_pages]
                            except Exception:
                                parsed_pages = [raw_pages]

                    for page_idx, page_text in enumerate(parsed_pages, start=1):
                        page_text_str = str(page_text or "")
                        raw_text += " " + page_text_str

                        if not page_match_found and raw_ocr_person_match(persona_gpt, page_text_str):
                            raw_ocr_doc_ids.add(doc_id)
                            raw_ocr_first_page_by_doc[doc_id] = page_idx
                            page_match_found = True

                    if not page_match_found and raw_ocr_person_match(persona_gpt, raw_text):
                        raw_ocr_doc_ids.add(doc_id)

                print("[RAW_OCR_DOC_IDS SAMPLE]", sorted(list(raw_ocr_doc_ids))[:20])
                print("[RAW_OCR_DOC_IDS COUNT]", len(raw_ocr_doc_ids))

            if keywords:
                text_stmt = select(documents.c.id).select_from(
                    documents.join(
                        document_fields,
                        documents.c.id == document_fields.c.document_id
                    )
                )

                general_conditions = []
                persona_conditions = []
                persona_tokens = []

                for kw in keywords:
                    general_conditions.append(document_fields.c.valore.ilike(f"%{kw}%"))
                    general_conditions.append(document_fields.c.campo.ilike(f"%{kw}%"))
                    general_conditions.append(documents.c.tipo_documento.ilike(f"%{kw}%"))

                if tipo_doc_gpt:
                    general_conditions.append(documents.c.tipo_documento.ilike(f"%{tipo_doc_gpt}%"))

                if nome_file_gpt:
                    general_conditions.append(documents.c.file_path.ilike(f"%{nome_file_gpt}%"))

                if persona_gpt:
                    persona_tokens = tokenize_text(persona_gpt)

                for token in persona_tokens:
                    persona_conditions.append(document_fields.c.valore.ilike(f"%{token}%"))

                if general_conditions:
                    text_stmt = text_stmt.where(or_(*general_conditions))

                if persona_conditions:
                    text_stmt = text_stmt.where(or_(*persona_conditions))

                text_rows = (await conn.execute(text_stmt)).mappings().all()
                text_doc_ids = {row["id"] for row in text_rows}

                print("[TEXT_DOC_IDS SAMPLE]", sorted(list(text_doc_ids))[:20])
                print("[TEXT_DOC_IDS COUNT]", len(text_doc_ids))

                semantic_top_k = 50
                chunk_top_k = 80

                if persona_gpt:
                    semantic_top_k = 20
                    chunk_top_k = 120

                semantic_hits = await asyncio.to_thread(search, query, top_k=semantic_top_k)
                chunk_hits = await asyncio.to_thread(search_chunks, query, top_k=chunk_top_k)

                for ch in chunk_hits:
                    doc_id = ch["doc_id"]
                    current = best_chunk_per_doc.get(doc_id)

                    if current is None or (ch.get("score") or 0) > (current.get("score") or 0):
                        best_chunk_per_doc[doc_id] = ch

                semantic_hits_by_id = {}

                for hit in semantic_hits:
                    doc_id = hit["doc_id"]
                    current = semantic_hits_by_id.get(doc_id)
                    if current is None or (hit.get("score") or 0) > (current.get("score") or 0):
                        semantic_hits_by_id[doc_id] = {
                            "doc_id": doc_id,
                            "score": hit.get("score") or 0.0,
                            "source": "doc",
                            "chunk_text": None,
                            "chunk_index": None
                        }

                for hit in chunk_hits:
                    doc_id = hit["doc_id"]
                    chunk_score = hit.get("score") or 0.0

                    current = semantic_hits_by_id.get(doc_id)
                    if current is None or chunk_score > (current.get("score") or 0):
                        semantic_hits_by_id[doc_id] = {
                            "doc_id": doc_id,
                            "score": chunk_score,
                            "source": "chunk",
                            "chunk_text": hit.get("text"),
                            "chunk_index": hit.get("chunk_index"),
                            "page_number": hit.get("page_number")
                        }

                semantic_doc_ids = set(semantic_hits_by_id.keys())

                print("[CHUNK_HITS COUNT]", len(chunk_hits))
                print("[SEMANTIC_DOC_IDS COUNT AFTER CHUNKS]", len(semantic_doc_ids))


            all_doc_ids = list(text_doc_ids | semantic_doc_ids | raw_ocr_doc_ids)

            print("[SEMANTIC_DOC_IDS SAMPLE]", sorted(list(semantic_doc_ids))[:20])
            print("[SEMANTIC_DOC_IDS COUNT]", len(semantic_doc_ids))

            print("[ALL_DOC_IDS SAMPLE]", sorted(list(all_doc_ids))[:20])
            print("[ALL_DOC_IDS COUNT]", len(all_doc_ids))

            if not all_doc_ids:
                return JSONResponse({
                    "success": True,
                    "mode": "semantic",
                    "results": []
                })

            docs_stmt = select(
                documents.c.id,
                documents.c.tipo_documento,
                documents.c.file_path,
                documents.c.data_creazione
            ).where(documents.c.id.in_(all_doc_ids))

            if tipo_doc_gpt:
                tipo_doc_norm = normalize_text(tipo_doc_gpt)

                if tipo_doc_norm in {"e ticket", "e-ticket", "ticket"}:
                    docs_stmt = docs_stmt.where(
                        or_(
                            documents.c.tipo_documento.ilike("%ticket%"),
                            documents.c.tipo_documento.ilike("%biglietto%"),
                            documents.c.tipo_documento.ilike("%biglietto aereo%")
                        )
                    )
                else:
                    docs_stmt = docs_stmt.where(documents.c.tipo_documento.ilike(f"%{tipo_doc_gpt}%"))

            if nome_file_gpt:
                docs_stmt = docs_stmt.where(documents.c.file_path.ilike(f"%{nome_file_gpt}%"))

            if data_da_gpt:
                docs_stmt = docs_stmt.where(documents.c.data_creazione >= data_da_gpt)

            if data_a_gpt:
                docs_stmt = docs_stmt.where(documents.c.data_creazione <= data_a_gpt)

            doc_rows = (await conn.execute(docs_stmt)).mappings().all()
            docs_map = {row["id"]: row for row in doc_rows}
            
            print("[DOC_ROWS AFTER SQL FILTER SAMPLE]", [r["id"] for r in doc_rows][:20])
            print("[DOC_ROWS AFTER SQL FILTER COUNT]", len(doc_rows))

            # Fallback fuzzy recall persona generalizzato
            if persona_gpt:
                person_tokens = tokenize_text(persona_gpt)

                fuzzy_stmt = select(
                    documents.c.id,
                    documents.c.tipo_documento,
                    documents.c.file_path,
                    documents.c.data_creazione
                )

                if tipo_doc_gpt:
                    tipo_doc_norm = normalize_text(tipo_doc_gpt)

                    if tipo_doc_norm in {"e ticket", "e-ticket", "ticket"}:
                        fuzzy_stmt = fuzzy_stmt.where(
                            or_(
                                documents.c.tipo_documento.ilike("%ticket%"),
                                documents.c.tipo_documento.ilike("%biglietto%"),
                                documents.c.tipo_documento.ilike("%biglietto aereo%"),
                                documents.c.tipo_documento.ilike("%biglietto traghetto%")
                            )
                        )
                    else:
                        fuzzy_stmt = fuzzy_stmt.where(
                            documents.c.tipo_documento.ilike(f"%{tipo_doc_gpt}%")
                        )

                fuzzy_doc_rows = (await conn.execute(fuzzy_stmt)).mappings().all()

                fuzzy_added = 0

                for fuzzy_row in fuzzy_doc_rows:
                    fuzzy_doc_id = fuzzy_row["id"]

                    if fuzzy_doc_id in docs_map:
                        continue

                    fields_stmt = select(
                        document_fields.c.campo,
                        document_fields.c.valore
                    ).where(document_fields.c.document_id == fuzzy_doc_id)

                    fields_rows = (await conn.execute(fields_stmt)).mappings().all()
                    fields = {f["campo"]: f["valore"] for f in fields_rows}

                    if has_fuzzy_person_match(fields, person_tokens):
                        docs_map[fuzzy_doc_id] = fuzzy_row
                        if fuzzy_doc_id not in all_doc_ids:
                            all_doc_ids.append(fuzzy_doc_id)
                        fuzzy_added += 1

                print("[FUZZY_RECALL_ADDED]", fuzzy_added)
                print("[ALL_DOC_IDS_AFTER_FUZZY]", len(all_doc_ids))
                
            final_results = []

            skipped_missing_row = 0
            skipped_not_ticket = 0
            processed_docs = 0

            for doc_id in all_doc_ids:
                row = docs_map.get(doc_id)
                if not row:
                    skipped_missing_row += 1
                    continue

                fields_stmt = select(
                    document_fields.c.campo,
                    document_fields.c.valore
                ).where(document_fields.c.document_id == doc_id)

                fields_rows = (await conn.execute(fields_stmt)).mappings().all()
                fields = {f["campo"]: f["valore"] for f in fields_rows}
                
                ocr_stmt = select(
                    documents.c.ocr_text,
                    documents.c.ocr_pages
                ).where(documents.c.id == doc_id)

                ocr_row = (await conn.execute(ocr_stmt)).mappings().first()

                raw_ocr_text = ""
                if ocr_row:
                    ocr_text_val = ocr_row.get("ocr_text") or ""
                    ocr_pages_val = ocr_row.get("ocr_pages") or ""

                    raw_ocr_text = str(ocr_text_val)

                    if ocr_pages_val:
                        if isinstance(ocr_pages_val, str):
                            raw_ocr_text += " " + ocr_pages_val
                        else:
                            raw_ocr_text += " " + str(ocr_pages_val)
            

                hit = semantic_hits_by_id.get(doc_id)
                best_chunk_hit = best_chunk_per_doc.get(doc_id)

                semantic_score = hit.get("score") if hit else 0.0

                best_chunk_text = None
                best_page_number = None
                best_chunk_index = None

                if hit and hit.get("source") == "chunk":
                    best_chunk_text = hit.get("chunk_text")
                    best_page_number = hit.get("page_number")
                    best_chunk_index = hit.get("chunk_index")
                elif best_chunk_hit:
                    best_chunk_text = best_chunk_hit.get("text")
                    best_page_number = best_chunk_hit.get("page_number")
                    best_chunk_index = best_chunk_hit.get("chunk_index")

                persona_page_number = raw_ocr_first_page_by_doc.get(doc_id)

                if persona_page_number is not None:
                    best_page_number = persona_page_number

                score_info = compute_document_score(
                    query=query,
                    parsed_query=parsed_query,
                    row=row,
                    fields=fields,
                    semantic_score=semantic_score,
                    best_chunk_text=best_chunk_text,
                    raw_ocr_text=raw_ocr_text
                )
                raw_ocr_hit = doc_id in raw_ocr_doc_ids

                if raw_ocr_hit and not score_info["persona_match"]:
                    score_info["persona_match"] = True
                    score_info["persona_token_matches_generic"] = max(
                        score_info.get("persona_token_matches_generic", 0), 1
                    )
                    score_info["persona_token_matches"] = (
                        (score_info.get("persona_token_matches_strong", 0) or 0)
                        + (score_info.get("persona_token_matches_medium", 0) or 0)
                        + (score_info.get("persona_token_matches_generic", 0) or 0)
                    ) 
                if raw_ocr_hit:
                    print("[RAW_OCR_PERSON_MATCH]", doc_id)                    

                preview_source_text = best_chunk_text or score_info.get("searchable_text") or ""
                best_chunk_preview = build_match_excerpt(preview_source_text, query, window=120)
                best_chunk_preview_html = highlight_query_terms(best_chunk_preview, query)

                print(
                    "[CHUNK UI DEBUG]",
                    "doc_id=", doc_id,
                    "best_chunk_text=", (best_chunk_text[:120] if best_chunk_text else None),
                    "best_chunk_page=", best_page_number,
                    "best_chunk_index=", best_chunk_index,
                    "best_chunk_preview=", best_chunk_preview,
                    "best_chunk_preview_html=", best_chunk_preview_html
                )

                if persona_gpt:
                    if (
                        not score_info["persona_match"]
                        and score_info.get("persona_token_matches_filename", 0) > 0
                        and (semantic_score or 0) < 0.40
                    ):
                        continue

                boosted_score = score_info["score"]
                
                if hit and hit.get("source") == "chunk":
                    boosted_score += 0.05
                boosted_score = min(boosted_score, 1.0)
    
                tipo_norm = normalize_text(row["tipo_documento"] or "")
                query_norm = normalize_text(query)

                is_ticket_query = any(x in query_norm for x in ["bigliett", "ticket", "volo", "voli"])
                is_ticket_doc = any(x in tipo_norm for x in ["ticket", "biglietto", "biglietto aereo", "biglietto traghetto"])

                if is_ticket_query and not is_ticket_doc:
                    skipped_not_ticket += 1
                    continue

                print(
                    "[SEARCH SCORE]",
                    "doc_id=", doc_id,
                    "boosted_score=", boosted_score,
                    "semantic_score=", semantic_score,
                    "persona_match=", score_info["persona_match"],
                    "filename_only_match=", score_info.get("filename_only_match"),
                    "persona_token_matches=", score_info["persona_token_matches"],
                    "persona_token_matches_filename=", score_info.get("persona_token_matches_filename", 0),
                    "tipo_match=", score_info["tipo_match"],
                    "text_match=", score_info["text_match"],
                    "tipo_documento=", row["tipo_documento"],
                    "file=", row["file_path"]
                )

                processed_docs += 1

                final_results.append({
                    "id": doc_id,
                    "tipo_documento": row["tipo_documento"],
                    "file_path": row["file_path"],
                    "data_creazione": row["data_creazione"].strftime("%Y-%m-%d %H:%M:%S"),
                    "campi": fields,
                    "score": semantic_score,
                    "boosted_score": boosted_score,
                    "score_label": get_result_label(min(boosted_score, 1.0), score_info["text_match"]),
                    "score_percent": min(round((boosted_score or 0) * 100, 1), 100),
                    "text_match": score_info["text_match"],
                    "persona_match": score_info["persona_match"],
                    "raw_ocr_person_match": score_info.get("raw_ocr_person_match", False),
                    "tipo_match": score_info["tipo_match"],
                    "persona_token_matches": score_info["persona_token_matches"],
                    "persona_token_matches_strong": score_info["persona_token_matches_strong"],
                    "persona_token_matches_medium": score_info["persona_token_matches_medium"],
                    "persona_token_matches_generic": score_info["persona_token_matches_generic"],
                    "persona_token_matches_chunk": score_info["persona_token_matches_chunk"],
                    "persona_token_matches_filename": score_info["persona_token_matches_filename"],
                    "match_type": (
                    "hybrid_chunk" if (doc_id in text_doc_ids and best_chunk_text)
                    else "hybrid_doc" if (doc_id in text_doc_ids and hit and hit.get("source") == "doc")
                    else "text" if doc_id in text_doc_ids
                    else "semantic_chunk" if best_chunk_text
                    else "semantic_doc"
                ),
                    "semantic_source": hit.get("source") if hit else None,
                    "best_chunk_text": best_chunk_text,
                    "best_chunk_preview": best_chunk_preview,
                    "best_chunk_preview_html": best_chunk_preview_html,
                    "best_chunk_index": best_chunk_index,
                    "best_chunk_page": best_page_number,
                    "persona_page": raw_ocr_first_page_by_doc.get(doc_id),
                })

            final_results.sort(
                key=lambda r: (
                    1 if r.get("persona_match") else 0,
                    r.get("persona_token_matches") or 0,
                    1 if r.get("text_match") else 0,
                    1 if r.get("tipo_match") else 0,
                    r.get("boosted_score") or 0,
                    r.get("score") or 0
                ),
                reverse=True
            )   
            print("[SKIPPED_MISSING_ROW]", skipped_missing_row)
            print("[SKIPPED_NOT_TICKET]", skipped_not_ticket)
            print("[PROCESSED_DOCS]", processed_docs)
            print("[FINAL_RESULTS_BEFORE_THRESHOLD]", len(final_results))
            
            print("[PERSON FILTER] Persona richiesta:", persona_gpt)
            print("[PERSON FILTER] Prima filtro:", len(final_results))
            for r in final_results[:10]:
                print(
                    "[PERSON FILTER CHECK]",
                    "id=", r.get("id"),
                    "persona_match=", r.get("persona_match"),
                    "strong=", r.get("persona_token_matches_strong"),
                    "medium=", r.get("persona_token_matches_medium"),
                    "generic=", r.get("persona_token_matches_generic"),
                    "chunk=", r.get("persona_token_matches_chunk"),
                    "raw_ocr=", r.get("raw_ocr_person_match"),
                    "filename=", r.get("persona_token_matches_filename"),
                    "total=", r.get("persona_token_matches"),
                )
            
            if persona_gpt:
                final_results = [
                    r for r in final_results
                    if (
                        r.get("persona_match") is True
                        and (
                            (r.get("persona_token_matches_strong") or 0) > 0
                            or (r.get("persona_token_matches_medium") or 0) > 0
                            or (r.get("persona_token_matches_generic") or 0) > 0
                            or (r.get("persona_token_matches_chunk") or 0) > 0
                            or r.get("raw_ocr_person_match") is True
                        )
                        and not (
                            (r.get("persona_token_matches_strong") or 0) == 0
                            and (r.get("persona_token_matches_medium") or 0) == 0
                            and (r.get("persona_token_matches_generic") or 0) == 0
                            and (r.get("persona_token_matches_chunk") or 0) == 0
                            and (r.get("raw_ocr_person_match") is not True)
                            and (r.get("persona_token_matches_filename") or 0) > 0
                        )
                    )
                ]

                print("[PERSON FILTER] Dopo filtro:", len(final_results))

    # =========================================================
    # PRIORITÀ ASSOLUTA AI MATCH REALI SU OCR RAW
    # Se la persona è stata trovata davvero nelle pagine OCR,
    # mostriamo SOLO quei documenti
    # =========================================================
                if raw_ocr_doc_ids:
                    final_results = [
                        r for r in final_results
                        if r.get("id") in raw_ocr_doc_ids
                    ]

                    print("[RAW OCR PRIORITY ACTIVE]")
                    print("[RAW OCR PRIORITY COUNT]", len(final_results))

            threshold = 0.55

            if persona_gpt:
                threshold = 0.0
                
            print("[THRESHOLD USED]", threshold)
            
            below_threshold = [
                r for r in final_results
                if (r.get("boosted_score") or 0) < threshold
            ]

            above_threshold = [
                r for r in final_results
                if (r.get("boosted_score") or 0) >= threshold
            ]

            filtered_results = [
                r for r in final_results
                if (r.get("boosted_score") or 0) >= threshold
            ]

            if persona_gpt:
                final_results = filtered_results
            else:
                if filtered_results:
                    final_results = filtered_results
            
            print("[FINAL_RESULTS_AFTER_THRESHOLD]", len(final_results))

            for r in final_results[:10]:
                print(
                    "[FINAL DOC]",
                    "id=", r["id"],
                    "boosted_score=", r.get("boosted_score"),
                    "semantic_score=", r.get("score"),
                    "persona_match=", r.get("persona_match"),
                    "tipo_match=", r.get("tipo_match"),
                    "text_match=", r.get("text_match"),
                    "match_type=", r.get("match_type"),
                    "tipo_documento=", r.get("tipo_documento"),
                    "file=", r.get("file_path")
                )
    
            final_results.sort(
                key=lambda r: (
                    1 if r.get("persona_match") else 0,
                    r.get("persona_token_matches") or 0,
                    1 if r.get("text_match") else 0,
                    1 if r.get("tipo_match") else 0,
                    r.get("boosted_score") or 0,
                    r.get("score") or 0
                ),
                reverse=True
            )
            print("[FINAL_RESULT_IDS]", [r["id"] for r in final_results], "count=", len(final_results))
            
            print("[FINAL_RESULT_IDS]", [r["id"] for r in final_results], "count=", len(final_results))

            
            deduped_results = []
            seen_ids = set()

            for r in final_results:
                doc_id = r.get("id")
                if doc_id in seen_ids:
                    continue
                seen_ids.add(doc_id)
                deduped_results.append(r)

            final_results = deduped_results
            
            print("================ SEARCH DEBUG END ================\n")            

            final_ids = [r["id"] for r in final_results]
            missing_vs_raw = sorted(set(raw_ocr_match_ids) - set(final_ids))

            print("[FINAL_IDS]", final_ids)
            print("[MISSING_VS_RAW]", missing_vs_raw)
            
            return JSONResponse({
                "success": True,
                "mode": "semantic",
                "results": final_results
            })


    except Exception as e:
        return json_error_response(e, context="SEARCH_DOCUMENTS")

@app.get("/documents_snapshot/")
async def documents_snapshot():
    async with async_engine.connect() as conn:
        result = await conn.execute(
            select(
                documents.c.id,
                documents.c.tipo_documento,
                documents.c.file_path,
                documents.c.data_creazione
            )
        )
        rows = result.fetchall()

    docs = []
    tipo_count = {}
    date_count = {}

    for r in rows:
        docs.append({
            "id": r.id,
            "tipo_documento": r.tipo_documento,
            "file_path": r.file_path,
            "data_creazione": r.data_creazione.strftime("%Y-%m-%d %H:%M:%S")
        })

        tipo_count[r.tipo_documento] = tipo_count.get(r.tipo_documento, 0) + 1
        day_str = r.data_creazione.strftime("%Y-%m-%d")
        date_count[day_str] = date_count.get(day_str, 0) + 1

    if rows:
        last_upload = max(rows, key=lambda r: r.data_creazione).data_creazione.strftime("%d/%m/%Y, %H:%M:%S")
    else:
        last_upload = "-"

    return JSONResponse(
        content={
            "docs": docs,
            "stats": {
                "tipo_count": tipo_count,
                "date_count": date_count,
                "last_upload": last_upload
            }
        },
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )

  
@app.post("/jobs/{job_id}/cancel")
async def cancel_job(job_id: int):
    from sqlalchemy import update
    from datetime import datetime

    try:
        async with async_engine.begin() as conn:
            result = await conn.execute(
                select(
                    document_jobs.c.id,
                    document_jobs.c.file_name,
                    document_jobs.c.file_path,
                    document_jobs.c.status,
                    document_jobs.c.document_id
                ).where(document_jobs.c.id == job_id)
            )
            job = result.mappings().first()

            if not job:
                return JSONResponse(
                    status_code=404,
                    content={"success": False, "error": "Job non trovato"}
                )

            if job["status"] in ["completed", "error", "cancelled"]:
                return JSONResponse(
                    content={
                        "success": True,
                        "message": f"Job già in stato {job['status']}",
                        "status": job["status"]
                    }
                )

            await conn.execute(
                update(document_jobs)
                .where(document_jobs.c.id == job_id)
                .values(
                    cancelled=1,
                    status="cancelled",
                    phase="cancelled",
                    updated_at=datetime.utcnow()
                )
            )

        await notify_clients({
            "event": "job_cancelled",
            "job_id": job_id,
            "file_name": job["file_name"],
            "status": "cancelled",
            "phase": "cancelled",
            "message": "⛔ Caricamento annullato"
        })

        return JSONResponse(
            content={
                "success": True,
                "job_id": job_id,
                "file_name": job["file_name"],
                "status": "cancelled"
            }
        )

    except Exception as e:
        print(f"[CANCEL JOB] Errore annullamento job {job_id}: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": "Errore durante annullamento job"}
        )