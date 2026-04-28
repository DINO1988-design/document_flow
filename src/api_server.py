# src/api_server.py

from fastapi import FastAPI, Request, File, UploadFile, Query, WebSocket, Body
from typing import List
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import os, json, zipfile
from io import BytesIO
from src.db_setup import async_engine, async_session, documents, document_fields, document_jobs, clients
from src.process_document import process_document
from sqlalchemy import select, insert, delete, update, text, or_
from fastapi.responses import JSONResponse
from datetime import datetime, date
from src.document_field_flattener import flatten_document_fields
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
    add_document_chunks,
    add_document_page_chunks,
    search_chunks,
    get_chunk_index_count,
    get_chunk_doc_ids,
    get_chunk_store_count,
    debug_chunk_store_lengths   # <-- AGGIUNGI QUESTO
)
import asyncio
from sqlalchemy.exc import DBAPIError
import re
import copy


from src.document_field_normalizer import (
    normalize_document_type,
    DOCUMENT_TYPE_ALIASES,
    normalize_structured_query_filters,
    normalize_query_field_name,
    normalize_text,
    extract_canonical_fields_only,
)
from src.search_ranking import (
    tokenize_text,
    raw_ocr_person_match,
    has_fuzzy_person_match,
    is_generic_document_type,
)
from src.search_explain import get_semantic_label
from src.search_candidates import collect_candidate_doc_ids

from src.search_scoring_service import (
    load_documents_full_payload,
    build_document_frequency_for_candidates,
    compute_scored_document,
    build_preview_source_text,
)
from src.search_postprocessing import postprocess_search_results
from src.search_date_filter import filter_documents_by_query_date
from src.search_fuzzy_recall import apply_fuzzy_person_recall

from src.search_query_preparation import prepare_search_query_context
from src.search_results_pipeline import build_search_results
from src.search_structured_service import (
    build_structured_results,
    search_structured_documents,
    apply_conditions_to_documents_stmt,
)

from src.search_query_enrichment import (
    infer_tipo_documento_from_keywords,
    enrich_tipo_conditions_from_query_text,
    has_positive_tipo_condition,
)

faiss_rebuild_lock = asyncio.Lock()
templates = Jinja2Templates(directory="src/templates")

QUERY_PARSE_CACHE: dict[str, dict] = {}

UPLOAD_FOLDER = "src/uploads/"
OUTPUT_FOLDER = "output/"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def get_file_metadata(file_path: str) -> dict:
    file_name = os.path.basename(file_path or "")
    file_extension = os.path.splitext(file_name)[1].lower().replace(".", "")

    possible_paths = [
        file_path,
        os.path.join(UPLOAD_FOLDER, file_name),
        os.path.abspath(file_path or "")
    ]

    real_path = next((p for p in possible_paths if p and os.path.exists(p)), None)

    file_size = os.path.getsize(real_path) if real_path else 0

    return {
        "file_extension": file_extension,
        "file_size": file_size
    }

def build_tipo_documento_sql_conditions(tipo_value: str | None):
    tipo_canonico = normalize_document_type(tipo_value)

    if not tipo_canonico or tipo_canonico == "generico":
        return []

    aliases = DOCUMENT_TYPE_ALIASES.get(tipo_canonico, [])
    variants = [tipo_canonico, *aliases]

    normalized_seen = set()
    conditions = []

    for variant in variants:
        v = normalize_text(variant)
        if not v or v in normalized_seen:
            continue
        normalized_seen.add(v)
        conditions.append(documents.c.tipo_documento.ilike(f"%{v}%"))

    return conditions


def try_parse_date_value(value) -> date | None:
    if value in [None, "", []]:
        return None

    s = str(value).strip()
    if not s:
        return None

    s = s.replace("\\", "/").replace(".", "/").strip()

    formats = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%d/%m/%y",
        "%d-%m-%y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass

    # fallback ISO tipo 2026-01-15T00:00:00
    try:
        return datetime.fromisoformat(s.replace("Z", "")).date()
    except Exception:
        pass

    # prova a trovare una data dentro una stringa più lunga
    patterns = [
        r"\b(\d{4}-\d{2}-\d{2})\b",
        r"\b(\d{2}/\d{2}/\d{4})\b",
        r"\b(\d{2}-\d{2}-\d{4})\b",
    ]

    for pattern in patterns:
        m = re.search(pattern, s)
        if not m:
            continue

        candidate = m.group(1)

        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(candidate, fmt).date()
            except Exception:
                pass

    return None

def get_document_date_from_fields(fields: dict) -> date | None:
    """
    Cerca dinamicamente una data documento nei campi.
    Priorità a campi data più probabili.
    """
    if not fields:
        return None

    flat_fields = flatten_document_fields(fields or {})

    preferred_keys = [
        "data_documento",
        "data_emissione",
        "data_bonifico",
        "data_operazione",
        "data_valuta",
        "data",
        "booking_date",
        "departure_date",
        "travel_date",
        "data_inizio",
        "data_fine",
    ]

    # 1. priorità ai campi noti
    for key in preferred_keys:
        value = flat_fields.get(key)
        parsed = try_parse_date_value(value)
        if parsed:
            return parsed

    # 2. fallback: qualsiasi campo che sembri una data
    for k, v in flat_fields.items():
        k_norm = normalize_text(k)

        if not any(token in k_norm for token in ["data", "date", "valuta", "booking", "departure", "travel"]):
            continue

        parsed = try_parse_date_value(v)
        if parsed:
            return parsed

    return None

def parse_query_date_bounds(parsed_query: dict) -> tuple[date | None, date | None]:
    data_da = try_parse_date_value(parsed_query.get("data_da"))
    data_a = try_parse_date_value(parsed_query.get("data_a"))
    return data_da, data_a

def document_matches_query_date(
    fields: dict,
    row: dict,
    data_da: date | None,
    data_a: date | None,
    use_creation_fallback: bool = True
) -> tuple[bool, date | None, str | None]:
    """
    Restituisce:
    - match boolean
    - data usata
    - source: 'document_field' | 'data_creazione' | None
    """
    if not data_da and not data_a:
        return True, None, None

    doc_date = get_document_date_from_fields(fields)
    if doc_date:
        if data_da and doc_date < data_da:
            return False, doc_date, "document_field"
        if data_a and doc_date > data_a:
            return False, doc_date, "document_field"
        return True, doc_date, "document_field"

    if use_creation_fallback:
        created = row.get("data_creazione")
        created_date = None

        if isinstance(created, datetime):
            created_date = created.date()
        elif created:
            created_date = try_parse_date_value(created)

        if created_date:
            if data_da and created_date < data_da:
                return False, created_date, "data_creazione"
            if data_a and created_date > data_a:
                return False, created_date, "data_creazione"
            return True, created_date, "data_creazione"

    return True, None, None    
    


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
                    documents.c.ocr_pages,
                    documents.c.campi_canonici_json
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
                
                canonical_fields = {}
                raw_canonical = row.get("campi_canonici_json")

                if raw_canonical:
                    try:
                        canonical_fields = json.loads(raw_canonical)
                        if not isinstance(canonical_fields, dict):
                            canonical_fields = {}
                    except Exception as e:
                        print(f"[AI_SEARCH] Errore parsing campi_canonici_json doc_id={doc_id}: {e}")
                        canonical_fields = {}        

                doc_text_for_ai = build_text_for_ai(
                    row["tipo_documento"],
                    row["file_path"],
                    fields_dict,
                    canonical_fields=canonical_fields,
                    ocr_text=row.get("ocr_text")
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
                    fields_dict,
                    canonical_fields=canonical_fields
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
                    document_fields.c.id,
                    document_fields.c.document_id,
                    document_fields.c.campo,
                    document_fields.c.valore
                )
                .where(document_fields.c.document_id.in_(doc_ids))
                .order_by(document_fields.c.id.asc())
            )

            for f in fields_result.fetchall():
                campo = str(f.campo or "").strip()
                fields_map.setdefault(f.document_id, {})[campo] = f.valore

        docs = []
        for r in rows:
            meta = get_file_metadata(r.file_path)

            docs.append({
                "id": r.id,
                "tipo_documento": r.tipo_documento,
                "file_path": r.file_path,
                "file_extension": meta["file_extension"],
                "file_size": meta["file_size"],
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
            select(
                document_fields.c.id,
                document_fields.c.campo,
                document_fields.c.valore
            )
            .where(document_fields.c.document_id == doc_id)
            .order_by(document_fields.c.id.asc())
        )

        fields = {}
        for f in fields_result.fetchall():
            campo = str(f.campo or "").strip()
            fields[campo] = f.valore

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

@app.post("/documents/{doc_id}/update_fields")
async def update_document_fields(doc_id: int, payload: dict = Body(...)):
    try:
        fields_to_update = payload.get("fields", {})

        if not isinstance(fields_to_update, dict) or not fields_to_update:
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": "Payload non valido. Usa {'fields': {...}}"
                }
            )

        async with async_engine.begin() as conn:
            doc_result = await conn.execute(
                select(documents).where(documents.c.id == doc_id)
            )
            doc_row = doc_result.mappings().first()

            if not doc_row:
                return JSONResponse(
                    status_code=404,
                    content={
                        "success": False,
                        "error": "Documento non trovato"
                    }
                )

            existing_fields = await load_document_fields_map(conn, doc_id)

            for campo, valore in fields_to_update.items():
                campo = str(campo).strip()
                valore = normalize_field_value_for_save(valore)

                if not campo:
                    continue

                campo_norm = normalize_field_key_for_compare(campo)

                print("[UPDATE_FIELDS] campo ricevuto =", repr(campo))
                print("[UPDATE_FIELDS] valore ricevuto =", repr(valore))

                existing_rows_stmt = (
                    select(
                        document_fields.c.id,
                        document_fields.c.campo,
                        document_fields.c.valore
                    )
                    .where(document_fields.c.document_id == doc_id)
                    .order_by(document_fields.c.id.asc())
                )
                existing_rows = (await conn.execute(existing_rows_stmt)).mappings().all()

                matching_rows = [
                    row for row in existing_rows
                    if normalize_field_key_for_compare(row["campo"]) == campo_norm
                ]

                print("[UPDATE_FIELDS] matching_rows =", [
                    {
                        "id": r["id"],
                        "campo": r["campo"],
                        "valore": r["valore"]
                    }
                    for r in matching_rows
                ])

                if matching_rows:
                    matching_ids = [row["id"] for row in matching_rows]

                    result = await conn.execute(
                        update(document_fields)
                        .where(document_fields.c.id.in_(matching_ids))
                        .values(
                            campo=campo,
                            valore=valore
                        )
                    )

                    print("[UPDATE_FIELDS] updated rowcount =", result.rowcount)
                else:
                    await conn.execute(
                        insert(document_fields).values(
                            document_id=doc_id,
                            campo=campo,
                            valore=valore
                        )
                    )
                    print("[UPDATE_FIELDS] inserted new field =", campo)

                existing_fields[campo] = valore

            tipo_documento = doc_row["tipo_documento"]
            canonical_fields = await rebuild_document_canonical_fields(
                conn,
                doc_id,
                tipo_documento,
                existing_fields
            )
        async def regenerate_word_background(fields_snapshot, file_path_snapshot, current_doc_id):
            try:
                from src.main import generate_word
                await asyncio.to_thread(generate_word, fields_snapshot, file_path_snapshot)
            except Exception as e:
                print(f"[UPDATE_FIELDS] Errore rigenerazione Word doc_id={current_doc_id}: {e}")

        asyncio.create_task(
            regenerate_word_background(
                dict(existing_fields),
                doc_row["file_path"],
                doc_id
            )
        )

        try:
            QUERY_PARSE_CACHE.clear()
        except Exception:
            pass

        asyncio.create_task(rebuild_faiss_from_db())

        return JSONResponse(
            content={
                "success": True,
                "doc_id": doc_id,
                "updated_fields": list(fields_to_update.keys()),
                "canonical_fields": canonical_fields
            }
        )

    except Exception as e:
        return json_error_response(e, context="UPDATE_DOCUMENT_FIELDS")

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


async def load_fields_for_doc(conn, doc_id: int) -> dict:
    fields_stmt = select(
        document_fields.c.campo,
        document_fields.c.valore
    ).where(document_fields.c.document_id == doc_id)

    fields_rows = (await conn.execute(fields_stmt)).mappings().all()
    return {f["campo"]: f["valore"] for f in fields_rows}

def success_search_response(mode: str, results: list):
    return JSONResponse({
        "success": True,
        "mode": mode,
        "results": results
    })

def get_query_cache_key(query: str) -> str:
    return (query or "").strip().lower()

async def get_parsed_query_with_cache(query: str):
    from src.query_understanding_gpt import parse_search_query_with_gpt
    import time

    query_cache_key = get_query_cache_key(query)
    t_parse_query = time.time()

    if query_cache_key in QUERY_PARSE_CACHE:
        parsed_query = copy.deepcopy(QUERY_PARSE_CACHE[query_cache_key])
        print(f"[PERF] parse_query_gpt: {round(time.time() - t_parse_query, 3)}s [CACHE]")
        return parsed_query

    parsed_query = await asyncio.to_thread(parse_search_query_with_gpt, query)
    QUERY_PARSE_CACHE[query_cache_key] = copy.deepcopy(parsed_query)
    print(f"[PERF] parse_query_gpt: {round(time.time() - t_parse_query, 3)}s")
    return parsed_query

async def collect_debug_raw_ocr_match_ids(
    *,
    persona_gpt: str | None,
    tipo_doc_gpt: str | None,
) -> list[int]:
    if not persona_gpt:
        return []

    raw_ocr_match_ids: list[int] = []

    async with async_engine.connect() as conn_debug:
        raw_stmt = select(
            documents.c.id,
            documents.c.tipo_documento,
            documents.c.ocr_text,
            documents.c.ocr_pages,
        )

        if tipo_doc_gpt and not is_generic_document_type(tipo_doc_gpt):
            tipo_conditions = build_tipo_documento_sql_conditions(tipo_doc_gpt)
            if tipo_conditions:
                raw_stmt = raw_stmt.where(or_(*tipo_conditions))

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

    return raw_ocr_match_ids

def unpack_candidate_bundle(candidate_bundle: dict) -> dict:
    return {
        "keywords": candidate_bundle["keywords"],
        "text_doc_ids": candidate_bundle["text_doc_ids"],
        "semantic_hits_by_id": candidate_bundle["semantic_hits_by_id"],
        "semantic_doc_ids": candidate_bundle["semantic_doc_ids"],
        "raw_ocr_doc_ids": candidate_bundle["raw_ocr_doc_ids"],
        "raw_ocr_first_page_by_doc": candidate_bundle["raw_ocr_first_page_by_doc"],
        "best_chunk_per_doc": candidate_bundle["best_chunk_per_doc"],
        "all_doc_ids": candidate_bundle["all_doc_ids"],
    }
def print_candidate_debug_info(
    *,
    raw_ocr_doc_ids,
    text_doc_ids,
    semantic_doc_ids,
    all_doc_ids,
):
    print("[RAW_OCR_DOC_IDS SAMPLE]", sorted(list(raw_ocr_doc_ids))[:20])
    print("[RAW_OCR_DOC_IDS COUNT]", len(raw_ocr_doc_ids))
    print("[TEXT_DOC_IDS SAMPLE]", sorted(list(text_doc_ids))[:20])
    print("[TEXT_DOC_IDS COUNT]", len(text_doc_ids))
    print("[SEMANTIC_DOC_IDS SAMPLE]", sorted(list(semantic_doc_ids))[:20])
    print("[SEMANTIC_DOC_IDS COUNT]", len(semantic_doc_ids))
    print("[ALL_DOC_IDS SAMPLE]", sorted(list(all_doc_ids))[:20])
    print("[ALL_DOC_IDS COUNT]", len(all_doc_ids))

async def load_filtered_docs_map(
    *,
    conn,
    all_doc_ids,
    conditions,
    tipo_doc_gpt,
    nome_file_gpt,
):
    docs_stmt = select(
        documents.c.id,
        documents.c.tipo_documento,
        documents.c.file_path,
        documents.c.data_creazione,
    ).where(documents.c.id.in_(all_doc_ids))

    docs_stmt = apply_conditions_to_documents_stmt(
        docs_stmt,
        conditions,
        build_tipo_documento_sql_conditions,
    )

    if tipo_doc_gpt and not is_generic_document_type(tipo_doc_gpt):
        has_tipo_condition = any(
            isinstance(c, dict)
            and normalize_text(str(c.get("target") or "")) in {"tipo_documento", "tipo documento"}
            for c in conditions
        )

        if not has_tipo_condition:
            tipo_conditions = build_tipo_documento_sql_conditions(tipo_doc_gpt)
            if tipo_conditions:
                docs_stmt = docs_stmt.where(or_(*tipo_conditions))

    if nome_file_gpt:
        docs_stmt = docs_stmt.where(documents.c.file_path.ilike(f"%{nome_file_gpt}%"))

    doc_rows = (await conn.execute(docs_stmt)).mappings().all()
    docs_map = {row["id"]: row for row in doc_rows}

    return doc_rows, docs_map
    
def print_date_filter_debug_info(
    *,
    query_data_da,
    query_data_a,
    candidate_bundle,
    all_doc_ids,
    date_filter_debug,
):
    if query_data_da or query_data_a:
        print("[DATE FILTER ACTIVE]")
        print("[DATE FILTER RANGE]", query_data_da, query_data_a)
        print("[DATE FILTER BEFORE]", len(candidate_bundle["all_doc_ids"]))
        print("[DATE FILTER AFTER]", len(all_doc_ids))
        print("[DATE FILTER DEBUG SAMPLE]", date_filter_debug[:10])    
        
def print_person_filter_debug(persona_gpt, final_results):
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

def print_final_results_debug(final_results):
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

    print("[FINAL_RESULT_IDS]", [r["id"] for r in final_results], "count=", len(final_results))

def print_final_id_debug(final_results, raw_ocr_match_ids):
    final_ids = [r["id"] for r in final_results]
    missing_vs_raw = sorted(set(raw_ocr_match_ids) - set(final_ids))

    print("[FINAL_IDS]", final_ids)
    print("[MISSING_VS_RAW]", missing_vs_raw)

def normalize_field_value_for_save(value):
    if isinstance(value, (dict, list)):
        value = json.dumps(value, ensure_ascii=False)

    if value is None:
        return ""

    value = str(value)

    if len(value) > 4000:
        value = value[:4000]

    return value
    
def normalize_field_key_for_compare(value: str) -> str:
    return normalize_text(str(value or "").strip())
    
async def load_document_fields_map(conn, doc_id: int) -> dict:
    fields_stmt = (
        select(
            document_fields.c.id,
            document_fields.c.campo,
            document_fields.c.valore
        )
        .where(document_fields.c.document_id == doc_id)
        .order_by(document_fields.c.id.asc())
    )

    fields_rows = (await conn.execute(fields_stmt)).mappings().all()

    fields_map = {}
    for f in fields_rows:
        campo = str(f["campo"] or "").strip()
        fields_map[campo] = f["valore"]

    return fields_map

async def rebuild_document_canonical_fields(conn, doc_id: int, tipo_documento: str, fields_map: dict):
    canonical_fields = extract_canonical_fields_only(tipo_documento, fields_map)

    await conn.execute(
        update(documents)
        .where(documents.c.id == doc_id)
        .values(
            campi_canonici_json=json.dumps(canonical_fields, ensure_ascii=False)
        )
    )

    return canonical_fields
    
@app.get("/search_documents/")
async def search_documents(request: Request, debug: bool = False):
    
    import time
    t0 = time.time()

    try:
        query = request.query_params.get("query", "").strip()
        parsed_query = await get_parsed_query_with_cache(query)
        
        parsed_query = normalize_structured_query_filters(query, parsed_query)
        print("[AFTER NORMALIZE STRUCTURED]", parsed_query)
        
        parsed_query = infer_tipo_documento_from_keywords(parsed_query)
        parsed_query = enrich_tipo_conditions_from_query_text(query, parsed_query)
    
        query_context = prepare_search_query_context(query, parsed_query)

        parsed_query = query_context["parsed_query"]
        query_norm = query_context["query_norm"]
        tipo_doc_gpt = query_context["tipo_doc_gpt"]
        persona_gpt = query_context["persona_gpt"]
        nome_file_gpt = query_context["nome_file_gpt"]
        keywords_gpt = query_context["keywords_gpt"]
        conditions_gpt = query_context["conditions_gpt"]
        has_textual_theme = query_context["has_textual_theme"]
        has_positive_tipo = has_positive_tipo_condition(conditions_gpt)   
        
        raw_ocr_match_ids = await collect_debug_raw_ocr_match_ids(
            persona_gpt=persona_gpt,
            tipo_doc_gpt=tipo_doc_gpt,
        )

        print("[RAW_OCR_MATCH_IDS]", raw_ocr_match_ids)
        print("[RAW_OCR_MATCH_COUNT]", len(raw_ocr_match_ids))

 

        print("\n================ SEARCH DEBUG START ================")
        print("[QUERY]", query)
        print("[SEARCH GPT PARSED]", parsed_query)
        print("[HAS_TEXTUAL_THEME]", has_textual_theme)
        print("[TIPO_DOC_GPT]", tipo_doc_gpt)
        
        print("[STRUCTURED SEARCH INPUT]", {
            "tipo_documento": parsed_query.get("tipo_documento"),
            "campo_target": parsed_query.get("campo_target"),
            "valore_target": parsed_query.get("valore_target"),
            "operatore": parsed_query.get("operatore"),
            "tipo_valore": parsed_query.get("tipo_valore"),
            "data_da": parsed_query.get("data_da"),
            "data_a": parsed_query.get("data_a"),
        })
        if not query:
            return success_search_response("empty", [])
            
        async with async_engine.connect() as conn:
            # =========================
            # 1. TENTATIVO RICERCA STRUTTURATA
            # =========================
            structured_rows = await search_structured_documents(
                conn,
                parsed_query,
                build_tipo_documento_sql_conditions,
            )

            tipo_doc = parsed_query.get("tipo_documento")

            if structured_rows:
                if not persona_gpt and not parsed_query.get("campo_target") and not has_textual_theme:
                    structured_results = await build_structured_results(conn, structured_rows, tipo_doc)

                    return success_search_response("structured", structured_results)
  
            # =========================
            # 2. FALLBACK IBRIDO: TESTUALE + SEMANTICO
            # =========================
         
            t_candidates = time.time()
            
            candidate_bundle = await collect_candidate_doc_ids(
                conn=conn,
                query=query,
                keywords_gpt=keywords_gpt,
                persona_gpt=persona_gpt,
                tipo_doc_gpt=tipo_doc_gpt,
                nome_file_gpt=nome_file_gpt,
                build_tipo_documento_sql_conditions=build_tipo_documento_sql_conditions,
                is_generic_document_type=is_generic_document_type,
                search=search,
                search_chunks=search_chunks,
            )
            print(f"[PERF] candidate_collection: {round(time.time() - t_candidates, 3)}s")
            
            candidate_data = unpack_candidate_bundle(candidate_bundle)

            keywords = candidate_data["keywords"]
            text_doc_ids = candidate_data["text_doc_ids"]
            semantic_hits_by_id = candidate_data["semantic_hits_by_id"]
            semantic_doc_ids = candidate_data["semantic_doc_ids"]
            raw_ocr_doc_ids = candidate_data["raw_ocr_doc_ids"]
            raw_ocr_first_page_by_doc = candidate_data["raw_ocr_first_page_by_doc"]
            best_chunk_per_doc = candidate_data["best_chunk_per_doc"]
            all_doc_ids = candidate_data["all_doc_ids"]

            print_candidate_debug_info(
                raw_ocr_doc_ids=raw_ocr_doc_ids,
                text_doc_ids=text_doc_ids,
                semantic_doc_ids=semantic_doc_ids,
                all_doc_ids=all_doc_ids,
            )

            if not all_doc_ids:
                return success_search_response("semantic", [])

            conditions = conditions_gpt

            doc_rows, docs_map = await load_filtered_docs_map(
                conn=conn,
                all_doc_ids=all_doc_ids,
                conditions=conditions,
                tipo_doc_gpt=tipo_doc_gpt,
                nome_file_gpt=nome_file_gpt,
            )

            all_doc_ids = [doc_id for doc_id in all_doc_ids if doc_id in docs_map]

            query_data_da, query_data_a = parse_query_date_bounds(parsed_query)


            
            print("[DOC_ROWS AFTER SQL FILTER SAMPLE]", [r["id"] for r in doc_rows][:20])
            print("[DOC_ROWS AFTER SQL FILTER COUNT]", len(doc_rows))


            # Fallback fuzzy recall persona generalizzato
            if persona_gpt:
                docs_map, all_doc_ids, fuzzy_added = await apply_fuzzy_person_recall(
                    conn,
                    parsed_query=parsed_query,
                    persona_gpt=persona_gpt,
                    tipo_doc_gpt=tipo_doc_gpt,
                    docs_map=docs_map,
                    all_doc_ids=all_doc_ids,
                    documents=documents,
                    document_fields=document_fields,
                    tokenize_text=tokenize_text,
                    has_fuzzy_person_match=has_fuzzy_person_match,
                    apply_conditions_to_documents_stmt=apply_conditions_to_documents_stmt,
                    build_tipo_documento_sql_conditions=build_tipo_documento_sql_conditions,
                    is_generic_document_type=is_generic_document_type,
                )

                print("[FUZZY_RECALL_ADDED]", fuzzy_added)
                print("[ALL_DOC_IDS_AFTER_FUZZY]", len(all_doc_ids))
 
 
            # =========================
            # FILTRO DATA DOCUMENTO
            # =========================
            docs_map, all_doc_ids, date_filter_debug = await filter_documents_by_query_date(
                conn=conn,
                all_doc_ids=all_doc_ids,
                docs_map=docs_map,
                raw_ocr_doc_ids=raw_ocr_doc_ids,
                has_positive_tipo=has_positive_tipo,
                data_da=query_data_da,
                data_a=query_data_a,
                document_matches_query_date=document_matches_query_date,
                load_fields_for_doc=load_fields_for_doc,
            )
            docs_map = {
                doc_id: row
                for doc_id, row in docs_map.items()
                if doc_id in all_doc_ids
            }

            docs_payload = await load_documents_full_payload(conn, list(all_doc_ids))

            document_frequency = build_document_frequency_for_candidates(
                docs_payload=docs_payload,
                best_chunk_per_doc=best_chunk_per_doc,
            )

            print_date_filter_debug_info(
                query_data_da=query_data_da,
                query_data_a=query_data_a,
                candidate_bundle=candidate_bundle,
                all_doc_ids=all_doc_ids,
                date_filter_debug=date_filter_debug,
            )
                
            if not all_doc_ids:
                return success_search_response("semantic", [])

  
            t_scoring = time.time()

            final_results, results_debug = await build_search_results(
                all_doc_ids=all_doc_ids,
                docs_map=docs_map,
                docs_payload=docs_payload,
                parsed_query=parsed_query,
                query=query,
                persona_gpt=persona_gpt,
                text_doc_ids=text_doc_ids,
                semantic_hits_by_id=semantic_hits_by_id,
                best_chunk_per_doc=best_chunk_per_doc,
                raw_ocr_doc_ids=raw_ocr_doc_ids,
                raw_ocr_first_page_by_doc=raw_ocr_first_page_by_doc,
                document_frequency=document_frequency,
                compute_scored_document=compute_scored_document,
                build_preview_source_text=build_preview_source_text,
            )   
            
            print(f"[PERF] scoring: {round(time.time() - t_scoring, 3)}s")            
            
            
            print("[SKIPPED_MISSING_ROW]", results_debug["skipped_missing_row"])
            print("[SKIPPED_NOT_TICKET]", results_debug["skipped_not_ticket"])
            print("[PROCESSED_DOCS]", results_debug["processed_docs"])
            print("[FINAL_RESULTS_BEFORE_POSTPROCESS]", len(final_results))

            print_person_filter_debug(persona_gpt, final_results)

            final_results, postprocess_debug = postprocess_search_results(
                final_results,
                parsed_query=parsed_query,
                query=query,
                persona_gpt=persona_gpt,
            )

            print("[POSTPROCESS DEBUG]", postprocess_debug)
            print("[FINAL_RESULTS_AFTER_POSTPROCESS]", len(final_results))

            print_final_results_debug(final_results)

            print("================ SEARCH DEBUG END ================\n")

            print_final_id_debug(final_results, raw_ocr_match_ids)
            
            return success_search_response("semantic", final_results)

    except Exception as e:
        return json_error_response(e, context="SEARCH_DOCUMENTS")
        
    finally:
        print(f"[PERF] TOTAL TIME: {round(time.time() - t0, 3)}s")


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
        meta = get_file_metadata(r.file_path)

        docs.append({
            "id": r.id,
            "tipo_documento": r.tipo_documento,
            "file_path": r.file_path,
            "file_extension": meta["file_extension"],
            "file_size": meta["file_size"],
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