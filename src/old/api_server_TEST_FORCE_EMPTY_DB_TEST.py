# src/api_server.py

from fastapi import FastAPI, Request, File, UploadFile, Query, WebSocket, BackgroundTasks
from typing import List
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import os, json, zipfile
TEST_FORCE_EMPTY_DB = True
from io import BytesIO
from src.db_setup import async_engine, async_session, documents, document_fields, document_jobs, clients
from src.process_document import process_document
from src.main import interpret_search_query
from sqlalchemy import select, cast, Float, insert, delete
from fastapi.responses import JSONResponse
from src.ai_search import load_index, search, rebuild_index_from_items, reset_index, get_index_count
import asyncio
from sqlalchemy.exc import DBAPIError


faiss_rebuild_lock = asyncio.Lock()

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
    from sqlalchemy import or_

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
        await update_job_row(
            job_id,
            status="error",
            phase="error",
            error_message=str(e),
            completed_at=datetime.utcnow()
        )

        await notify_progress(
            100,
            f"❌ Errore: {str(e)}",
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

    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(select(documents.c.id))
            db_doc_ids = [row[0] for row in result.fetchall()]

        db_count = len(db_doc_ids)
        ai_count = get_index_count()

        print(f"[STARTUP] Documenti nel DB: {db_count}")
        print(f"[STARTUP] Documenti nell'indice AI: {ai_count}")

        if ai_count != db_count:
            print("[STARTUP] Indice AI disallineato dal DB, ricostruzione in corso...")
            await rebuild_faiss_from_db()
        else:
            print("[STARTUP] Indice AI già allineato al DB")

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

    return {"uploaded": uploaded_files}
    
@app.get("/jobs_status/")
async def get_jobs_status():

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

    return {"jobs": jobs}
    
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

        async with async_engine.connect() as conn:
            docs_result = await conn.execute(
                select(
                    documents.c.id,
                    documents.c.tipo_documento,
                    documents.c.file_path
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

                text_for_ai = f"tipo_documento: {row['tipo_documento']}\n"
                for f in fields_rows:
                    text_for_ai += f"{f['campo']}: {f['valore']}\n"

                text_for_ai += f"file: {os.path.basename(row['file_path'])}"

                items.append({
                    "doc_id": doc_id,
                    "text": text_for_ai
                })

        if items:
            await asyncio.to_thread(rebuild_index_from_items, items)
        else:
            await asyncio.to_thread(reset_index)

        print(f"[AI_SEARCH] Indice ricostruito: {len(items)} documenti")
        

# -------------------------------
# HTML principale
# -------------------------------


html_content = """
<h1 style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333;">Document Flow Web</h1>

<div style="display:flex; gap:15px; margin-bottom:20px;">
    <div class="kpi-card kpi-total">
        <div class="kpi-title">📄 Totale documenti</div>
        <div id="kpi_total" class="kpi-value">0</div>
    </div>
    <div class="kpi-card kpi-today">
        <div class="kpi-title">📅 Documenti oggi</div>
        <div id="kpi_today" class="kpi-value">0</div>
    </div>
    <div class="kpi-card kpi-type">
        <div class="kpi-title">🏷️ Tipo più comune</div>
        <div id="kpi_tipo" class="kpi-value">-</div>
    </div>
    <div class="kpi-card kpi-last">
        <div class="kpi-title">🕒 Ultimo upload</div>
        <div id="kpi_last" class="kpi-value">-</div>
    </div>
</div>


<!-- Upload -->
<section style="margin-bottom:30px;">
<h2>Carica documento</h2>
<div id="drop-area">
    <p>📂 Trascina qui i file oppure clicca per selezionarli</p>
    <input type="file" id="fileElem" multiple style="display:none;">
</div>

<ul id="fileList" style="list-style:none; padding:0; margin-top:10px;"></ul>

<p id="statusText" style="margin-top:5px;"></p>
</section>

<!-- Ricerca AI -->
<section style="margin-bottom:30px;">
    <h2 style="display:flex; align-items:center; gap:10px;">
    Ricerca AI documenti
    <span id="searchStatusBadge" class="search-badge" style="display:none;"></span>
    </h2>
    <div style="display:flex; gap:10px; align-items:center; margin-bottom:10px;">
    <input id="aiQuery" type="text" placeholder="Inserisci query AI..." style="flex:1; padding:6px; border-radius:4px; border:1px solid #ccc;">
    <button id="aiSearchBtn" style="padding:6px 12px; border:none; border-radius:4px; background-color:#007BFF; color:white; cursor:pointer;">Cerca</button>
    <button id="resetSearchBtn" style="padding:6px 12px; border:none; border-radius:4px; background-color:#6c757d; color:white; cursor:pointer;">Reset</button>
</div>
</section>

<!-- Lista documenti -->
<!-- Lista documenti -->
<section>
<h2 style="display:flex; align-items:center; gap:12px;">
    Documenti elaborati
    <span id="activeFiltersBadge" class="active-filters-badge" style="display:none;"></span>
</h2>

<div class="table-filters">
    <div class="filter-group filter-group-tipo">
        <label>Tipo documento</label>
        <div id="filterTipoDocumentoDropdown" class="multi-dropdown">
            <button type="button" id="filterTipoDocumentoToggle" class="multi-dropdown-toggle">
                Seleziona tipo documento
            </button>
            <div id="filterTipoDocumentoBox" class="multi-checkbox-box" style="display:none;">
                <div class="multi-checkbox-actions">
                <button type="button" id="selectAllTipiBtn" class="mini-filter-btn">Tutti</button>
                <button type="button" id="clearAllTipiBtn" class="mini-filter-btn">Nessuno</button>
            </div>
            <div id="filterTipoDocumentoList"></div>
        </div>
        </div>
    </div>

    <div class="filter-group">
        <label for="filterDateFrom">Data da</label>
        <input type="date" id="filterDateFrom">
    </div>

    <div class="filter-group">
        <label for="filterDateTo">Data a</label>
        <input type="date" id="filterDateTo">
    </div>

    <div class="filter-group filter-actions">
        <label>&nbsp;</label>
        <div style="display:flex; gap:10px;">
            <button id="clearTableFilters" type="button">Pulisci filtri</button>
            <button id="resetAllFiltersBtn" type="button">Reset tutto</button>
        </div>
    </div>

</div>


<table id="doc_table" class="display" style="width:100%">
<thead>
<tr>
<th id="th-select-all" style="text-align:center;">
    <input type="checkbox" id="selectAllDocs" title="Seleziona/Deseleziona tutti">
</th>
<th>ID</th>
<th>Tipo Documento</th>
<th>File</th>
<th>Data Creazione</th>
<th>Dettagli</th>
<th>Azioni</th>
</tr>
</thead>
<tbody></tbody>
</table>
</section>

<!-- Grafici -->
<section style="margin-top:30px;">
    <h2>Dashboard Statistiche</h2>

    <div class="chart-box">
        <canvas id="chartTipo"></canvas>
    </div>

    <div class="chart-box">
        <canvas id="chartDate"></canvas>
    </div>
</section>

<!-- Modal -->
<div id="modal" style="display:none; position:fixed; z-index:10; left:0; top:0; width:100%; height:100%; overflow:auto; background-color: rgba(0,0,0,0.5);">
    <div style="background-color:#fff; margin:5% auto; padding:20px; border-radius:10px; width:80%; max-width:700px; max-height:80%; overflow-y:auto; box-shadow:0 4px 12px rgba(0,0,0,0.2);">
        <span id="close" style="float:right; font-size:24px; font-weight:bold; cursor:pointer;">&times;</span>
        <div id="modal_body"></div>
    </div>
</div>

<link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
<link rel="stylesheet" href="https://cdn.datatables.net/buttons/2.4.1/css/buttons.dataTables.min.css">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
<script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.1/js/dataTables.buttons.min.js"></script>
<script src="https://cdn.datatables.net/buttons/2.4.1/js/buttons.html5.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/jszip/3.10.1/jszip.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/pdfmake/0.2.7/pdfmake.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/pdfmake/0.2.7/vfs_fonts.js"></script>

<style>

.table-filters.active {
    border: 1px solid #007BFF;
    background: #f8fbff;
}

.table-filters {
    display: flex;
    flex-wrap: wrap;
    gap: 20px;
    align-items: flex-end;
    margin-bottom: 20px;
    padding: 16px 18px;
    background: #ffffff;
    border: 1px solid #e3edf7;
    border-radius: 14px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.06);
}
#filterDateFrom,
#filterDateTo {
    min-width: 160px;
}
.filter-group {
    display: flex;
    flex-direction: column;
    gap: 6px;
    min-width: 180px;
}

.filter-group-tipo {
    min-width: 280px;
    position: relative;
}

.filter-group label {
    font-size: 13px;
    font-weight: 600;
    color: #374151;
}

.filter-group input {
    height: 38px;
    padding: 6px 10px;
    border: 1px solid #cfd8e3;
    border-radius: 8px;
    background: #fff;
    font-size: 14px;
}

.multi-dropdown {
    position: relative;
    width: 100%;
}

.multi-dropdown-toggle {
    width: 100%;
    height: 38px;
    padding: 6px 34px 6px 12px;
    border: 1px solid #cfd8e3;
    border-radius: 8px;
    background: #fff;
    font-size: 14px;
    text-align: left;
    cursor: pointer;
    position: relative;
    color: #111827;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.multi-dropdown-toggle:hover {
    border-color: #93c5fd;
    background: #f8fbff;
}

.multi-dropdown-toggle::after {
    content: "▾";
    position: absolute;
    right: 12px;
    top: 50%;
    transform: translateY(-50%);
    color: #6b7280;
    font-size: 12px;
}

.multi-dropdown.open .multi-dropdown-toggle::after {
    content: "▴";
}

.multi-checkbox-box {
    position: absolute;
    top: calc(100% + 6px);
    left: 0;
    width: 100%;
    min-width: 280px;
    max-height: 260px;
    overflow-y: auto;
    border: 1px solid #d7e1ec;
    border-radius: 10px;
    background: #fff;
    padding: 10px;
    box-shadow: 0 10px 24px rgba(0,0,0,0.14);
    z-index: 1000;
}

.multi-checkbox-actions {
    position: sticky;
    top: -10px;
    background: #fff;
    display: flex;
    gap: 8px;
    margin: -10px -10px 10px -10px;
    padding: 10px;
    border-bottom: 1px solid #e5e7eb;
    z-index: 2;
}

.mini-filter-btn {
    height: 30px;
    padding: 0 12px;
    border: 1px solid #cfd8e3;
    border-radius: 7px;
    background: #f8fafc;
    color: #374151;
    cursor: pointer;
    font-size: 12px;
    font-weight: 600;
}

.mini-filter-btn:hover {
    background: #eef4ff;
    border-color: #b8c8e6;
}

.multi-checkbox-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 4px;
    margin-bottom: 2px;
    font-size: 14px;
    border-radius: 6px;
}

.multi-checkbox-item:hover {
    background: #f8fbff;
}

.multi-checkbox-item input[type="checkbox"] {
    accent-color: #007BFF;
}

.filter-actions {
    min-width: auto;
}
.filter-actions {
    margin-left: auto;
    display: flex;
    gap: 10px;

    padding-left: 20px;
    border-left: 1px solid #e5e7eb;
}

#clearTableFilters,
#resetAllFiltersBtn {
    height: 38px;
    padding: 0 14px;
    border: none;
    border-radius: 8px;
    color: #fff;
    cursor: pointer;
    font-weight: 600;
}

#clearTableFilters {
    background: #6b7280;
}

#clearTableFilters:hover {
    background: #4b5563;
}

#resetAllFiltersBtn {
    background: #fff;
    color: #dc3545;
    border: 1px solid #dc3545;
}

#resetAllFiltersBtn:hover {
    background: #fff5f5;
    border-color: #b91c1c;
    color: #b91c1c;
}

#resetAllFiltersBtn:disabled {
    background: #a0a7af;
    cursor: wait;
    opacity: 0.75;
}

#resetAllFiltersBtn.resetting {
    position: relative;
    pointer-events: none;
}

#resetAllFiltersBtn.resetting::after {
    content: "";
    display: inline-block;
    width: 12px;
    height: 12px;
    margin-left: 8px;
    border: 2px solid rgba(255,255,255,0.7);
    border-top-color: #ffffff;
    border-radius: 50%;
    animation: btnSpin 0.8s linear infinite;
    vertical-align: middle;
}

.filter-pill {
    background: #eef4ff;
    border: 1px solid #cfe0ff;
    border-radius: 999px;
    padding: 4px 10px;
    font-size: 12px;
    font-weight: 600;
    color: #1d4ed8;
    display: inline-flex;
    align-items: center;
    gap: 6px;
    line-height: 1.2;
}

.filter-pill.removeable {
    cursor: pointer;
    transition: all 0.2s ease;
}

.filter-pill.removeable:hover {
    background: #dfeeff;
    border-color: #b9d2ff;
}

.filter-pill .pill-close {
    font-weight: 700;
    font-size: 13px;
    opacity: 0.7;
}

.active-filters-badge {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
    background: transparent;
    border: none;
    padding: 0;
    margin-left: 8px;
}

.active-filters-badge strong {
    margin-right: 2px;
    color: #4b5563;
}

.dt-select-col {
    text-align: center !important;
}

#th-select-all {
    width: 42px !important;
    min-width: 42px !important;
    text-align: center !important;
    vertical-align: middle !important;
    padding: 8px 4px !important;
}

#selectAllDocs {
    display: inline-block !important;
    width: 16px;
    height: 16px;
    cursor: pointer;
    accent-color: #007BFF;
    vertical-align: middle;
}

.btn-download-selected.hidden-download-btn {
    display: none !important;
}

.btn-download-selected {
    background-color: #28a745 !important;
    color: white !important;
    border-radius: 4px !important;
    border: none !important;
}

.table-bottom {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    flex-wrap: wrap;
    margin-top: 14px;
    padding-top: 10px;
}

.table-bottom-left,
.table-bottom-center,
.table-bottom-right {
    display: flex;
    align-items: center;
    gap: 12px;
}

.dataTables_wrapper .dataTables_info,
.dataTables_wrapper .dataTables_length,
.dataTables_wrapper .dataTables_paginate,
.dataTables_wrapper .dt-buttons {
    float: none !important;
    margin: 0 !important;
    padding: 0 !important;
}

.table-bottom-right {
    margin-left: auto;
    flex-wrap: wrap;
    justify-content: flex-end;
}

.dataTables_wrapper .dataTables_length label {
    display: flex;
    align-items: center;
    gap: 8px;
    white-space: nowrap;
    font-weight: 500;
}

.dataTables_wrapper .dataTables_length select {
    min-width: 60px;
    height: 32px;
    padding: 4px 8px;
    border: 1px solid #cfd8e3;
    border-radius: 6px;
    background: #fff;
    vertical-align: middle;
}

.dataTables_wrapper .dataTables_info {
    white-space: nowrap;
    font-weight: 500;
}

.dataTables_wrapper .dataTables_paginate {
    display: flex;
    align-items: center;
    gap: 6px;
}

.dataTables_wrapper .dt-buttons {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
}

.chart-box {
    position: relative;
    width: 100%;
    height: 320px;
    margin-bottom: 20px;
    background: #fff;
    border-radius: 10px;
    padding: 10px;
    box-sizing: border-box;
}

.chart-box canvas {
    width: 100% !important;
    height: 100% !important;
}

.btn-delete.deleting {
    background: linear-gradient(90deg, #ff8a8a, #dc3545);
    opacity: 0.85 !important;
    cursor: wait !important;
    position: relative;
    pointer-events: none;
}

.btn-delete.deleting::after {
    content: "";
    display: inline-block;
    width: 12px;
    height: 12px;
    margin-left: 8px;
    border: 2px solid rgba(255,255,255,0.7);
    border-top-color: #ffffff;
    border-radius: 50%;
    animation: btnSpin 0.8s linear infinite;
    vertical-align: middle;
}

@keyframes btnSpin {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
}

@keyframes moveWave {
    0% { background-position: 0 0; }
    100% { background-position: 50px 0; }
}

.file-item {
    margin-bottom: 15px;
}

.file-progress {
    background: #e9ecef;
    border-radius: 8px;
    overflow: hidden;
    width: 100%;
    height: 8px;
    margin-top: 6px;
    box-shadow: inset 0 1px 2px rgba(0,0,0,0.08);
}

.file-progress div {
    width: 0%;
    height: 100%;
    transition: width 0.4s ease, background-color 0.3s ease;
    border-radius: 8px;
}

.file-progress div.uploading {
    background: linear-gradient(90deg, #4da3ff, #007BFF);
}

.file-progress div.phase-preparation {
    background: linear-gradient(90deg, #66b3ff, #3399ff);
}

.file-progress div.phase-ocr {
    background: linear-gradient(90deg, #7c4dff, #5e35b1);
}

.file-progress div.phase-save {
    background: linear-gradient(90deg, #00bcd4, #0097a7);
}

.file-progress div.phase-final {
    background: linear-gradient(90deg, #ffb74d, #fb8c00);
}

.file-progress div.completed {
    background: linear-gradient(90deg, #34c759, #28a745);
}

.file-progress div.error {
    background: linear-gradient(90deg, #ff6b6b, #dc3545);
}

#drop-area {
    border: 2px dashed #007BFF;
    background-color: #e6f0ff;
    color: #000000;
    padding: 25px;
    border-radius: 8px;
    text-align: center;
    cursor: pointer;
    transition: background-color 0.3s ease, border-color 0.3s ease, color 0.3s ease;
    font-weight: bold;
    font-size: 16px;
}

#drop-area:hover {
    background-color: #cce0ff;
    border-color: #0056b3;
    color: #0056b3;
}

#drop-area.dragover {
    background-color: #007BFF;
    border-color: #004080;
    color: #ffffff;
}

.doc-loader span {
    opacity: 0.2;
    animation: blink 1s infinite;
}

.doc-loader span:nth-child(1) { animation-delay: 0s; }
.doc-loader span:nth-child(2) { animation-delay: 0.2s; }
.doc-loader span:nth-child(3) { animation-delay: 0.4s; }
.doc-loader span:nth-child(4) { animation-delay: 0.6s; }

@keyframes blink {
    0%, 100% { opacity: 0.2; }
    50% { opacity: 1; }
}

.kpi-card {
    flex: 1;
    background: #ffffff;
    color: #1f2937;
    padding: 16px;
    border-radius: 12px;
    border: 1px solid #e5edf6;
    box-shadow: 0 4px 12px rgba(0,0,0,0.06);
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    justify-content: center;
    transition: transform 0.2s, box-shadow 0.2s;
    min-height: 92px;
    position: relative;
}


.kpi-card:hover {
    transform: translateY(-3px);
    box-shadow: 0 6px 18px rgba(0,0,0,0.10);
}

.kpi-card::before {
    content: "";
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 4px;
    border-radius: 12px 12px 0 0;
}
.kpi-total::before {
    background: #007BFF;
}
.kpi-today::before {
    background: #28a745;
}
.kpi-type::before {
    background: #7c4dff;
}
.kpi-last::before {
    background: #6b7280;
}
.kpi-card .kpi-title {
    font-size: 14px;
    color: #4b5563;
    font-weight: 600;
    margin-bottom: 8px;
}

.kpi-card .kpi-value {
    font-size: 22px;
    font-weight: 700;
    color: #111827;
    line-height: 1.2;
}

#doc_table tbody tr:hover {
    background: #f5faff !important;
    transition: background 0.2s ease;
}

#doc_table tbody td {
    transition: background 0.2s ease;
}
.btn-primary,
.btn-delete {
    transition: background-color 0.2s ease, transform 0.15s ease, box-shadow 0.2s ease;
}

.btn-primary:hover {
    background-color: #0056b3 !important;
    box-shadow: 0 4px 10px rgba(0,123,255,0.18);
}

.btn-delete:hover {
    background-color: #b91c1c !important;
    box-shadow: 0 4px 10px rgba(220,53,69,0.18);
}

.btn-primary:active,
.btn-delete:active {
    transform: translateY(1px);
}
.search-badge {
    padding: 4px 10px;
    border-radius: 20px;
    font-size: 13px;
    font-weight: bold;
    display: inline-flex;
    align-items: center;
    gap: 5px;
}

.search-badge.structured {
    background: #e6f0ff;
    color: #007BFF;
}

.search-badge.semantic {
    background: #e9f9ee;
    color: #28a745;
}

.search-badge.empty {
    background: #ffecec;
    color: #dc3545;
}

@media (max-width: 1100px) {
    .table-filters {
        align-items: stretch;
    }

    .filter-group,
    .filter-group-tipo {
        min-width: 220px;
        flex: 1 1 220px;
    }
}

@media (max-width: 700px) {
    .table-filters {
        padding: 14px;
    }

    .filter-group,
    .filter-group-tipo {
        min-width: 100%;
        flex: 1 1 100%;
    }

    .multi-checkbox-box {
        min-width: 100%;
    }
}

</style>

<script>
function hasActiveTableFilters(){
    return (
        $('.filterTipoDocumentoChk:checked').length > 0 ||
        !!$('#filterDateFrom').val() ||
        !!$('#filterDateTo').val()
    );
}

function getEmptyTableMessage(){
    const totalDocs = totalDocumentsInDb;
    const hasFilters = hasActiveTableFilters();
    const hasAiQuery = !!($('#aiQuery').val() || '').trim();

    if(totalDocs === 0 && !hasFilters && !hasAiQuery){
        return `
            <div style="
                color:#6b7280;
                font-weight:600;
                font-size:16px;
                text-align:center;
                padding:28px 20px;
                font-family:'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                display:flex;
                flex-direction:column;
                align-items:center;
                justify-content:center;
                gap:8px;
            ">
                <div style="font-size:24px;">📂</div>
                <div style="color:#111827;">Nessun documento presente</div>
                <div style="font-size:13px; font-weight:400; color:#6b7280;">
                    Carica il primo file per iniziare.
                </div>
            </div>
        `;
    }

    if(hasFilters && !hasAiQuery){
        return `
            <div style="
                color:#dc3545;
                font-weight:600;
                font-size:16px;
                text-align:center;
                padding:28px 20px;
                font-family:'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                display:flex;
                flex-direction:column;
                align-items:center;
                justify-content:center;
                gap:8px;
            ">
                <div style="font-size:24px;">🔎</div>
                <div>Nessun risultato con i filtri attivi</div>
                <div style="font-size:13px; font-weight:400; color:#6b7280;">
                    Prova a rimuovere o modificare i filtri.
                </div>
                <button
                    id="emptyStateClearFiltersBtn"
                    type="button"
                    style="
                        margin-top:6px;
                        height:36px;
                        padding:0 14px;
                        border:none;
                        border-radius:8px;
                        background:#6b7280;
                        color:#fff;
                        cursor:pointer;
                        font-weight:600;
                    "
                >
                    Rimuovi filtri
                </button>
            </div>
        `;
    }

    if(hasAiQuery){
        return `
            <div style="
                color:#dc3545;
                font-weight:600;
                font-size:16px;
                text-align:center;
                padding:28px 20px;
                font-family:'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                display:flex;
                flex-direction:column;
                align-items:center;
                justify-content:center;
                gap:8px;
            ">
                <div style="font-size:24px;">🤖</div>
                <div>Nessun risultato per la ricerca AI</div>
                <div style="font-size:13px; font-weight:400; color:#6b7280;">
                    Prova a cambiare query oppure a fare reset della ricerca.
                </div>
            </div>
        `;
    }

    return `
        <div style="
            color:#6b7280;
            font-weight:600;
            font-size:16px;
            text-align:center;
            padding:28px 20px;
            font-family:'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        ">
            Nessun dato disponibile
        </div>
    `;
}
function refreshEmptyTableMessage(){
    const msg = getEmptyTableMessage();
    const settings = table.settings()[0];

    settings.oLanguage.sEmptyTable = msg;
    settings.oLanguage.sZeroRecords = msg;

    // opzionale, per compatibilità
    settings.oLanguage.emptyTable = msg;
    settings.oLanguage.zeroRecords = msg;
}

function updateActiveFiltersBadge(){
    const tipi = getSelectedTipiDocumento();
    const dateFrom = $('#filterDateFrom').val();
    const dateTo = $('#filterDateTo').val();
    const aiQuery = ($('#aiQuery').val() || '').trim();
    
    const pills = [];

    tipi.forEach(tipo => {
        pills.push(`
            <span class="filter-pill removeable" data-filter="tipo" data-value="${tipo}">
                Tipo: ${tipo}
                <span class="pill-close">×</span>
            </span>
        `);
    });

    if(dateFrom){
        pills.push(`
            <span class="filter-pill removeable" data-filter="dateFrom">
                Dal: ${formatDate(dateFrom)}
                <span class="pill-close">×</span>
            </span>
        `);
    }

    if(dateTo){
        pills.push(`
            <span class="filter-pill removeable" data-filter="dateTo">
                Al: ${formatDate(dateTo)}
                <span class="pill-close">×</span>
            </span>
        `);
    }

    if(aiQuery){
        pills.push(`
            <span class="filter-pill removeable" data-filter="aiQuery">
                AI: ${aiQuery}
                <span class="pill-close">×</span>
            </span>
        `);
    }
    const badge = $('#activeFiltersBadge');

    if(pills.length === 0){
        badge.hide();
        badge.html("");
        return;
    }

    badge.html(`<strong>🔎</strong> ${pills.join('')}`);
    badge.show();
    
    const hasFilters =
    tipi.length > 0 ||
    !!dateFrom ||
    !!dateTo ||
    !!aiQuery;
    const filterBox = $('.table-filters');

    if(hasFilters){
        filterBox.addClass('active');
    } else {
        filterBox.removeClass('active');
    }
}

function formatDate(dateStr){
    if(!dateStr) return "";
    const [y, m, d] = dateStr.split("-");
    return `${d}/${m}`;
}
function getProgressClassByPhase(phase, status){
    if(status === "error") return "error";
    if(status === "completed") return "completed";

    switch(phase){
        case "start":
        case "queued":
        case "recovered":
            return "uploading";

        case "Preparazione documento":
            return "phase-preparation";

        case "Estrazione dati (OCR)":
            return "phase-ocr";

        case "Generazione output":
            return "phase-save";

        case "Finalizzazione":
            return "phase-final";

        default:
            return "uploading";
    }
}


function getBaseName(path){
    if(!path) return "";
    return path.split(/[\\\\/]/).pop();
}
function findFileLi(fileName){
    return Array.from(document.querySelectorAll("#fileList li")).find(
        li => li.dataset.name === fileName
    ) || null;
}

function safeSetTimeoutRemove(li, delayMs){
    setTimeout(() => {
        if(li && li.parentNode){
            li.remove();
        }
    }, delayMs);
}

let table;
let chartTipo, chartDate;
let syncRequestId = 0;
let syncInProgress = false;
let totalDocumentsInDb = 0;
function renderStats(stats){

    const tipoEntries = Object.entries(stats.tipo_count || {})
        .sort((a, b) => b[1] - a[1]);   // alto -> basso

    const tipoLabels = tipoEntries.map(item => item[0]);
    const tipoData = tipoEntries.map(item => item[1]);

    const dateLabels = Object.keys(stats.date_count || {}).sort();
    const dateData = dateLabels.map(d => stats.date_count[d]);

    if(chartTipo){ chartTipo.destroy(); }
    chartTipo = new Chart(document.getElementById('chartTipo').getContext('2d'), {
        type: 'bar',
        data: {
            labels: tipoLabels,
            datasets: [{
                label: 'Documenti per Tipo',
                data: tipoData,
                backgroundColor: '#007BFF'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } }
        }
    });

    if(chartDate){ chartDate.destroy(); }
    chartDate = new Chart(document.getElementById('chartDate').getContext('2d'), {
        type: 'line',
        data: {
            labels: dateLabels,
            datasets: [{
                label: 'Upload giornalieri',
                data: dateData,
                borderColor: '#28a745',
                backgroundColor: 'rgba(40,167,69,0.2)',
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: true } }
        }
    });

    const totalDocs = Object.values(stats.tipo_count || {}).reduce((a, b) => a + b, 0);
    $("#kpi_total").text(totalDocs);

    const today = new Date().toISOString().slice(0,10);
    $("#kpi_today").text((stats.date_count && stats.date_count[today]) || 0);

    let maxTipo = "-", maxVal = 0;
    for(const [k, v] of Object.entries(stats.tipo_count || {})){
        if(v > maxVal){
            maxVal = v;
            maxTipo = k;
        }
    }

    $("#kpi_tipo").text(maxTipo);
    $("#kpi_last").text(stats.last_upload || "-");
}

async function updateCharts(){
    try{
        const res = await fetch('/stats/?t=' + Date.now(), { cache: "no-store" });
        const stats = await res.json();
        renderStats(stats);
    } catch(err){
        console.error("Errore updateCharts:", err);
    }
}

async function syncTableAndStats(){
    const requestId = ++syncRequestId;

    try{
        syncInProgress = true;

        const res = await fetch('/documents_snapshot/?t=' + Date.now(), {
            cache: "no-store"
        });

        const payload = await res.json();
        const docs = payload.docs || [];
        const stats = payload.stats || {};
        totalDocumentsInDb = docs.length;
        
        if(requestId !== syncRequestId) return;

        table.clear();

        docs.forEach(doc => {
            const fileName = getBaseName(doc.file_path);
            table.row.add([
                `<input type="checkbox" class="selectDoc" value="${fileName}">`,
                doc.id,
                doc.tipo_documento,
                fileName,
                doc.data_creazione,
                `<button onclick='showDetails(${doc.id})' class="btn-primary">Mostra</button>`,
                `<button onclick='deleteDocument(this, ${doc.id}, "${fileName}")' class="btn-delete">Elimina</button>`
            ]);
        });
        refreshEmptyTableMessage();
        table.draw(false);
        updateDownloadSelectedButtonVisibility();
        updateSelectAllCheckboxState();
        populateTipoDocumentoFilterFromTable();
        updateActiveFiltersBadge();

        if(requestId !== syncRequestId) return;

        const selectedTipi = getSelectedTipiDocumento();

        const hasTableFilters =
            selectedTipi.length > 0 ||
            !!($('#filterDateFrom').val() || '').trim() ||
            !!($('#filterDateTo').val() || '').trim();

        const hasAiSearch = !!($('#aiQuery').val() || '').trim();

        if(hasTableFilters || hasAiSearch){
            updateDashboardFromTable();
        } else {
            renderStats(stats);
        }

    } catch(err){
        console.error("Errore syncTableAndStats:", err);
    } finally {
        if(requestId === syncRequestId){
            syncInProgress = false;
        }
    }
}

function setDeleteButtonLoading(btn, isLoading){
    if(!btn) return;

    if(isLoading){
        btn.dataset.originalText = btn.innerHTML;
        btn.innerHTML = "Eliminazione";
        btn.classList.add("deleting");
        btn.disabled = true;
    } else {
        btn.innerHTML = btn.dataset.originalText || "Elimina";
        btn.classList.remove("deleting");
        btn.disabled = false;
    }
}
function updateDownloadSelectedButtonVisibility(){
    const selectedCount = $('.selectDoc:checked').length;
    const btn = $('.btn-download-selected');

    if(selectedCount > 0){
        btn.removeClass('hidden-download-btn');
    } else {
        btn.addClass('hidden-download-btn');
    }
}
function updateSelectAllCheckboxState(){
    const checkboxes = $('.selectDoc');
    const checked = $('.selectDoc:checked');

    const selectAll = $('#selectAllDocs');

    if(checkboxes.length === 0){
        selectAll.prop('checked', false);
        selectAll.prop('indeterminate', false);
        return;
    }

    if(checked.length === 0){
        selectAll.prop('checked', false);
        selectAll.prop('indeterminate', false);
    }
    else if(checked.length === checkboxes.length){
        selectAll.prop('checked', true);
        selectAll.prop('indeterminate', false);
    }
    else{
        selectAll.prop('checked', false);
        selectAll.prop('indeterminate', true);
    }
}

function populateTipoDocumentoFilterFromTable(){
    const list = $('#filterTipoDocumentoList');
    const currentValues = getSelectedTipiDocumento();

    const tipiMap = new Map();

    table.rows().every(function(){
        const row = this.data();
        const tipoRaw = row[2];

        if(tipoRaw){
            const clean = String(tipoRaw).trim();
            const key = clean.toLowerCase();

            if(!tipiMap.has(key)){
                tipiMap.set(key, clean);
            }
        }
    });

    const sortedTipi = Array.from(tipiMap.values()).sort((a, b) => a.localeCompare(b, 'it'));

    list.empty();

    sortedTipi.forEach(tipo => {
        list.append(`
            <label class="multi-checkbox-item">
                <input type="checkbox" class="filterTipoDocumentoChk" value="${tipo}">
                <span>${tipo}</span>
            </label>
        `);
    });

    setSelectedTipiDocumento(
        currentValues.filter(v =>
            sortedTipi.some(x => x.toLowerCase() === String(v).toLowerCase())
        )
    );

    updateTipoDocumentoToggleLabel();
}

function updateTipoDocumentoToggleLabel(){
    const selected = getSelectedTipiDocumento();
    const btn = $('#filterTipoDocumentoToggle');

    if(selected.length === 0){
        btn.text('Seleziona tipo documento');
    }
    else if(selected.length === 1){
        btn.text(selected[0]);
    }
    else{
        btn.text(`${selected.length} tipi selezionati`);
    }
}

function getSelectedTipiDocumento(){
    return $('.filterTipoDocumentoChk:checked').map(function(){
        return $(this).val();
    }).get();
}

function setSelectedTipiDocumento(values){
    const selected = Array.isArray(values) ? values : [];
    $('.filterTipoDocumentoChk').prop('checked', false);

    selected.forEach(val => {
        $(`.filterTipoDocumentoChk[value="${val}"]`).prop('checked', true);
    });

    updateTipoDocumentoToggleLabel();
}
function selectAllTipiDocumento(){
    $('.filterTipoDocumentoChk').prop('checked', true);
    updateTipoDocumentoToggleLabel();
}

function clearAllTipiDocumento(){
    $('.filterTipoDocumentoChk').prop('checked', false);
    updateTipoDocumentoToggleLabel();
}
function normalizeDateOnly(value){
    if(!value) return "";
    return String(value).substring(0, 10);
}

function registerCustomTableFilters(){
    $.fn.dataTable.ext.search.push(function(settings, data, dataIndex){
        if(settings.nTable.id !== 'doc_table'){
            return true;
        }

        const selectedTipi = getSelectedTipiDocumento()
            .map(x => String(x).trim().toLowerCase());

        const dateFrom = $('#filterDateFrom').val();
        const dateTo = $('#filterDateTo').val();

        const tipoDocumento = (data[2] || "").trim().toLowerCase();
        const dataCreazioneRaw = data[4] || "";
        const dataCreazione = normalizeDateOnly(dataCreazioneRaw);

        if(selectedTipi.length > 0 && !selectedTipi.includes(tipoDocumento)){
            return false;
        }

        if(dateFrom && dataCreazione < dateFrom){
            return false;
        }

        if(dateTo && dataCreazione > dateTo){
            return false;
        }

        return true;
    });
}
const STORAGE_KEYS = {
    tableFilters: "docflow_table_filters",
    aiQuery: "docflow_ai_query",
    aiMode: "docflow_ai_mode"
};

function saveTableFilters(){
    const payload = {
        tipo: getSelectedTipiDocumento(),
        dateFrom: $('#filterDateFrom').val() || "",
        dateTo: $('#filterDateTo').val() || ""
    };
    localStorage.setItem(STORAGE_KEYS.tableFilters, JSON.stringify(payload));
}

function loadTableFilters(){
    try{
        return JSON.parse(localStorage.getItem(STORAGE_KEYS.tableFilters) || "{}");
    } catch(e){
        return {};
    }
}

function clearTableFiltersStorage(){
    localStorage.removeItem(STORAGE_KEYS.tableFilters);
}

function saveAiSearchState(query, mode){
    localStorage.setItem(STORAGE_KEYS.aiQuery, query || "");
    localStorage.setItem(STORAGE_KEYS.aiMode, mode || "");
}

function loadAiSearchState(){
    return {
        query: localStorage.getItem(STORAGE_KEYS.aiQuery) || "",
        mode: localStorage.getItem(STORAGE_KEYS.aiMode) || ""
    };
}

function clearAiSearchState(){
    localStorage.removeItem(STORAGE_KEYS.aiQuery);
    localStorage.removeItem(STORAGE_KEYS.aiMode);
}
async function reloadFullDataAfterReset(){
    const res = await fetch('/documents_snapshot/?t=' + Date.now(), {
        cache: "no-store"
    });

    const payload = await res.json();
    const docs = payload.docs || [];
    const stats = payload.stats || {};
    totalDocumentsInDb = docs.length;
    
    // reset totale DataTable
    table.search('');
    table.columns().search('');
    table.order([[1, "desc"]]);
    table.page.len(10);
    table.page('first');
    table.clear();

    docs.forEach(doc => {
        const fileName = getBaseName(doc.file_path);
        table.row.add([
            `<input type="checkbox" class="selectDoc" value="${fileName}">`,
            doc.id,
            doc.tipo_documento,
            fileName,
            doc.data_creazione,
            `<button onclick='showDetails(${doc.id})' class="btn-primary">Mostra</button>`,
            `<button onclick='deleteDocument(this, ${doc.id}, "${fileName}")' class="btn-delete">Elimina</button>`
        ]);
    });

    table.draw(true);

    updateDownloadSelectedButtonVisibility();
    updateSelectAllCheckboxState();
    populateTipoDocumentoFilterFromTable();
    updateActiveFiltersBadge();

    // dashboard e KPI dal backend
    renderStats(stats);
}
async function resetAllFilters(){
    const btn = $('#resetAllFiltersBtn');

    if(btn.prop('disabled')) return;

    btn
        .prop('disabled', true)
        .addClass('resetting')
        .text('Reset completo...');

    try {
        setSelectedTipiDocumento([]);
        $('#filterTipoDocumentoDropdown').removeClass('open');
        $('#filterTipoDocumentoBox').hide();

        $('#filterDateFrom').val('');
        $('#filterDateTo').val('');
        $('#aiQuery').val('');
        $('#searchStatusBadge').hide().removeClass().text('');
        $("#statusText").text("");

        clearTableFiltersStorage();
        clearAiSearchState();

        table.search('');
        table.columns().search('');
        table.order([[1, "desc"]]);
        table.page.len(10);
        table.page('first');

        updateActiveFiltersBadge();

        await reloadFullDataAfterReset();

    } catch(err){
        console.error("Errore resetAllFilters:", err);
    } finally {
        btn
            .prop('disabled', false)
            .removeClass('resetting')
            .text('Reset completo');
    }
}

async function resetAiSearchOnly(){
    const btn = $('#resetSearchBtn');

    if(btn.prop('disabled')) return;

    btn.prop('disabled', true).text('Reset...');

    try {
        // reset solo ricerca AI
        $('#aiQuery').val('');
        $('#searchStatusBadge').hide().removeClass().text('');
        $("#statusText").text("");

        clearAiSearchState();

        // ricarica tabella completa dal backend
        await syncTableAndStats();

        // se ci sono filtri tabella attivi, riapplicali
        table.draw();

        const hasTableFilters =
            getSelectedTipiDocumento().length > 0 ||
            !!$('#filterDateFrom').val() ||
            !!$('#filterDateTo').val();

        if(hasTableFilters){
            updateDashboardFromTable();
        }
    } catch(err){
        console.error("Errore resetAiSearchOnly:", err);
    } finally {
        btn.prop('disabled', false).text('Reset');
    }
}
async function clearOnlyTableFiltersFromEmptyState(){
    setSelectedTipiDocumento([]);
    $('#filterDateFrom').val('');
    $('#filterDateTo').val('');

    $('#filterTipoDocumentoDropdown').removeClass('open');
    $('#filterTipoDocumentoBox').hide();

    clearTableFiltersStorage();
    updateActiveFiltersBadge();
    refreshEmptyTableMessage();

    await syncTableAndStats();
}
$(document).ready(function(){

    const dropArea = document.getElementById("drop-area");
    const fileElem = document.getElementById("fileElem");
    const fileListUI = document.getElementById("fileList");
    
    registerCustomTableFilters();

 // --- Inizializza DataTable ---
        table = $('#doc_table').DataTable({
    
        order: [[1, "desc"]],
        data: [],
        columns:[
            { title: "", width: "42px", orderable: false, className: "dt-select-col" },
            { title: "ID", width: "50px" },
            { title: "Tipo Documento", width: "250px" },
            { title: "File", width: "300px" },
            { title: "Data Creazione", width: "150px" },
            { title: "Dettagli", width: "100px" },
            { title: "Azioni", width: "120px" }
        ],
        pageLength: 10,
        dom: 'rt<"table-bottom"<"table-bottom-left"i><"table-bottom-center"l><"table-bottom-right"pB>>',
        buttons: [
            {
                text: 'Scarica selezionati',
                className: 'btn-download-selected',
                action: function () {
                    const selected = Array.from($('.selectDoc:checked')).map(cb => cb.value);

                    if(selected.length === 0){
                        alert("Seleziona almeno un documento");
                        return;
                    }

                    const params = new URLSearchParams();
                    selected.forEach(f => params.append("filenames", f));

                    window.location.href = "/download_zip/?" + params.toString();
                }
            },
            { extend: 'csvHtml5', text: 'Esporta CSV', exportOptions: { columns: [1,2,3,4] } },
            { extend: 'excelHtml5', text: 'Esporta Excel', exportOptions: { columns: [1,2,3,4] } },
            { extend: 'pdfHtml5', text: 'Esporta PDF', exportOptions: { columns: [1,2,3,4] } }
        ],
    language: {
        emptyTable: getEmptyTableMessage(),
        zeroRecords: getEmptyTableMessage()
    }

    });
    $('#th-select-all').html('<input type="checkbox" id="selectAllDocs" title="Seleziona/Deseleziona tutti">');
    updateDownloadSelectedButtonVisibility();
    updateSelectAllCheckboxState();
    populateTipoDocumentoFilterFromTable();

    $(document).on('change', '#selectAllDocs', function(){
    const isChecked = $(this).is(':checked');
    $('.selectDoc').prop('checked', isChecked);

    updateDownloadSelectedButtonVisibility();
    updateSelectAllCheckboxState();
    populateTipoDocumentoFilterFromTable();
});

    table.on('draw', function () {
    $('#doc_table button.btn-primary').css({
        'background-color':'#007BFF',
        'color':'white',
        'border':'none',
        'border-radius':'4px',
        'cursor':'pointer',
        'padding':'4px 8px'
    });

    $('#doc_table button.btn-delete').css({
        'background-color':'#dc3545',
        'color':'white',
        'border':'none',
        'border-radius':'4px',
        'cursor':'pointer',
        'padding':'4px 8px'
    });

    updateDownloadSelectedButtonVisibility();
    updateSelectAllCheckboxState();
});

$(document).on('change', '.selectDoc', function(){
    updateDownloadSelectedButtonVisibility();
    updateSelectAllCheckboxState();
});
$(document).on('change', '.filterTipoDocumentoChk, #filterDateFrom, #filterDateTo', function(){
    saveTableFilters();
    updateTipoDocumentoToggleLabel();
    updateActiveFiltersBadge();
    refreshEmptyTableMessage();
    table.draw();
    updateDashboardFromTable();
});


$(document).on('click', '#clearTableFilters', function(){
    setSelectedTipiDocumento([]);
    $('#filterDateFrom').val('');
    $('#filterDateTo').val('');

    $('#filterTipoDocumentoDropdown').removeClass('open');
    $('#filterTipoDocumentoBox').hide();

    clearTableFiltersStorage();
    updateActiveFiltersBadge();
    refreshEmptyTableMessage();
    table.draw();
    updateDashboardFromTable();
});

$(document).on('click', '.filter-pill.removeable', function(){
    const filterName = $(this).data('filter');

    if(filterName === 'tipo'){
        const valueToRemove = String($(this).data('value')).toLowerCase();

        $('.filterTipoDocumentoChk').each(function(){
            if(String($(this).val()).toLowerCase() === valueToRemove){
                $(this).prop('checked', false);
            }
        });
    }
    else if(filterName === 'dateFrom'){
        $('#filterDateFrom').val('');
    }
    else if(filterName === 'dateTo'){
        $('#filterDateTo').val('');
    }
    
    else if(filterName === 'aiQuery'){
        $('#aiQuery').val('');
        $('#searchStatusBadge').hide().removeClass().text('');
        clearAiSearchState();
        syncTableAndStats();
        return;
    }

    saveTableFilters();
    updateActiveFiltersBadge();
    refreshEmptyTableMessage();
    table.draw();
    updateDashboardFromTable();
});

$(document).on('click', '#resetAllFiltersBtn', async function(){
    await resetAllFilters();
});

$(document).on('click', '#filterTipoDocumentoToggle', function(e){
    e.stopPropagation();

    const dropdown = $('#filterTipoDocumentoDropdown');
    const box = $('#filterTipoDocumentoBox');

    dropdown.toggleClass('open');
    box.toggle();
});

$(document).on('click', '#filterTipoDocumentoBox', function(e){
    e.stopPropagation();
});

$(document).on('click', function(){
    $('#filterTipoDocumentoDropdown').removeClass('open');
    $('#filterTipoDocumentoBox').hide();
});
$(document).on('click', '#selectAllTipiBtn', function(e){
    e.stopPropagation();
    selectAllTipiDocumento();
    saveTableFilters();
    updateActiveFiltersBadge();
    table.draw();
    updateDashboardFromTable();
});

$(document).on('click', '#clearAllTipiBtn', function(e){
    e.stopPropagation();
    clearAllTipiDocumento();
    saveTableFilters();
    updateActiveFiltersBadge();
    table.draw();
    updateDashboardFromTable();

    $('#filterTipoDocumentoDropdown').removeClass('open');
    $('#filterTipoDocumentoBox').hide();
});
$(document).on('click', '#emptyStateClearFiltersBtn', async function(){
    await clearOnlyTableFiltersFromEmptyState();
});
async function awaitReloadDocuments(){
    try{
        const res = await fetch('/documents/?t=' + Date.now());
        const docs = await res.json();
        totalDocumentsInDb = docs.length;
        refreshEmptyTableMessage();

        table.clear();

        docs.forEach(doc => {
            const fileName = getBaseName(doc.file_path);
            const row = [
                `<input type="checkbox" class="selectDoc" value="${fileName}">`,
                doc.id,
                doc.tipo_documento,
                fileName,
                doc.data_creazione,
                `<button onclick='showDetails(${doc.id})' class="btn-primary">Mostra</button>`,
                `<button onclick='deleteDocument(this, ${doc.id}, "${fileName}")' class="btn-delete">Elimina</button>`
            ];
            table.row.add(row);
           
        });

        table.draw(true);
        updateDownloadSelectedButtonVisibility();
        updateSelectAllCheckboxState();
        populateTipoDocumentoFilterFromTable();
        totalDocumentsInDb = docs.length;
    } catch(err){
        console.error("Errore refresh documents:", err);
    }
}
    
(async function initPage(){
    try{
        const hasActive = await loadJobsStatus();
        if(hasActive){
            ensureJobsPolling();
        }
    } catch(err){
        console.error("Errore loadJobsStatus:", err);
    }

    try{
        await syncTableAndStats();

        const savedFilters = loadTableFilters();
        setSelectedTipiDocumento(savedFilters.tipo || []);
        $('#filterDateFrom').val(savedFilters.dateFrom || '');
        $('#filterDateTo').val(savedFilters.dateTo || '');

        const savedAi = loadAiSearchState();
        if(savedAi.query){
            $('#aiQuery').val(savedAi.query);
        }

        updateActiveFiltersBadge();
        refreshEmptyTableMessage();
        table.draw();
        updateDashboardFromTable();


    } catch(err){
        console.error("Errore sync iniziale:", err);
    }
})();

    
$("#aiSearchBtn").click(async function(){
    const query = $("#aiQuery").val().trim();
    if(!query) return alert("Inserisci una query per la ricerca AI");

    // --- Mostra loader nella tabella ---
    const settings = table.settings()[0];
    settings.oLanguage.sEmptyTable = `
        <div style="
            color:#007BFF; 
            font-weight:bold; 
            font-size:16px; 
            text-align:center; 
            padding:20px;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            display:flex;
            flex-direction:column;
            align-items:center;
            justify-content:center;
        ">
            <div class="doc-loader" style="
                display:flex;
                gap:8px;
                font-size:24px;
                margin-bottom:10px;
            ">
                <span>📄</span>
                <span>📄</span>
                <span>📄</span>
                <span>📄</span>
            </div>
            Caricamento documenti in corso...
        </div>
    `;
    settings.oLanguage.sZeroRecords = settings.oLanguage.sEmptyTable;
    
    table.clear().draw();   // reset tabella e mostra loader
    $("#statusText").text(""); // niente messaggio sopra

try {
    const res = await fetch('/search_documents/?query=' + encodeURIComponent(query));
    if(!res.ok){
        alert("Errore nella ricerca AI: " + res.statusText);
        return;
    }

    const data = await res.json();
    const results = data.results || [];
    const mode = data.mode || "";
    
    saveAiSearchState(query, mode);
    updateActiveFiltersBadge();

const badge = $("#searchStatusBadge");

if(results.length === 0){
    refreshEmptyTableMessage();
}
else if(mode === "structured"){
    badge
        .removeClass()
        .addClass("search-badge structured")
        .text("🔎 Strutturata")
        .show();
}
else if(mode === "semantic"){
    badge
        .removeClass()
        .addClass("search-badge semantic")
        .text("🤖 Semantica")
        .show();
}
else{
    badge.hide();
}

    table.clear(); // rimuove ogni riga precedente

    if(results.length === 0){
        const settings = table.settings()[0];
        settings.oLanguage.sEmptyTable = `
            <div style="
                color:#dc3545;
                font-weight:600;
                font-size:16px;
                text-align:center;
                padding:28px 20px;
                font-family:'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                display:flex;
                flex-direction:column;
                align-items:center;
                justify-content:center;
                gap:8px;
            ">
                <div style="font-size:24px;">🔎</div>
                <div>Nessun documento trovato</div>
                <div style="font-size:13px; font-weight:400; color:#6b7280;">
                    Prova a modificare la ricerca AI o a rimuovere qualche filtro.
                </div>
            </div>
        `;
        settings.oLanguage.sZeroRecords = settings.oLanguage.sEmptyTable;
    } else {
        // --- Risultati trovati ---
    const settings = table.settings()[0];
    settings.oLanguage.sEmptyTable = "";
    settings.oLanguage.sZeroRecords = "";

        results.forEach(doc => {
            const fileName = getBaseName(doc.file_path);
            const row = [
                `<input type="checkbox" class="selectDoc" value="${fileName}">`,
                doc.id,
                doc.tipo_documento,
                fileName,
                doc.data_creazione,
                `<button onclick='showDetails(${doc.id})' class="btn-primary">Mostra</button>`,
                `<button onclick='deleteDocument(this, ${doc.id}, "${fileName}")' class="btn-delete">Elimina</button>`
            ];
            table.row.add(row);
        });
    }

    table.draw(true);
    updateDownloadSelectedButtonVisibility();
    updateSelectAllCheckboxState();
    populateTipoDocumentoFilterFromTable();
    updateDashboardFromTable();
    updateActiveFiltersBadge();
} catch(err){
        console.error(err);
        alert("Errore nella ricerca AI");
    }
});

    // --- Reset ricerca ---
$("#resetSearchBtn").click(async function(){
    await resetAiSearchOnly();
});

    $("#aiQuery").keypress(function(e){
        if(e.which == 13){ $("#aiSearchBtn").click(); }
    });

    const deletingDocIds = new Set();
    const deletingFileNames = new Set();
    let deleteSyncInProgress = false;
    let jobsPollingTimer = null;
    
let refreshTimer = null;
let refreshInProgress = false;
let refreshPending = false;

async function runRefreshUI(){
    if(refreshInProgress){
        refreshPending = true;
        return;
    }

    refreshInProgress = true;

    try{
        await awaitReloadDocuments();
        await updateCharts();
    } catch(err){
        console.error("Errore refresh UI:", err);
    } finally {
        refreshInProgress = false;

        if(refreshPending){
            refreshPending = false;
            setTimeout(() => {
                runRefreshUI();
            }, 200);
        }
    }
}

function scheduleRefreshUI(){
    clearTimeout(refreshTimer);
    refreshTimer = setTimeout(() => {
        runRefreshUI();
    }, 400);
}

function ensureJobsPolling(){
    if(jobsPollingTimer) return;

    jobsPollingTimer = setInterval(async () => {
        try{
            const hasActive = await loadJobsStatus();

            if(!hasActive){
                clearInterval(jobsPollingTimer);
                jobsPollingTimer = null;
            }
        } catch(err){
            console.error("Errore polling jobs:", err);
        }
    }, 3000);
}

async function loadJobsStatus(){
    const res = await fetch('/jobs_status/');
    const data = await res.json();
    const jobs = data.jobs || [];

    const visibleJobs = jobs.filter(job => {
        if (
            job.status === "queued" ||
            job.status === "processing" ||
            job.status === "error" 
        ) {
            return true;
        }

        if (job.status === "completed" && job.completed_at) {
            const completedMs = new Date(job.completed_at).getTime();
            return (Date.now() - completedMs) < 15000;

        }

        return false;
    });

    visibleJobs.forEach(job => {
        if(!job.file_name) return;

        const jobFileName = getBaseName(job.file_name);
        let li = findFileLi(jobFileName);

if(!li){
    li = document.createElement("li");
    li.dataset.name = jobFileName;
    li.dataset.jobId = job.id;
    li.className = "file-item";
    li.innerHTML = `
        📄 <span class="file-name">${jobFileName}</span>
        <button class="btn-cancel-upload" style="
            margin-left:10px;
            padding:3px 8px;
            border:none;
            border-radius:4px;
            background:#ffc107;
            color:#000;
            cursor:pointer;
            font-size:12px;
        ">Annulla</button>
        <div class="file-progress"><div></div></div>
    `;
    fileListUI.appendChild(li);

    const cancelBtn = li.querySelector(".btn-cancel-upload");
    if(cancelBtn){
        cancelBtn.onclick = async function(){
            cancelBtn.disabled = true;
            cancelBtn.textContent = "Annullamento...";

            try{
                const res = await fetch(`/jobs/${job.id}/cancel`, {
                    method: "POST",
                    cache: "no-store"
                });

                const data = await res.json();

                if(!res.ok || !data.success){
                    cancelBtn.disabled = false;
                    cancelBtn.textContent = "Annulla";
                    alert(data.error || "Errore durante annullamento job");
                }
            } catch(err){
                console.error("Errore annullamento job:", err);
                cancelBtn.disabled = false;
                cancelBtn.textContent = "Annulla";
                alert("Errore durante annullamento job");
            }
        };
    }
}

        const progressDiv = li.querySelector('.file-progress div');
        progressDiv.style.width = (job.progress || 0) + "%";
        progressDiv.style.background = "";

        let statusText = li.querySelector('.status-text');
        if(!statusText){
            statusText = document.createElement('span');
            statusText.className = 'status-text';
            statusText.style.marginLeft = '10px';
            li.appendChild(statusText);
        }

        if(job.status === "error"){
    const errorTime = job.updated_at || job.completed_at;
    let remainingMs = 0;

    if(errorTime){
        const errorDate = new Date(errorTime);
        const elapsedMs = Date.now() - errorDate.getTime();
        remainingMs = Math.max(0, 10000 - elapsedMs);
    }

    if(remainingMs <= 0){
        if(li && li.parentNode){
            li.remove();
        }
        return;
    }

    progressDiv.className = getProgressClassByPhase(job.phase, "error");
    progressDiv.style.width = "100%";
    statusText.textContent = job.error_message || "❌ Errore";
    statusText.style.color = "#dc3545";
    statusText.style.fontWeight = "bold";

    safeSetTimeoutRemove(li, remainingMs);
    
    const cancelBtn = li.querySelector(".btn-cancel-upload");
    if(cancelBtn){
        cancelBtn.remove();
    }
}

else if(job.status === "completed"){
    const completedTime = job.completed_at;
    let remainingMs = 10000;

    if(completedTime){
        const completedDate = new Date(completedTime);
        const elapsedMs = Date.now() - completedDate.getTime();
        remainingMs = Math.max(0, 10000 - elapsedMs);
    }

    if(remainingMs <= 0){
        if(li && li.parentNode){
            li.remove();
        }
        return;
    }

    progressDiv.className = getProgressClassByPhase(job.phase, "completed");
    progressDiv.style.width = "100%";
    statusText.textContent = "✅ Documento pronto";
    statusText.style.color = "#28a745";
    statusText.style.fontWeight = "bold";

    setTimeout(() => {
        const bar = li.querySelector('.file-progress');
        if(bar){
            bar.style.display = "none";
        }
    }, 500);

    safeSetTimeoutRemove(li, remainingMs);
    
    const cancelBtn = li.querySelector(".btn-cancel-upload");
    if(cancelBtn){
        cancelBtn.remove();
    }
}

else if(job.status === "cancelled"){
    progressDiv.className = "error";
    progressDiv.style.width = "100%";
    statusText.textContent = "⛔ Caricamento annullato";
    statusText.style.color = "#dc3545";
    statusText.style.fontWeight = "bold";

    const cancelBtn = li.querySelector(".btn-cancel-upload");
    if(cancelBtn){
        cancelBtn.remove();
    }

    safeSetTimeoutRemove(li, 2500);
}

else {
    progressDiv.className = getProgressClassByPhase(job.phase, job.status);

    const phaseLabels = {
        "queued": "📥 In coda…",
        "recovered": "♻️ Job recuperato dopo riavvio…",
        "start": "📤 Avvio job…",
        "Preparazione documento": "🔄 Preparazione documento…",
        "Estrazione dati (OCR)": "📝 Estrazione dati…",
        "Generazione output": "⚙️ Generazione output…",
        "Finalizzazione": "⏳ Finalizzazione…"
    };

    statusText.textContent = phaseLabels[job.phase] || `⏳ ${job.phase || 'Elaborazione'}…`;
    statusText.style.color = "";
    statusText.style.fontWeight = "";
}
    });
    
    return jobs.some(job => job.status === "queued" || job.status === "processing");
}

    // --- Upload ---


    dropArea.addEventListener("click", () => fileElem.click());

    dropArea.addEventListener("dragover", (e) => { 
        e.preventDefault(); 
        dropArea.classList.add("dragover"); 
    });

    dropArea.addEventListener("dragleave", (e) => { 
        e.preventDefault(); 
        dropArea.classList.remove("dragover"); 
    });

    dropArea.addEventListener("drop", (e) => { 
        e.preventDefault(); 
        dropArea.classList.remove("dragover"); 
        handleFiles(e.dataTransfer.files);
    });

    fileElem.addEventListener("change", (e) => handleFiles(e.target.files));


    function handleFiles(files){
    const newFiles = [];
    for(const file of files){
        // evita duplicati
        if(findFileLi(file.name)) continue;

        const li = document.createElement("li");
        li.dataset.name = file.name;
        li.className = "file-item";
        // creiamo la struttura: icona + nome + barra di caricamento
        li.innerHTML = `
            📄 <span class="file-name">${file.name}</span>
            <button class="btn-cancel-upload" style="
                margin-left:10px;
                padding:3px 8px;
                border:none;
                border-radius:4px;
                background:#ffc107;
                color:#000;
                cursor:pointer;
                font-size:12px;
            ">Annulla</button>
            <div class="file-progress"><div></div></div>
        `;
        fileListUI.appendChild(li);
        newFiles.push(file);
    }
    if(newFiles.length) uploadFiles(newFiles);
}

function uploadFiles(files){
    const formData = new FormData();
    files.forEach(f => formData.append("files", f));

    fetch("/upload_multiple/", { method:"POST", body:formData })
    .then(res => res.json())
    .then(data => {
        data.uploaded.forEach(f => {
            const li = findFileLi(f.file_name);
            if(li){
                li.dataset.jobId = f.job_id;
                li.querySelector(".file-progress div").style.width = "0%";

                const cancelBtn = li.querySelector(".btn-cancel-upload");
                if(cancelBtn){
                    cancelBtn.onclick = async function(){
                        cancelBtn.disabled = true;
                        cancelBtn.textContent = "Annullamento...";

                        try{
                            const res = await fetch(`/jobs/${f.job_id}/cancel`, {
                                method: "POST",
                                cache: "no-store"
                            });

                            const data = await res.json();

                            if(!res.ok || !data.success){
                                cancelBtn.disabled = false;
                                cancelBtn.textContent = "Annulla";
                                alert(data.error || "Errore durante annullamento job");
                            }
                        } catch(err){
                            console.error("Errore annullamento job:", err);
                            cancelBtn.disabled = false;
                            cancelBtn.textContent = "Annulla";
                            alert("Errore durante annullamento job");
                        }
                    };
                }
            }
        });
    });
}

    // --- WebSocket ---
    const ws = new WebSocket("ws://" + window.location.host + "/ws/docs");

    ws.onclose = function(){
        console.warn("WebSocket chiuso, riallineo la UI...");
        scheduleRefreshUI();
    };
    

    ws.onmessage = async function(event){
    const msg = JSON.parse(event.data);
    const li = msg.file_name ? findFileLi(getBaseName(msg.file_name)) : null;

    if(msg.event === "job_queued"){
        let li = findFileLi(getBaseName(msg.file_name));

        if(!li){
            li = document.createElement("li");
            li.dataset.name = getBaseName(msg.file_name);
            li.className = "file-item";
            li.innerHTML = `📄 <span class="file-name">${getBaseName(msg.file_name)}</span> <div class="file-progress"><div></div></div>`;
            fileListUI.appendChild(li);
        }

        const progressDiv = li.querySelector('.file-progress div');
        progressDiv.style.width = "2%";
        progressDiv.className = "uploading";
        progressDiv.style.background = "";

        let statusText = li.querySelector('.status-text');
        if(!statusText){
            statusText = document.createElement('span');
            statusText.className = 'status-text';
            statusText.style.marginLeft = '10px';
            li.appendChild(statusText);
        }

        statusText.textContent = msg.message || "📥 In coda…";
        statusText.style.color = "";
        statusText.style.fontWeight = "";
    }

    else if(msg.event === "job_progress" && li){
        const progressDiv = li.querySelector('.file-progress div');
        progressDiv.style.transition = "width 0.4s ease";
        progressDiv.style.width = (msg.progress || 0) + "%";

        let statusText = li.querySelector('.status-text');
        
        const cancelBtn = li ? li.querySelector(".btn-cancel-upload") : null;

        if(msg.status === "error" || msg.status === "completed" || msg.status === "cancelled"){
            if(cancelBtn){
                cancelBtn.remove();
            }
        }

        if(!statusText){
            statusText = document.createElement('span');
            statusText.className = 'status-text';
            statusText.style.marginLeft = '10px';
            li.appendChild(statusText);
        }

        progressDiv.style.background = "";

        if(msg.status === "error"){
            progressDiv.className = getProgressClassByPhase(msg.phase, "error");
            statusText.textContent = msg.message || "❌ Errore";
            statusText.style.color = "#dc3545";
            statusText.style.fontWeight = "bold";  
            

            setTimeout(() => {
                if(li && li.parentNode){
                    li.remove();
                }
            }, 10000);
        }
        else {
            progressDiv.className = getProgressClassByPhase(msg.phase, msg.status);

            const phaseLabels = {
                "queued": "📥 In coda…",
                "recovered": "♻️ Job recuperato dopo riavvio…",
                "start": "📤 Avvio job…",
                "Preparazione documento": "🔄 Preparazione documento…",
                "Estrazione dati (OCR)": "📝 Estrazione dati…",
                "Generazione output": "⚙️ Generazione output…",
                "Finalizzazione": "⏳ Finalizzazione…"
            };

            statusText.textContent = phaseLabels[msg.phase] || msg.message || "⏳ Elaborazione in corso…";
            statusText.style.color = "";
            statusText.style.fontWeight = "";
        }
    }

    else if(msg.event === "document_ready_fast"){
        const docId = msg.doc.id;
        const fileName = getBaseName(msg.doc.file_path);

        if(deletingDocIds.has(String(docId)) || deletingFileNames.has(String(fileName))){
            return;
        }

        const li = findFileLi(fileName);

        if(li){
            const progressDiv = li.querySelector('.file-progress div');
            progressDiv.style.width = "100%";
            progressDiv.className = "completed";

            let statusText = li.querySelector('.status-text');
            if(!statusText){
                statusText = document.createElement('span');
                statusText.className = 'status-text';
                statusText.style.marginLeft = '10px';
                li.appendChild(statusText);
            }

            statusText.textContent = "✅ Documento pronto";
            statusText.style.color = "#28a745";
            statusText.style.fontWeight = "bold";
            
           const cancelBtn = li.querySelector(".btn-cancel-upload");
                if(cancelBtn){
                cancelBtn.remove();
            }

            li.style.opacity = "1";
            li.style.transition = "opacity 0.5s ease";

            setTimeout(() => {
                const bar = li.querySelector('.file-progress');
                if(bar){
                    bar.style.display = "none";
                }
            }, 500);

            setTimeout(() => {
                if(li){
                    li.style.opacity = "0";
                }
            }, 19500);

            setTimeout(() => {
                if(li && li.parentNode){
                    li.remove();
                }
            }, 20000);
        }

        // QUI NON SI AGGIUNGE NESSUNA RIGA IN TABELLA
    }

    else if(msg.event === "document_ready_full"){
    const doc = msg.doc;
    const fileName = getBaseName(doc.file_path);

    if(deletingDocIds.has(String(doc.id)) || deletingFileNames.has(String(fileName))){
        return;
    }

    await syncTableAndStats();
}

    else if(msg.event === "document_deleted"){
    const li = msg.file_name ? findFileLi(getBaseName(msg.file_name)) : null;

    if(li && li.parentNode){
        li.remove();
    }

    if($('#modal').is(':visible')){
        $('#modal').hide();
    }
}

else if(msg.event === "job_cancelled"){
    const li = msg.file_name ? findFileLi(getBaseName(msg.file_name)) : null;

    if(li){
        const progressDiv = li.querySelector('.file-progress div');
        if(progressDiv){
            progressDiv.className = "error";
            progressDiv.style.width = "100%";
        }

        let statusText = li.querySelector('.status-text');
        if(!statusText){
            statusText = document.createElement('span');
            statusText.className = 'status-text';
            statusText.style.marginLeft = '10px';
            li.appendChild(statusText);
        }

        statusText.textContent = "⛔ Caricamento annullato";
        statusText.style.color = "#dc3545";
        statusText.style.fontWeight = "bold";

        const cancelBtn = li.querySelector(".btn-cancel-upload");
        if(cancelBtn){
            cancelBtn.remove();
        }

        setTimeout(() => {
            if(li && li.parentNode){
                li.remove();
            }
        }, 2500);
    }
}
    };

window.showDetails = function(docId){
    fetch('/documents/' + docId + '?t=' + Date.now())
        .then(r => r.json())
        .then(doc => {
            if(!doc || doc.success === false) return;

            const filename = getBaseName(doc.file_path);
            const fileExt = filename.split('.').pop().toLowerCase();
            const fileUrl = "/uploads/" + encodeURIComponent(filename) + "?t=" + Date.now();

            let html = `<h3>${doc.tipo_documento} - ${filename}</h3>`;

            if(fileExt === 'pdf'){
                html += `<iframe src="${fileUrl}" width="100%" height="400px" style="border:1px solid #ccc;"></iframe>`;
            }
            else if(['png','jpg','jpeg','gif'].includes(fileExt)){
                html += `<img src="${fileUrl}" style="max-width:100%; max-height:400px; display:block; margin-bottom:10px; border:1px solid #ccc;">`;
            }

            html += '<ul>';
            for(const [k, v] of Object.entries(doc.campi)){
                html += `<li><strong>${k}</strong>: ${v}</li>`;
            }
            html += '</ul>';

            html += `<p><a href="/download/${filename}" style="color:#28a745; font-weight:bold;">Scarica Word</a></p>`;

            $('#modal_body').html(html);
            $('#modal').show();
        })
        .catch(err => {
            console.error("Errore showDetails:", err);
            alert("Errore caricamento dettagli documento");
        });
};

    $("#close").click(()=>{ $('#modal').hide(); });
    $(window).click((event)=>{ if(event.target.id=='modal') $('#modal').hide(); });

window.deleteDocument = async function(button, docId, fileName){
    const conferma = confirm("Vuoi davvero eliminare questo documento?");
    if(!conferma) return;

    const btn = button;
    setDeleteButtonLoading(btn, true);

    if($('#modal').is(':visible')){
        $('#modal').hide();
    }

    try{
        const res = await fetch(`/documents/${docId}`, {
            method: "DELETE",
            cache: "no-store"
        });

        const data = await res.json();

        if(!res.ok || !data.success){
            setDeleteButtonLoading(btn, false);
            alert(data.error || "Errore durante l'eliminazione del documento");
            return;
        }

        await syncTableAndStats();

    } catch(err){
        console.error("Errore deleteDocument:", err);
        setDeleteButtonLoading(btn, false);
        await syncTableAndStats();
        alert("Errore durante l'eliminazione del documento");
    }
};
function updateDashboardFromTable(){
    const rowsData = table.rows({ search: 'applied' }).data().toArray();
    const info = table.page.info();

    const tipo_count = {};
    const date_count = {};
    let lastUploadRaw = null;

    rowsData.forEach(row => {
        const rawTipo = row[2] || "-";
        const dataCreazione = row[4] || "";

        const tipoNormalized = String(rawTipo).trim().toLowerCase();

        const tipo =
            tipoNormalized === "-" || tipoNormalized === ""
                ? "-"
                : tipoNormalized.charAt(0).toUpperCase() + tipoNormalized.slice(1);

        tipo_count[tipo] = (tipo_count[tipo] || 0) + 1;

        if(dataCreazione){
            const day = dataCreazione.substring(0, 10);
            date_count[day] = (date_count[day] || 0) + 1;

            if(!lastUploadRaw || dataCreazione > lastUploadRaw){
                lastUploadRaw = dataCreazione;
            }
        }
    });

    const tipoEntries = Object.entries(tipo_count)
        .sort((a, b) => b[1] - a[1]);   // alto -> basso

    const tipoLabels = tipoEntries.map(item => item[0]);
    const tipoData = tipoEntries.map(item => item[1]);


    const dateLabels = Object.keys(date_count).sort();
    const dateData = dateLabels.map(d => date_count[d]);

    if(chartTipo){ chartTipo.destroy(); }
    chartTipo = new Chart(document.getElementById('chartTipo').getContext('2d'), {
        type: 'bar',
        data: {
            labels: tipoLabels,
            datasets: [{
                label: 'Documenti per Tipo',
                data: tipoData,
                backgroundColor: '#007BFF'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    ticks: {
                        maxRotation: 35,
                        minRotation: 20
                    }
                }
            }
        }
    });

    if(chartDate){ chartDate.destroy(); }
    chartDate = new Chart(document.getElementById('chartDate').getContext('2d'), {
        type: 'line',
        data: {
            labels: dateLabels,
            datasets: [{
                label: 'Upload giornalieri',
                data: dateData,
                borderColor: '#28a745',
                backgroundColor: 'rgba(40,167,69,0.2)',
                fill: true
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: true } }
        }
    });

    const totalFiltered = info.recordsDisplay;
    $("#kpi_total").text(totalFiltered);

    const today = new Date().toISOString().slice(0,10);
    $("#kpi_today").text(date_count[today] || 0);

    let maxTipo = "-", maxVal = 0;
    for(const [k, v] of Object.entries(tipo_count)){
        if(v > maxVal){
            maxVal = v;
            maxTipo = k;
        }
    }

    if(totalFiltered === 0){
        $("#kpi_tipo").text("Nessun risultato");
        $("#kpi_last").text("—");
        return;
    }

    $("#kpi_tipo").text(maxTipo);

    if(lastUploadRaw){
        const dt = new Date(lastUploadRaw.replace(" ", "T"));
        if(!isNaN(dt.getTime())){
            const dd = String(dt.getDate()).padStart(2, '0');
            const mm = String(dt.getMonth() + 1).padStart(2, '0');
            const yyyy = dt.getFullYear();
            const hh = String(dt.getHours()).padStart(2, '0');
            const mi = String(dt.getMinutes()).padStart(2, '0');
            const ss = String(dt.getSeconds()).padStart(2, '0');
            $("#kpi_last").text(`${dd}/${mm}/${yyyy}, ${hh}:${mi}:${ss}`);
        } else {
            $("#kpi_last").text(lastUploadRaw);
        }
    } else {
        $("#kpi_last").text("—");
    }
}


    let chartsRequestId = 0;

    
});
</script>
"""

@app.get("/", response_class=HTMLResponse)
def home_page():
    return HTMLResponse(content=html_content)



# -------------------------------
# Lista documenti
# -------------------------------
@app.get("/documents/")
async def list_documents():
    if TEST_FORCE_EMPTY_DB:
        return JSONResponse(
            content=[],
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )

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
    if TEST_FORCE_EMPTY_DB:
        return JSONResponse(
            content={
                "tipo_count": {},
                "date_count": {},
                "last_upload": "-"
            },
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )

    async with async_engine.connect() as conn:
        result = await conn.execute(select(documents))
        rows = result.fetchall()

    tipo_count = {}
    date_count = {}

    for r in rows:
        tipo_count[r.tipo_documento] = tipo_count.get(r.tipo_documento, 0) + 1
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
            stmt = stmt.join(document_fields, documents.c.id == document_fields.c.document_id)
            try:
                numero = float(str(valore).replace("h","").strip())
                if operatore == ">":
                    stmt = stmt.where(cast(document_fields.c.valore, Float) > numero)
                elif operatore == "<":
                    stmt = stmt.where(cast(document_fields.c.valore, Float) < numero)
                elif operatore == ">=":
                    stmt = stmt.where(cast(document_fields.c.valore, Float) >= numero)
                elif operatore == "<=":
                    stmt = stmt.where(cast(document_fields.c.valore, Float) <= numero)
                else:
                    stmt = stmt.where(document_fields.c.valore.ilike(f"%{valore}%"))
            except ValueError:
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
    semantic_hits = await asyncio.to_thread(search, query, top_k=30)

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

        doc_rows = (await conn.execute(docs_stmt)).mappings().all()
        docs_map = {row["id"]: row for row in doc_rows}

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

            final_results.append({
                "id": doc_id,
                "tipo_documento": row["tipo_documento"],
                "file_path": row["file_path"],
                "data_creazione": row["data_creazione"].strftime("%Y-%m-%d %H:%M:%S"),
                "campi": fields,
                "score": hit.get("score")
            })

    final_results = final_results[:10]

    return {
        "query": query,
        "results": final_results
    }

@app.get("/search_documents/")
async def search_documents(request: Request):
    query = request.query_params.get("query", "").strip()
    if not query:
        return JSONResponse({"mode": "empty", "results": []})

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

            try:
                numero = float(str(valore).replace("h", "").strip())
                if operatore == ">":
                    stmt = stmt.where(cast(document_fields.c.valore, Float) > numero)
                elif operatore == "<":
                    stmt = stmt.where(cast(document_fields.c.valore, Float) < numero)
                elif operatore == ">=":
                    stmt = stmt.where(cast(document_fields.c.valore, Float) >= numero)
                elif operatore == "<=":
                    stmt = stmt.where(cast(document_fields.c.valore, Float) <= numero)
                else:
                    stmt = stmt.where(document_fields.c.valore.ilike(f"%{valore}%"))
            except ValueError:
                stmt = stmt.where(document_fields.c.valore.ilike(f"%{valore}%"))

        if tipo_doc:
            stmt = stmt.where(documents.c.tipo_documento.ilike(f"%{tipo_doc}%"))

        structured_rows = (await conn.execute(stmt)).mappings().all()

        if structured_rows:
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
                    "campi": fields
                })

            return JSONResponse({
                "mode": "structured",
                "results": final_results
            })

        # =========================
        # 2. FALLBACK RICERCA SEMANTICA
        # =========================
        semantic_hits = await asyncio.to_thread(search, query, top_k=30)

        if not semantic_hits:
            return JSONResponse({
                "mode": "semantic",
                "results": []
            })

        semantic_doc_ids = [hit["doc_id"] for hit in semantic_hits]

        docs_stmt = select(
            documents.c.id,
            documents.c.tipo_documento,
            documents.c.file_path,
            documents.c.data_creazione
        ).where(documents.c.id.in_(semantic_doc_ids))

        doc_rows = (await conn.execute(docs_stmt)).mappings().all()
        docs_map = {row["id"]: row for row in doc_rows}

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

            final_results.append({
                "id": doc_id,
                "tipo_documento": row["tipo_documento"],
                "file_path": row["file_path"],
                "data_creazione": row["data_creazione"].strftime("%Y-%m-%d %H:%M:%S"),
                "campi": fields,
                "score": hit.get("score")
            })

        final_results = final_results[:10]

        return JSONResponse({
            "mode": "semantic",
            "results": final_results
        })
        
@app.get("/documents_snapshot/")
async def documents_snapshot():
    if TEST_FORCE_EMPTY_DB:
        return JSONResponse(
            content={
                "docs": [],
                "stats": {
                    "tipo_count": {},
                    "date_count": {},
                    "last_upload": "-"
                }
            },
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0"
            }
        )

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