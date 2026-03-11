from fastapi import FastAPI, Request, File, UploadFile, BackgroundTasks, Query, WebSocket
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import shutil, os, json, zipfile
from io import BytesIO
from src.db_setup import engine, documents, document_fields
from src.process_document import process_document
from src.main import interpret_search_query
from sqlalchemy import select, and_, cast, Float
from fastapi.responses import JSONResponse
from src.ai_search import search_documents


app = FastAPI()

# -------------------------------
# Cartelle
# -------------------------------
UPLOAD_FOLDER = "src/uploads/"
OUTPUT_FOLDER = "output/"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Serviamo i file caricati per anteprime
app.mount("/uploads", StaticFiles(directory=UPLOAD_FOLDER), name="uploads")

# -------------------------------
# WebSocket realtime
# -------------------------------
clients = []

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
async def notify_clients(doc: dict):
    to_remove = []
    for ws in clients:
        try:
            await ws.send_text(json.dumps({"event":"document_ready","doc":doc}))
        except:
            to_remove.append(ws)
    for ws in to_remove:
        clients.remove(ws)

# -------------------------------
# HTML principale
# -------------------------------


html_content = """
<h1 style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333;">Document Flow Web</h1>

<div style="display:flex; gap:15px; margin-bottom:20px;">
    <div style="flex:1; background:white; padding:12px; border-radius:8px; box-shadow:0 2px 6px rgba(0,0,0,0.1);">
        <div style="font-size:13px; color:#666;">Totale documenti</div>
        <div id="kpi_total" style="font-size:24px; font-weight:bold;">0</div>
    </div>
    <div style="flex:1; background:white; padding:12px; border-radius:8px; box-shadow:0 2px 6px rgba(0,0,0,0.1);">
        <div style="font-size:13px; color:#666;">Documenti oggi</div>
        <div id="kpi_today" style="font-size:24px; font-weight:bold;">0</div>
    </div>
    <div style="flex:1; background:white; padding:12px; border-radius:8px; box-shadow:0 2px 6px rgba(0,0,0,0.1);">
        <div style="font-size:13px; color:#666;">Tipo più comune</div>
        <div id="kpi_tipo" style="font-size:18px; font-weight:bold;">-</div>
    </div>
    <div style="flex:1; background:white; padding:12px; border-radius:8px; box-shadow:0 2px 6px rgba(0,0,0,0.1);">
        <div style="font-size:13px; color:#666;">Ultimo upload</div>
        <div id="kpi_last" style="font-size:18px; font-weight:bold;">-</div>
    </div>
</div>

<!-- Upload -->
<section style="margin-bottom:20px;">
<h2>Carica documento</h2>
<form id="uploadForm" style="display:flex; gap:10px; align-items:center;">
<input name="file" type="file" required style="padding:5px; border-radius:4px; border:1px solid #ccc;">
<input type="submit" value="Carica" style="padding:6px 12px; border-radius:4px; border:none; background-color:#28a745; color:white; font-weight:bold; cursor:pointer;">
</form>
<div id="progressContainer" style="width:100%; background:#e0e0e0; border-radius:6px; margin-top:10px; display:none; overflow:hidden; position:relative;">
<div id="progressBar" style="width:0%; height:16px; border-radius:6px; background: linear-gradient(-45deg, #28a745 25%, #5cd65c 50%, #28a745 75%); background-size:50px 50px; animation: moveWave 1s linear infinite;"></div>
</div>
<p id="statusText" style="margin-top:5px;"></p>
</section>

<!-- Ricerca AI -->
<section style="margin-bottom:20px;">
    <h2>Ricerca AI documenti</h2>
    <div style="display:flex; gap:10px; align-items:center; margin-bottom:10px;">
    <input id="aiQuery" type="text" placeholder="Inserisci query AI..." style="flex:1; padding:6px; border-radius:4px; border:1px solid #ccc;">
    <button id="aiSearchBtn" style="padding:6px 12px; border:none; border-radius:4px; background-color:#007BFF; color:white; cursor:pointer;">Cerca</button>
    <button id="resetSearchBtn" style="padding:6px 12px; border:none; border-radius:4px; background-color:#6c757d; color:white; cursor:pointer;">Reset</button>
</div>
</section>

<!-- Lista documenti -->
<section>
<h2>Documenti elaborati</h2>
<table id="doc_table" class="display" style="width:100%">
<thead>
<tr>
<th>Seleziona</th>
<th>ID</th>
<th>Tipo Documento</th>
<th>File</th>
<th>Data Creazione</th>
<th>Dettagli</th>
</tr>
</thead>
<tbody></tbody>
</table>
<button id="downloadSelectedBtn" style="margin-top:10px; padding:6px 12px; border:none; border-radius:4px; background-color:#28a745; color:white; cursor:pointer;">Scarica selezionati</button>
</section>

<!-- Grafici -->
<section style="margin-top:30px;">
<h2>Dashboard Statistiche</h2>
<canvas id="chartTipo" style="width:100%; height:300px; margin-bottom:20px;"></canvas>
<canvas id="chartDate" style="width:100%; height:300px;"></canvas>
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
@keyframes moveWave { 0% {background-position:0 0;} 100% {background-position:50px 0;} }
</style>

<script>
$(document).ready(function(){

    let table = $('#doc_table').DataTable({
        order: [[1, "desc"]],
        data: [],
        columns:[{ title: "Seleziona" },{ title: "ID" },{ title: "Tipo Documento" },{ title: "File" },{ title: "Data Creazione" },{ title: "Dettagli" }],
        pageLength: 10,
        dom: 'Bfrtip',
        buttons: [
            { extend: 'csvHtml5', text: 'Esporta CSV', exportOptions: { columns: [1,2,3,4] } },
            { extend: 'excelHtml5', text: 'Esporta Excel', exportOptions: { columns: [1,2,3,4] } },
            { extend: 'pdfHtml5', text: 'Esporta PDF', exportOptions: { columns: [1,2,3,4] } }
        ]
    });
    
    // --- Bottone Ricerca AI ---
$("#aiSearchBtn").click(async function(){
    const query = $("#aiQuery").val().trim();
    if(!query) return alert("Inserisci una query per la ricerca AI");

    const res = await fetch("/semantic_search/?query=" + encodeURIComponent(query));
    const data = await res.json();
    const results = data.results || [];

    // Svuota tabella
    table.clear();

    // Aggiungi risultati
    results.forEach(doc => {
        const row = [
            `<input type="checkbox" class="selectDoc" value="${doc.file_path.split('/').pop()}">`,
            doc.id,
            doc.tipo_documento,
            doc.file_path.split('/').pop(),
            doc.data_creazione,
            `<button onclick='showDetails(${doc.id})' style="padding:4px 8px; border:none; border-radius:4px; background-color:#007BFF; color:white; cursor:pointer;">Mostra</button>`
        ];
        table.row.add(row);
    });
    table.draw(false);

    // Aggiorna grafici/KPI
    updateCharts();
});

// --- Reset ricerca ---
$("#resetSearchBtn").click(async function(){

    // pulisce campo ricerca
    $("#aiQuery").val("");

    // svuota tabella
    table.clear();

    // ricarica tutti i documenti
    const res = await fetch('/documents/');
    const docs = await res.json();

    docs.forEach(doc => {
        const row = [
            `<input type="checkbox" class="selectDoc" value="${doc.file_path.split('/').pop()}">`,
            doc.id,
            doc.tipo_documento,
            doc.file_path.split('/').pop(),
            doc.data_creazione,
            `<button onclick='showDetails(${doc.id})'
            style="padding:4px 8px; border:none; border-radius:4px; background-color:#007BFF; color:white; cursor:pointer;">
            Mostra</button>`
        ];
        table.row.add(row);
    });

    table.draw(false);

    updateCharts();
});
$("#aiQuery").keypress(function(e){
    if(e.which == 13){
        $("#aiSearchBtn").click();
    }
});
    let chartTipo, chartDate;

    // --- Funzione per popolare tabella e KPI dai dati esistenti ---
    async function loadInitialData(){
        const res = await fetch('/documents/');
        const docs = await res.json();

        docs.forEach(doc => {
            const row = [
                `<input type="checkbox" class="selectDoc" value="${doc.file_path.split('/').pop()}">`,
                doc.id,
                doc.tipo_documento,
                doc.file_path.split('/').pop(),
                doc.data_creazione,
                `<button onclick='showDetails(${doc.id})' style="padding:4px 8px; border:none; border-radius:4px; background-color:#007BFF; color:white; cursor:pointer;">Mostra</button>`
            ];
            table.row.add(row);
        });
        table.draw(false);

        // Aggiorna grafici e KPI
        await updateCharts();
    }

    // --- Upload ---
    let uploadedFileName = null;
    $("#uploadForm").on("submit", function(event){
        event.preventDefault();
        const fileInput = $('input[name="file"]')[0];
        const file = fileInput.files[0];
        if(!file) return;
        uploadedFileName = file.name;

        $("#progressContainer").show();
        $("#progressBar").css("width","10%");
        $("#statusText").text("Caricamento documento...");

        const formData = new FormData();
        formData.append("file", file);

        const xhr = new XMLHttpRequest();
        xhr.upload.addEventListener("progress", function(e){
            if(e.lengthComputable){
                const percent = (e.loaded / e.total) * 70;
                $("#progressBar").css("width", percent + "%");
            }
        });

        xhr.onload = function(){
            $("#progressBar").css("width","70%");
            $("#statusText").text("Upload completato. Elaborazione...");
            fileInput.value = "";
        };

        xhr.open("POST","/upload/");
        xhr.send(formData);
    });

    // --- WebSocket realtime ---
    const ws = new WebSocket("ws://" + window.location.host + "/ws/docs");
    ws.onmessage = function(event){
        const msg = JSON.parse(event.data);
        if(msg.event === "document_ready"){
            const doc = msg.doc;
            const row = [
                `<input type="checkbox" class="selectDoc" value="${doc.file_path.split('/').pop()}">`,
                doc.id,
                doc.tipo_documento,
                doc.file_path.split('/').pop(),
                doc.data_creazione,
                `<button onclick='showDetails(${doc.id})' style="padding:4px 8px; border:none; border-radius:4px; background-color:#007BFF; color:white; cursor:pointer;">Mostra</button>`
            ];
            table.row.add(row).draw(false);
            updateCharts();
            $("#statusText").text("Documento pronto: " + doc.file_path.split('/').pop());
            $("#progressBar").css("width","100%");
            setTimeout(()=>{ $("#progressContainer").hide(); $("#progressBar").css("width","0%"); },500);
        }
    };

    // --- Show Details ---
    window.showDetails = function(docId){
        fetch('/documents/').then(r=>r.json()).then(docs=>{
            const doc = docs.find(d => d.id===docId);
            if(!doc) return;
            const filename = doc.file_path.split('/').pop();
            const fileExt = filename.split('.').pop().toLowerCase();
            let html = `<h3>${doc.tipo_documento} - ${filename}</h3>`;
            if(fileExt==='pdf'){ html += `<iframe src="/uploads/${filename}" width="100%" height="400px"></iframe>`; }
            else if(['png','jpg','jpeg','gif'].includes(fileExt)){ html += `<img src="/uploads/${filename}" style="max-width:100%; max-height:400px; display:block; margin-bottom:10px;">`; }
            html += '<ul>';
            for(const [k,v] of Object.entries(doc.campi)){ html += `<li><strong>${k}</strong>: ${v}</li>`; }
            html += '</ul>';
            html += `<p><a href="/download/${filename}" style="color:#28a745; font-weight:bold;">Scarica Word</a></p>`;
            $('#modal_body').html(html);
            $('#modal').show();
        });
    };

    $("#close").click(()=>{ $('#modal').hide(); });
    $(window).click((event)=>{ if(event.target.id=='modal') $('#modal').hide(); });

    // --- Download multiplo ---
    $("#downloadSelectedBtn").click(()=> {
        const selected = Array.from($('.selectDoc:checked')).map(cb => cb.value);
        if(selected.length===0){ alert("Seleziona almeno un documento"); return; }
        const params = new URLSearchParams();
        selected.forEach(f => params.append("filenames", f));
        window.location.href = "/download_zip/?" + params.toString();
    });

    // --- Grafici e KPI ---
    async function updateCharts(){
        const res = await fetch('/stats/'); 
        const stats = await res.json();
        const tipoLabels = Object.keys(stats.tipo_count);
        const tipoData = Object.values(stats.tipo_count);
        const dateLabels = Object.keys(stats.date_count).sort();
        const dateData = dateLabels.map(d => stats.date_count[d]);

        if(chartTipo){ chartTipo.destroy(); }
        chartTipo = new Chart(document.getElementById('chartTipo').getContext('2d'),{
            type:'bar',
            data:{ labels: tipoLabels, datasets:[{ label:'Documenti per Tipo', data:tipoData, backgroundColor:'#007BFF' }] },
            options:{ responsive:true, plugins:{ legend:{ display:false } } }
        });

        if(chartDate){ chartDate.destroy(); }
        chartDate = new Chart(document.getElementById('chartDate').getContext('2d'),{
            type:'line',
            data:{ labels: dateLabels, datasets:[{ label:'Upload giornalieri', data:dateData, borderColor:'#28a745', backgroundColor:'rgba(40,167,69,0.2)', fill:true }] },
            options:{ responsive:true, plugins:{ legend:{ display:true } } }
        });

        // KPI
        const totalDocs = tipoData.length ? tipoData.reduce((a,b)=>a+b,0) : 0;
        $("#kpi_total").text(totalDocs);
        const today = new Date().toISOString().slice(0,10);
        $("#kpi_today").text(stats.date_count[today] || 0);
        let maxTipo = "-", maxVal=0;
        for(const [k,v] of Object.entries(stats.tipo_count)){ if(v>maxVal){ maxVal=v; maxTipo=k; } }
        $("#kpi_tipo").text(maxTipo);
        if(dateLabels.length){ $("#kpi_last").text(dateLabels[dateLabels.length-1]); }
    }

    // --- Inizializza pagina ---
    loadInitialData();
});


</script>
"""

@app.get("/", response_class=HTMLResponse)
def home_page():
    return HTMLResponse(content=html_content)

# -------------------------------
# Upload documento con background task
# -------------------------------
@app.post("/upload/")
async def upload(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    file_location = os.path.join(UPLOAD_FOLDER, file.filename)
    with open(file_location, "wb") as f:
        shutil.copyfileobj(file.file, f)
    # Esegui elaborazione in background
    background_tasks.add_task(process_document, file_location)
    return {"filename": file.filename, "status": "in elaborazione"}

# -------------------------------
# Lista documenti
# -------------------------------
@app.get("/documents/")
def list_documents():
    with engine.connect() as conn:
        result = conn.execute(documents.select()).all()
        docs = []
        for r in result:
            doc_id = r.id
            doc_fields = conn.execute(
                document_fields.select().where(document_fields.c.document_id==doc_id)
            ).all()
            fields_dict = {f.campo: f.valore for f in doc_fields}
            docs.append({
                "id": doc_id,
                "tipo_documento": r.tipo_documento,
                "file_path": r.file_path,
                "data_creazione": r.data_creazione.strftime("%Y-%m-%d %H:%M:%S"),
                "campi": fields_dict
            })
        return docs

# -------------------------------
# Statistiche per grafici
# -------------------------------
@app.get("/stats/")
def stats():
    with engine.connect() as conn:
        result = conn.execute(documents.select()).all()
        tipo_count = {}
        date_count = {}
        for r in result:
            tipo_count[r.tipo_documento] = tipo_count.get(r.tipo_documento,0)+1
            date_str = r.data_creazione.strftime("%Y-%m-%d")
            date_count[date_str] = date_count.get(date_str,0)+1
    return {"tipo_count": tipo_count, "date_count": date_count}

# -------------------------------
# Download singolo Word
# -------------------------------
@app.get("/download/{filename}")
def download_word(filename: str):
    path = os.path.join(OUTPUT_FOLDER, filename + ".docx")
    if os.path.exists(path):
        return FileResponse(
            path,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            filename=filename+".docx"
        )
    return {"error": "File non trovato"}

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

    # --- 1. Usa GPT per interpretare la query ---
    # interpret_search_query deve restituire anche "operatore"
    filters = interpret_search_query(query)
    tipo_doc = filters.get("tipo_documento")
    campo = filters.get("campo")
    valore = filters.get("valore")
    operatore = filters.get("operatore", None)  # es: ">", "<", ">=", "<="

    # --- 2. Costruisci query SQL ---
    with engine.connect() as conn:
        stmt = select(documents.c.id, documents.c.tipo_documento, documents.c.file_path, documents.c.data_creazione)

        if campo and valore is not None:
            stmt = stmt.join(document_fields, documents.c.id == document_fields.c.document_id)

            try:
                # Prova a convertire il valore in numero
                numero = float(str(valore).replace("h","").strip())  # rimuove "h" se c'è
                if operatore == ">":
                    stmt = stmt.where(cast(document_fields.c.valore, Float) > numero)
                elif operatore == "<":
                    stmt = stmt.where(cast(document_fields.c.valore, Float) < numero)
                elif operatore == ">=":
                    stmt = stmt.where(cast(document_fields.c.valore, Float) >= numero)
                elif operatore == "<=":
                    stmt = stmt.where(cast(document_fields.c.valore, Float) <= numero)
                else:
                    # fallback: ricerca testuale
                    stmt = stmt.where(document_fields.c.valore.ilike(f"%{valore}%"))
            except ValueError:
                # non è numero, ricerca testuale
                stmt = stmt.where(document_fields.c.valore.ilike(f"%{valore}%"))

        if tipo_doc:
            stmt = stmt.where(documents.c.tipo_documento.ilike(f"%{tipo_doc}%"))

        results = conn.execute(stmt).mappings().all()

        # --- 3. Recupera anche i campi dei documenti per mostrare dettagli ---
        final_results = []
        for row in results:
            doc_id = row["id"]
            fields_stmt = select(document_fields.c.campo, document_fields.c.valore).where(document_fields.c.document_id == doc_id)
            fields = {f["campo"]: f["valore"] for f in conn.execute(fields_stmt).mappings().all()}
            final_results.append({
                "id": doc_id,
                "tipo_documento": row["tipo_documento"],
                "file_path": row["file_path"],
                "data_creazione": row["data_creazione"].isoformat(),
                "campi": fields
            })

    return JSONResponse({"results": final_results})
    
@app.get("/semantic_search/")
async def semantic_search(query: str):

    doc_ids = search_documents(query)

    if not doc_ids:
        return {"results":[]}

    with engine.connect() as conn:

        stmt = select(documents).where(documents.c.id.in_(doc_ids))

        rows = conn.execute(stmt).mappings().all()

        results = []
        for r in rows:

            fields_stmt = select(document_fields).where(
                document_fields.c.document_id == r["id"]
            )

            fields = {
                f["campo"]:f["valore"]
                for f in conn.execute(fields_stmt).mappings()
            }

            results.append({
                "id": r["id"],
                "tipo_documento": r["tipo_documento"],
                "file_path": r["file_path"],
                "data_creazione": r["data_creazione"].isoformat(),
                "campi": fields
            })

    return {"results": results}