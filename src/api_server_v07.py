from fastapi import FastAPI, File, UploadFile, BackgroundTasks
from fastapi.responses import HTMLResponse, FileResponse
import shutil
import os
from src.db_setup import engine, documents, document_fields
from src.main import process_document

app = FastAPI()

# Cartelle
UPLOAD_FOLDER = "src/uploads/"
OUTPUT_FOLDER = "output/"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------------------
# Pagina principale con upload e lista documenti
# -------------------------------
@app.get("/", response_class=HTMLResponse)
def home_page():
    html_content = """
    <h1 style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333;">Document Flow Web</h1>

    <section style="margin-bottom: 20px;">
        <h2>Carica documento</h2>
        <form id="uploadForm" style="display:flex; gap: 10px; align-items:center;">
            <input name="file" type="file" style="padding: 5px; border-radius: 4px; border: 1px solid #ccc;" required>
            <input type="submit" value="Carica" style="padding: 6px 12px; border-radius: 4px; border:none; background-color:#28a745; color:white; font-weight:bold; cursor:pointer;">
        </form>
        <div id="progressContainer" style="width:100%; background:#e0e0e0; border-radius:6px; margin-top:10px; display:none; overflow:hidden; position:relative;">
            <div id="progressBar" style="width:0%; height:16px; border-radius:6px; background: linear-gradient(-45deg, #28a745 25%, #5cd65c 50%, #28a745 75%); background-size: 50px 50px; animation: moveWave 1s linear infinite;"></div>
        </div>
        <p id="statusText" style="margin-top:5px;"></p>
    </section>

    <section>
        <h2>Documenti elaborati</h2>
        <table id="doc_table" class="display" style="width:100%">
            <thead>
                <tr>
                    <th>ID</th>
                    <th>Tipo Documento</th>
                    <th>File</th>
                    <th>Data Creazione</th>
                    <th>Dettagli</th>
                </tr>
            </thead>
            <tbody></tbody>
        </table>
    </section>

    <!-- Modal -->
    <div id="modal" style="display:none; position:fixed; z-index:10; left:0; top:0; width:100%; height:100%; overflow:auto; background-color: rgba(0,0,0,0.5);">
        <div style="background-color:#fff; margin:5% auto; padding:20px; border-radius:10px; width:80%; max-width:700px; max-height:80%; overflow-y:auto; box-shadow: 0 4px 12px rgba(0,0,0,0.2);">
            <span id="close" style="float:right; font-size:24px; font-weight:bold; cursor:pointer;">&times;</span>
            <div id="modal_body"></div>
        </div>
    </div>

    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css">
    <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>

    <style>
    @keyframes moveWave {
        0% { background-position: 0 0; }
        100% { background-position: 50px 0; }
    }
    </style>

    <script>
    let pendingFiles = [];

    document.getElementById("uploadForm").onsubmit = async (event) => {
        event.preventDefault();
        const fileInput = document.querySelector('input[name="file"]');
        const file = fileInput.files[0];
        if(!file) return;

        pendingFiles.push(file.name);
        document.getElementById("progressContainer").style.display = "block";
        document.getElementById("statusText").textContent = "Documento inviato. Elaborazione in corso...";

        const formData = new FormData();
        formData.append("file", file);

        await fetch('/upload/', { method: "POST", body: formData });
        fileInput.value = ""; // reset input
    };

    let table;

    async function loadDocs() {
        const res = await fetch('/documents/');
        const docs = await res.json();

        // Filtra solo i documenti già processati
        const processedDocs = docs.filter(d => !pendingFiles.includes(d.file_path.split('/').pop()));

        const data = processedDocs.map(doc => [
            doc.id,
            doc.tipo_documento,
            doc.file_path.split('/').pop(),
            doc.data_creazione,
            `<button onclick='showDetails(${doc.id})' style="padding:4px 8px; border:none; border-radius:4px; background-color:#007BFF; color:white; cursor:pointer;">Mostra</button>`
        ]);

        if(!table){
            table = $('#doc_table').DataTable({
                data: data,
                columns: [
                    { title: "ID" },
                    { title: "Tipo Documento" },
                    { title: "File" },
                    { title: "Data Creazione" },
                    { title: "Dettagli" }
                ],
                pageLength: 10,    // 10 documenti per pagina
                lengthChange: true,
                searching: true,
                ordering: true
            });
        } else {
            table.clear();
            table.rows.add(data);
            table.draw();
        }

        // Aggiorna barra progresso
        if(pendingFiles.length){
            const processedCount = pendingFiles.filter(f => docs.find(d => d.file_path.split('/').pop() === f)).length;
            const percent = (processedCount / pendingFiles.length) * 100;
            document.getElementById("progressBar").style.width = percent + "%";

            if(processedCount === pendingFiles.length){
                document.getElementById("statusText").textContent = "Elaborazione completata!";
                document.getElementById("statusText").style.color = "#28a745";
                pendingFiles = [];
                setTimeout(() => {
                    document.getElementById("progressContainer").style.display = "none";
                    document.getElementById("progressBar").style.width = "0%";
                }, 1500);
            }
        }
    }

    async function showDetails(docId) {
        const res = await fetch('/documents/');
        const docs = await res.json();
        const doc = docs.find(d => d.id === docId);
        if(!doc) return;
        let html = `<h3 style="margin-top:0;">${doc.tipo_documento} - ${doc.file_path.split('/').pop()}</h3>`;
        html += '<ul style="padding-left:20px;">';
        for(const [k,v] of Object.entries(doc.campi)){
            html += `<li><strong>${k}</strong>: ${v}</li>`;
        }
        html += '</ul>';
        html += `<p><a href="/download/${doc.file_path.split('/').pop()}" style="color:#28a745; font-weight:bold;">Scarica Word</a></p>`;
        document.getElementById('modal_body').innerHTML = html;
        document.getElementById('modal').style.display = 'block';
    }

    document.getElementById("close").onclick = () => { document.getElementById('modal').style.display = 'none'; }
    window.onclick = (event) => { if(event.target == document.getElementById('modal')) { document.getElementById('modal').style.display = 'none'; } }

    loadDocs();
    setInterval(loadDocs, 5000);
    </script>
    """
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
# Lista documenti con campi
# -------------------------------
@app.get("/documents/")
def list_documents():
    with engine.connect() as conn:
        result = conn.execute(documents.select()).all()
        docs = []
        for r in result:
            doc_id = r.id
            doc_fields = conn.execute(document_fields.select().where(document_fields.c.document_id==doc_id)).all()
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
# Download Word
# -------------------------------
@app.get("/download/{filename}")
def download_word(filename: str):
    path = os.path.join(OUTPUT_FOLDER, filename + ".docx")
    if os.path.exists(path):
        return FileResponse(path, media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document", filename=filename+".docx")
    return {"error": "File non trovato"}