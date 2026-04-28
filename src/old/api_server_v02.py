from fastapi import FastAPI, File, UploadFile
from fastapi.responses import HTMLResponse, FileResponse
import shutil
import os
from datetime import datetime
import json
from src.db_setup import engine, documents, document_fields
from src.main import process_document  # la funzione che processa e salva documenti

app = FastAPI()

# Cartelle
UPLOAD_FOLDER = "src/uploads/"
OUTPUT_FOLDER = "output/"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# -------------------------------
# Pagina principale con upload e tabella documenti
# -------------------------------
@app.get("/", response_class=HTMLResponse)
def home_page():
    html_content = """
    <h1 style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333;">Document Flow Web</h1>
    
    <!-- Sezione Upload -->
    <section style="margin-bottom: 20px;">
        <h2>Carica documento</h2>
        <form id="uploadForm" enctype="multipart/form-data" style="display:flex; flex-direction:column; gap: 10px; width: 400px;">
            <input name="file" type="file" required style="padding: 5px; border-radius: 4px; border: 1px solid #ccc;">
            <button type="submit" style="padding: 6px 12px; border-radius: 4px; border:none; background-color:#28a745; color:white; font-weight:bold; cursor:pointer;">Carica</button>
            <div style="width:100%; background-color:#f0f0f0; height:20px; border-radius: 4px; overflow:hidden; display:none;" id="progressContainer">
                <div id="progressBar" style="width:0%; height:100%; background-color:#28a745;"></div>
            </div>
        </form>
    </section>

    <!-- Sezione Tabella Documenti -->
    <section>
        <h2>Documenti elaborati</h2>
        <table id="doc_table" style="border-collapse: collapse; width: 100%; font-family: 'Segoe UI', sans-serif; box-shadow: 0 2px 6px rgba(0,0,0,0.1); border-radius: 8px; overflow: hidden;">
            <thead style="background-color: #007BFF; color: white;">
                <tr>
                    <th style="padding: 10px;">ID</th>
                    <th style="padding: 10px;">Tipo Documento</th>
                    <th style="padding: 10px;">File</th>
                    <th style="padding: 10px;">Data Creazione</th>
                    <th style="padding: 10px;">Dettagli</th>
                </tr>
            </thead>
            <tbody style="background-color: #f9f9f9;"></tbody>
        </table>
    </section>

    <!-- Modal Dettagli -->
    <div id="modal" style="display:none; position:fixed; z-index:10; left:0; top:0; width:100%; height:100%; overflow:auto; background-color: rgba(0,0,0,0.5);">
        <div style="background-color:#fff; margin:5% auto; padding:20px; border-radius:10px; width:80%; max-width:700px; max-height:80%; overflow-y:auto; box-shadow: 0 4px 12px rgba(0,0,0,0.2);">
            <span id="close" style="float:right; font-size:24px; font-weight:bold; cursor:pointer;">&times;</span>
            <div id="modal_body"></div>
        </div>
    </div>

    <script>
    // ========================
    // Upload file con barra progresso
    // ========================
    document.getElementById('uploadForm').addEventListener('submit', (e) => {
        e.preventDefault();
        const fileInput = e.target.querySelector('input[name="file"]');
        if(!fileInput.files.length) return;

        const formData = new FormData();
        formData.append("file", fileInput.files[0]);

        const xhr = new XMLHttpRequest();
        const progressContainer = document.getElementById('progressContainer');
        const progressBar = document.getElementById('progressBar');

        progressContainer.style.display = 'block';
        progressBar.style.width = '0%';

        xhr.upload.addEventListener('progress', (event) => {
            if(event.lengthComputable){
                const percent = (event.loaded / event.total) * 100;
                progressBar.style.width = percent + '%';
            }
        });

        xhr.onreadystatechange = () => {
            if(xhr.readyState === 4){
                if(xhr.status === 200){
                    alert('File ' + fileInput.files[0].name + ' elaborato!');
                    e.target.reset();
                    progressContainer.style.display = 'none';
                    loadDocs(); // aggiorna tabella
                } else {
                    alert('Errore durante l\\'upload');
                }
            }
        };

        xhr.open('POST', '/upload/', true);
        xhr.send(formData);
    });

    // ========================
    // Carica documenti in tabella
    // ========================
    async function loadDocs() {
        const res = await fetch('/documents/');
        const docs = await res.json();
        const tbody = document.querySelector("#doc_table tbody");
        tbody.innerHTML = '';
        docs.forEach(doc => {
            const tr = document.createElement('tr');
            tr.style.transition = 'background-color 0.2s';
            tr.onmouseover = () => tr.style.backgroundColor = '#e6f0ff';
            tr.onmouseleave = () => tr.style.backgroundColor = '#f9f9f9';
            tr.innerHTML = `
                <td style="padding:8px;">${doc.id}</td>
                <td style="padding:8px;">${doc.tipo_documento}</td>
                <td style="padding:8px;">${doc.file_path.split('/').pop()}</td>
                <td style="padding:8px;">${doc.data_creazione}</td>
                <td style="padding:8px;"><button onclick='showDetails(${doc.id})' style="padding:4px 8px; border:none; border-radius:4px; background-color:#007BFF; color:white; cursor:pointer;">Mostra</button></td>
            `;
            tbody.appendChild(tr);
        });
    }

    // ========================
    // Mostra modal dettagli documento
    // ========================
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
    setInterval(loadDocs, 5000); // aggiorna tabella ogni 5 sec
    </script>
    """
    return HTMLResponse(content=html_content)


# -------------------------------
# Upload documento
# -------------------------------
@app.post("/upload/")
async def upload(file: UploadFile = File(...)):
    file_location = os.path.join(UPLOAD_FOLDER, file.filename)
    with open(file_location, "wb") as f:
        shutil.copyfileobj(file.file, f)
    
    # Processa e salva nel DB + genera Word
    process_document(file_location)
    
    return {"filename": file.filename, "status": "elaborato"}

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
        return FileResponse(
            path,
            media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            filename=filename+".docx"
        )
    return {"error": "File non trovato"}