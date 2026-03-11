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
        <form id="upload_form" enctype="multipart/form-data" method="post" style="display:flex; gap: 10px; align-items:center;">
            <input name="file" type="file" style="padding: 5px; border-radius: 4px; border: 1px solid #ccc;" required>
            <input type="submit" value="Carica" style="padding: 6px 12px; border-radius: 4px; border:none; background-color:#28a745; color:white; font-weight:bold; cursor:pointer;">
        </form>
        <div id="progress_container" style="display:none; width:100%; background:#eee; border-radius:5px; margin-top:10px; height:20px;">
            <div id="progress_bar" style="width:0%; height:100%; background:#28a745; border-radius:5px; transition: width 0.3s;"></div>
        </div>
        <p id="upload_status" style="font-style:italic; color:#555; margin-top:5px;"></p>
    </section>

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

    <!-- Modal -->
    <div id="modal" style="display:none; position:fixed; z-index:10; left:0; top:0; width:100%; height:100%; overflow:auto; background-color: rgba(0,0,0,0.5);">
        <div style="background-color:#fff; margin:5% auto; padding:20px; border-radius:10px; width:80%; max-width:700px; max-height:80%; overflow-y:auto; box-shadow: 0 4px 12px rgba(0,0,0,0.2);">
            <span id="close" style="float:right; font-size:24px; font-weight:bold; cursor:pointer;">&times;</span>
            <div id="modal_body"></div>
        </div>
    </div>

    <script>
    const uploadForm = document.getElementById("upload_form");
    const statusText = document.getElementById("upload_status");
    const progressContainer = document.getElementById("progress_container");
    const progressBar = document.getElementById("progress_bar");

    uploadForm.addEventListener("submit", async (e) => {
        e.preventDefault();
        const fileInput = uploadForm.querySelector("input[name='file']");
        if(!fileInput.files.length) return;

        const formData = new FormData();
        formData.append("file", fileInput.files[0]);

        // Mostra loader e barra
        statusText.textContent = "Documento inviato. Elaborazione in corso...";
        statusText.style.color = "#555";
        progressContainer.style.display = "block";
        progressBar.style.width = "0%";

        try {
            const res = await fetch("/upload/", {
                method: "POST",
                body: formData
            });
            const data = await res.json();
            console.log("Upload response:", data);

            const fileName = data.filename;
            let progress = 0;

            // Polling fino a quando il documento non appare
            const checkInterval = setInterval(async () => {
                progress = Math.min(progress + Math.random()*10, 90); // avanza barra lentamente
                progressBar.style.width = progress + "%";

                const docsRes = await fetch("/documents/");
                const docs = await docsRes.json();
                const found = docs.find(d => d.file_path.split('/').pop() === fileName);

                if(found){
                    clearInterval(checkInterval);
                    progressBar.style.width = "100%";
                    statusText.textContent = "Elaborazione completata!";
                    statusText.style.color = "#28a745";
                    loadDocs();
                    setTimeout(() => { progressContainer.style.display = "none"; progressBar.style.width="0%"; }, 1500);
                }
            }, 1500);

        } catch(err){
            console.error(err);
            statusText.textContent = "Errore durante l'upload.";
            statusText.style.color = "red";
            progressContainer.style.display = "none";
            progressBar.style.width = "0%";
        }
    });

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