from fastapi import FastAPI, UploadFile, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
from pathlib import Path
from src.main import process_document
from src.db_setup import engine, documents

# --------- Config ---------
UPLOAD_FOLDER = "src/uploads/"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# --------- App ---------
app = FastAPI(title="Document Flow API", version="0.1.0")

# cartelle statiche e template
templates = Jinja2Templates(directory="src/templates")
app.mount("/uploads", StaticFiles(directory=UPLOAD_FOLDER), name="uploads")
app.mount("/output", StaticFiles(directory="output"), name="output")

# --------- Routes ---------
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    # recupera documenti dal DB
    with engine.connect() as conn:
        result = conn.execute(documents.select())
        docs = [dict(r._mapping) for r in result]

    return templates.TemplateResponse("index.html", {"request": request, "documents": docs})

@app.post("/upload", response_class=HTMLResponse)
async def upload_file(request: Request, file: UploadFile):
    # salva il file nella cartella uploads
    file_path = os.path.join(UPLOAD_FOLDER, file.filename)
    with open(file_path, "wb") as f:
        f.write(await file.read())

    # processa il documento
    process_document(file_path)

    # ritorna la home aggiornata
    with engine.connect() as conn:
        result = conn.execute(documents.select())
        docs = [dict(r._mapping) for r in result]

    return templates.TemplateResponse("index.html", {"request": request, "documents": docs})