# src/db_setup.py

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncSession
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, DateTime, Text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
from dotenv import load_dotenv

# ========================
# CARICA VARIABILI AMBIENTE
# ========================

load_dotenv()

# ========================
# CONFIGURAZIONE DATABASE SQL SERVER ASINCRONA
# ========================

server = os.getenv("DB_SERVER", "N2982\\MSSQLSERVER2022")
database = os.getenv("DB_NAME", "PMSAPI")
username = os.getenv("DB_USER", "sa")
password = os.getenv("DB_PASSWORD")

if not password:
    raise ValueError("DB_PASSWORD non definita. Controlla il file .env")

# Connessione SQL Server asincrona (via aioodbc)
ASYNC_DB_URL = (
    f"mssql+aioodbc://{username}:{password}@{server}/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&Encrypt=no"
    "&TrustServerCertificate=yes"
)

# ========================
# ENGINE ASINCRONO
# ========================

async_engine: AsyncEngine = create_async_engine(
    ASYNC_DB_URL,
    pool_pre_ping=True,
    echo=False
)

# ========================
# SESSIONE ASINCRONA
# ========================

async_session = sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# ========================
# METADATA E TABELLE
# ========================

metadata = MetaData()

documents = Table(
    'documents', metadata,
    Column('id', Integer, primary_key=True),
    Column('tipo_documento', String(100)),
    Column('file_path', String(255)),
    Column('ocr_text', Text, nullable=True),
    Column("ocr_pages", Text),
    Column("campi_canonici_json", Text, nullable=True),
    Column('data_creazione', DateTime, default=datetime.now)
)

document_fields = Table(
    'document_fields', metadata,
    Column('id', Integer, primary_key=True),
    Column('document_id', Integer, ForeignKey('documents.id')),
    Column('campo', String(100)),
    Column('valore', String(255))
)

document_jobs = Table(
    'document_jobs', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('file_name', String(255)),
    Column('file_path', String(500), nullable=True),
    Column('upload_date', DateTime, default=datetime.now),
    Column('status', String(50), default='queued'),
    Column('document_id', Integer, ForeignKey('documents.id'), nullable=True),
    Column('error_message', Text, nullable=True),
    Column('phase', String(100), nullable=True),
    Column('progress', Integer, default=0),
    Column('cancelled', Integer, default=0),
    Column('started_at', DateTime, nullable=True),
    Column('completed_at', DateTime, nullable=True),
    Column('updated_at', DateTime, nullable=True)
)

# ========================
# CREAZIONE TABELLE
# ========================

async def init_db():
    async with async_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    print("Tabelle create / verificate correttamente (async)")

# ========================
# CONFIG UPLOAD
# ========================

UPLOAD_FOLDER = "src/uploads"

if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# ========================
# WEBSOCKET CLIENTS
# ========================

# lista condivisa dei websocket clients
# IMPORTARE SOLO DA QUI
clients = []