from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, DateTime
from datetime import datetime

# ========================
# CONFIGURAZIONE DATABASE SQL SERVER
# ========================
server = "N2982\MSSQLSERVER2022"   # Cambia con il tuo server
database = "PMSAPI"                # Cambia con il tuo database
username = "sa"                     # Cambia con username
password = "DINOsql40622336!"       # Cambia con password

# Connessione SQL Server
engine = create_engine(
    f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server",
    pool_pre_ping=True
)

metadata = MetaData()

# Tabella documenti
documents = Table(
    'documents', metadata,
    Column('id', Integer, primary_key=True),
    Column('tipo_documento', String(100)),
    Column('file_path', String(255)),
    Column('data_creazione', DateTime, default=datetime.now)
)

# Tabella campi documento
document_fields = Table(
    'document_fields', metadata,
    Column('id', Integer, primary_key=True),
    Column('document_id', Integer, ForeignKey('documents.id')),
    Column('campo', String(100)),
    Column('valore', String(255))
)

# Crea tabelle se non esistono
metadata.create_all(engine)
print("Tabelle create / verificate correttamente")