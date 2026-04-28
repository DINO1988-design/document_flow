import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from fastapi.testclient import TestClient
from src.api_server import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


def search(client, q: str):
    response = client.get("/search_documents/", params={"query": q})

    if response.status_code != 200:
        print("\n=== ERRORE API ===")
        print("QUERY:", q)
        print("STATUS:", response.status_code)
        print("BODY:", response.text)

    assert response.status_code == 200
    return response.json()


# ===============================
# ✅ TEST CHE DEVONO FUNZIONARE
# ===============================

@pytest.mark.parametrize("query", [
    "documenti di noureddine slimani che sono bonifici",
    "documenti di noureddine slimani che sono bonifico",
    "bonifici di noureddine slimani",
    "bonifico slimani",
    "slimani bonifici",
    "documenti slimani bonifico",
])
def test_bonifico_variants(client, query):
    data = search(client, query)

    assert data["results"], f"Nessun risultato per query: {query}"

    for r in data["results"]:
        tipo = (r.get("tipo_documento") or "").lower()
        assert "bonific" in tipo


@pytest.mark.parametrize("query", [
    "documenti di noureddine slimani che sono cedolini",
    "documenti di noureddine slimani che sono cedolino",
])
def test_cedolino(client, query):
    data = search(client, query)

    assert data["results"]

    for r in data["results"]:
        tipo = (r.get("tipo_documento") or "").lower()
        assert "cedolin" in tipo


@pytest.mark.parametrize("query", [
    "documenti di noureddine slimani che non sono bonifici",
    "documenti di noureddine slimani esclusi bonifici",
])
def test_negative_bonifico(client, query):
    data = search(client, query)

    assert data["results"]

    for r in data["results"]:
        tipo = (r.get("tipo_documento") or "").lower()
        assert "bonific" not in tipo


# ===============================
# 🔥 TEST ROBUSTEZZA
# ===============================

@pytest.mark.parametrize("query", [
    "slimani bonifici",
    "bonifici slimani documenti",
    "dammi bonifici di slimani",
])
def test_dirty_queries(client, query):
    data = search(client, query)

    assert data["results"]


# ===============================
# ⚠️ BUG CRITICO (IL TUO)
# ===============================

def test_singular_vs_plural_bug(client):
    data1 = search(client, "documenti di noureddine slimani che sono bonifico")
    data2 = search(client, "documenti di noureddine slimani che sono bonifici")

    ids1 = {r["id"] for r in data1["results"]}
    ids2 = {r["id"] for r in data2["results"]}

    assert ids1 == ids2


# ===============================
# 🧨 TEST FALSI POSITIVI
# ===============================

def test_wrong_tipo(client):
    data = search(client, "documenti di noureddine slimani che sono fattura")

    for r in data["results"]:
        tipo = (r.get("tipo_documento") or "").lower()
        assert "fattur" in tipo
        
@pytest.mark.parametrize("query, excluded_tipo", [
    ("documenti che non sono contratti ma parlano di bonifico", "contratt"),
    ("documenti che non sono bonifici ma parlano di bonifico", "bonific"),
    ("documenti che non sono cedolini ma parlano di bonifico", "cedolin"),
])
def test_not_tipo_but_contains_bonifico(client, query, excluded_tipo):
    data = search(client, query)

    assert data["results"], f"Nessun risultato per query: {query}"

    for r in data["results"]:
        tipo = (r.get("tipo_documento") or "").lower()

        # ❌ esclusione tipo
        assert excluded_tipo not in tipo, f"Tipo vietato trovato: {tipo}"

        # ✅ deve avere segnale bonifico
        assert (
            r.get("keyword_match")
            or r.get("text_match")
            or "bonific" in (r.get("best_chunk_text") or "").lower()
        ), f"Nessun segnale bonifico per doc {r['id']}"        
        
# ===============================
# 🆕 TEST PERSONA / VIAGGI
# ===============================

def extract_ids(data):
    return {r["id"] for r in data["results"]}


@pytest.mark.parametrize("query", [
    "documenti di MOHAMED AMINE BOUGHATTAS",
    "documenti di viaggio mohamed amine boughattas",
    "viaggi Mohamed amine boughattas",
])
def test_mohamed_amine_boughattas_queries(client, query):
    data = search(client, query)

    assert data["results"], f"Nessun risultato per query: {query}"

    ids = extract_ids(data)

    # Deve trovare almeno uno dei documenti reali
    assert 1211 in ids or 1212 in ids, f"Attesi doc 1211/1212 per query: {query}"

    # Tutti i risultati devono matchare la persona
    for r in data["results"]:
        assert r.get("persona_match") is True


def test_documenti_di_viaggio_slimani(client):
    data = search(client, "documenti di viaggio slimani")

    # Non deve più tornare vuoto (bug del keyword filter duro)
    assert data["results"], "Query non dovrebbe essere vuota"

    # Deve comunque rispettare la persona
    for r in data["results"]:
        assert r.get("persona_match") is True