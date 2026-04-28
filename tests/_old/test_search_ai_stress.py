# tests/test_search_ai_stress.py
import pytest

from src.api_server import (
    normalize_text,
    tokenize_text,
    are_similar_tokens,
    count_similar_token_matches,
    raw_ocr_person_match,
    has_fuzzy_person_match,
    document_type_matches,
    compute_document_score,
)


# =========================================================
# HELPERS
# =========================================================
def score_doc(
    *,
    query,
    parsed_query,
    row,
    fields,
    semantic_score=0.0,
    best_chunk_text="",
    raw_ocr_text="",
):
    return compute_document_score(
        query=query,
        parsed_query=parsed_query,
        row=row,
        fields=fields,
        semantic_score=semantic_score,
        best_chunk_text=best_chunk_text,
        raw_ocr_text=raw_ocr_text,
    )


def rank_documents(query, parsed_query, docs):
    results = []

    for doc in docs:
        result = score_doc(
            query=query,
            parsed_query=parsed_query,
            row=doc["row"],
            fields=doc["fields"],
            semantic_score=doc.get("semantic_score", 0.0),
            best_chunk_text=doc.get("best_chunk_text", ""),
            raw_ocr_text=doc.get("raw_ocr_text", ""),
        )

        results.append({
            "name": doc["name"],
            "score": result["score"],
            "result": result,
            "row": doc["row"],
            "fields": doc["fields"],
        })

    results.sort(
        key=lambda r: (
            1 if r["result"].get("persona_match") else 0,
            r["result"].get("persona_token_matches") or 0,
            1 if r["result"].get("text_match") else 0,
            1 if r["result"].get("tipo_match") else 0,
            r["score"],
        ),
        reverse=True
    )
    return results


# =========================================================
# 1. NORMALIZZAZIONE TESTO
# =========================================================
@pytest.mark.parametrize(
    "raw, expected",
    [
        ("  João Silva  ", "joao silva"),
        ("BIGLIETTO-AEREO", "biglietto aereo"),
        ("e_ticket/ryanair", "e ticket ryanair"),
        ("Città di Tunisi", "citta di tunisi"),
        ("Makrem\tSlimani", "makrem slimani"),
        ("Nesrine___Ali", "nesrine ali"),
        ("passaporto n° 123", "passaporto n 123"),
    ],
)
def test_normalize_text_cases(raw, expected):
    assert normalize_text(raw) == expected


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Makrem Slimani", ["makrem", "slimani"]),
        ("João", ["joao"]),
        ("e-ticket ryanair", ["e", "ticket", "ryanair"]),
        ("A B C", []),
        ("", []),
        (None, []),
    ],
)
def test_tokenize_text_cases(raw, expected):
    assert tokenize_text(raw) == expected


# =========================================================
# 2. FUZZY TOKEN MATCH
# =========================================================
@pytest.mark.parametrize(
    "a, b, expected",
    [
        ("makrem", "makrem", True),
        ("makrem", "makrm", True),
        ("joao", "joão", True),
        ("tuline", "tulin", True),
        ("milan", "milano", False),   # importante: evita match troppo permissivo
        ("ali", "alì", True),
        ("fattura", "ticket", False),
    ],
)
def test_are_similar_tokens_cases(a, b, expected):
    assert are_similar_tokens(a, b) is expected


@pytest.mark.parametrize(
    "query_tokens, target_text, expected",
    [
        (["makrem"], "Passenger Name Makrem Slimani", 1),
        (["makrem"], "Passenger Name Makrm Slimani", 1),
        (["nesrine", "ali"], "biglietto per Nesrine Ben Ali", 2),
        (["joao"], "Passenger João Silva", 1),
        (["milan"], "volo da milano a roma", 0),
        (["tuline"], "Passenger Makrem", 0),
        ([], "Passenger Makrem", 0),
    ],
)
def test_count_similar_token_matches_cases(query_tokens, target_text, expected):
    assert count_similar_token_matches(query_tokens, target_text) == expected


# =========================================================
# 3. MATCH OCR RAW
# =========================================================
@pytest.mark.parametrize(
    "persona, raw_text, expected",
    [
        ("makrem", "Passenger Name Makrem Slimani", True),
        ("makrem", "Passenger Name Makrm Slimani", True),
        ("joao", "Passenger João Silva", True),
        ("nesrine ali", "Nesrine Ben Ali boarding pass", True),
        ("tuline", "Passenger Makrem Slimani", False),
        ("", "Passenger Makrem", False),
        ("makrem", "", False),
    ],
)
def test_raw_ocr_person_match_cases(persona, raw_text, expected):
    assert raw_ocr_person_match(persona, raw_text) is expected


# =========================================================
# 4. MATCH PERSONA SU CAMPI
# =========================================================
@pytest.mark.parametrize(
    "fields, person_tokens, expected",
    [
        (
            {"passenger_name": "Makrem Slimani"},
            ["makrem"],
            True,
        ),
        (
            {"customer_name": "Nesrine Ben Ali"},
            ["nesrine", "ali"],
            True,
        ),
        (
            {"note": "viaggiatore Makrem Slimani confermato"},
            ["makrem"],
            True,
        ),
        (
            {"passenger_name": "Ahmed Trabelsi"},
            ["makrem"],
            False,
        ),
        (
            {"random_field": "nessuna persona utile"},
            ["makrem"],
            False,
        ),
    ],
)
def test_has_fuzzy_person_match_cases(fields, person_tokens, expected):
    assert has_fuzzy_person_match(fields, person_tokens) is expected


# =========================================================
# 5. MATCH TIPO DOCUMENTO
# =========================================================
@pytest.mark.parametrize(
    "query_tipo, row_tipo, expected",
    [
        ("ticket", "biglietto aereo", True),
        ("e-ticket", "ticket", True),
        ("biglietto", "biglietto traghetto", True),
        ("fattura", "fattura", True),
        ("invoice", "fattura", True),
        ("passaporto", "passport", True),
        ("contratto", "contract", True),
        ("ticket", "fattura", False),
        ("contratto", "biglietto aereo", False),
        ("generico", "ticket", False),
        ("ticket", "generico", False),
    ],
)
def test_document_type_matches_cases(query_tipo, row_tipo, expected):
    assert document_type_matches(query_tipo, row_tipo) is expected


# =========================================================
# 6. TEST UNITARI STRESS SU compute_document_score
# =========================================================
@pytest.mark.parametrize(
    "case",
    [
        {
            "name": "strong_person_match",
            "query": "ticket makrem",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/doc1.pdf"},
            "fields": {"passenger_name": "Makrem Slimani"},
            "semantic_score": 0.30,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.75,
        },
        {
            "name": "medium_person_match",
            "query": "ticket nesrine ali",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine ali", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc2.pdf"},
            "fields": {"customer_name": "Nesrine Ben Ali"},
            "semantic_score": 0.25,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.45,
        },
        {
            "name": "generic_text_match_only",
            "query": "makrem milano",
            "parsed_query": {"tipo_documento": None, "persona": "makrem", "nome_file": None},
            "row": {"tipo_documento": "biglietto", "file_path": "src/uploads/doc3.pdf"},
            "fields": {"note": "viaggio di Makrem da Milano a Tunisi"},
            "semantic_score": 0.20,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.35,
        },
        {
            "name": "chunk_match_only",
            "query": "ticket tuline",
            "parsed_query": {"tipo_documento": "ticket", "persona": "tuline", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc4.pdf"},
            "fields": {"booking_reference": "ABC123"},
            "semantic_score": 0.20,
            "best_chunk_text": "Passenger Tuline on board",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.30,
        },
        {
            "name": "raw_ocr_match_only",
            "query": "ticket joao",
            "parsed_query": {"tipo_documento": "ticket", "persona": "joao", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc5.pdf"},
            "fields": {"booking_reference": "XYZ"},
            "semantic_score": 0.15,
            "best_chunk_text": "",
            "raw_ocr_text": "Passenger João Silva",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.25,
        },
        {
            "name": "filename_only_should_be_weaker",
            "query": "ticket tuline",
            "parsed_query": {"tipo_documento": "ticket", "persona": "tuline", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/tuline_ticket.pdf"},
            "fields": {"booking_reference": "ABC"},
            "semantic_score": 0.10,
            "best_chunk_text": "Passenger Makrem",
            "raw_ocr_text": "",
            "expected_persona_match": False,
            "expected_filename_only": True,
            "max_score": 0.20,
        },
        {
            "name": "person_in_filename_and_chunk",
            "query": "ticket tuline",
            "parsed_query": {"tipo_documento": "ticket", "persona": "tuline", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/tuline_ticket.pdf"},
            "fields": {"booking_reference": "ABC"},
            "semantic_score": 0.40,
            "best_chunk_text": "Passenger Tuline",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.30,
        },
        {
            "name": "wrong_document_type_should_penalize",
            "query": "fattura makrem",
            "parsed_query": {"tipo_documento": "fattura", "persona": "makrem", "nome_file": None},
            "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/doc7.pdf"},
            "fields": {"passenger_name": "Makrem Slimani"},
            "semantic_score": 0.70,
            "best_chunk_text": "Passenger Makrem",
            "raw_ocr_text": "Passenger Makrem",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "max_score": 0.90,
        },
        {
            "name": "tipo_match_without_person_query",
            "query": "ticket ryanair",
            "parsed_query": {"tipo_documento": "ticket", "persona": None, "nome_file": None},
            "row": {"tipo_documento": "ticket ryanair", "file_path": "src/uploads/doc8.pdf"},
            "fields": {"booking_reference": "FR123", "compagnia": "Ryanair"},
            "semantic_score": 0.20,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": False,
            "expected_filename_only": False,
            "min_score": 0.20,
        },
        {
            "name": "generic_query_token_coverage",
            "query": "biglietto roma tunis",
            "parsed_query": {"tipo_documento": "biglietto", "persona": None, "nome_file": None},
            "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/doc9.pdf"},
            "fields": {"route": "roma tunis"},
            "semantic_score": 0.20,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": False,
            "expected_filename_only": False,
            "min_score": 0.20,
        },
        {
            "name": "no_signals_at_all",
            "query": "contratto milano",
            "parsed_query": {"tipo_documento": "contratto", "persona": None, "nome_file": None},
            "row": {"tipo_documento": "fattura", "file_path": "src/uploads/doc10.pdf"},
            "fields": {"random": "xyz"},
            "semantic_score": 0.0,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": False,
            "expected_filename_only": False,
            "max_score": 0.05,
        },
        {
            "name": "full_person_phrase_in_strong_text",
            "query": "ticket makrem slimani",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem slimani", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc11.pdf"},
            "fields": {"passenger_name": "Makrem Slimani"},
            "semantic_score": 0.25,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.80,
        },
        {
            "name": "person_with_accent",
            "query": "ticket joao",
            "parsed_query": {"tipo_documento": "ticket", "persona": "joao", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc12.pdf"},
            "fields": {"passenger_name": "João Silva"},
            "semantic_score": 0.30,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.35,
        },
        {
            "name": "multiple_person_tokens_partial_match",
            "query": "ticket nesrine ali",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine ali", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc13.pdf"},
            "fields": {"passenger_name": "Nesrine Ben Ali"},
            "semantic_score": 0.30,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.40,
        },
    ],
    ids=lambda c: c["name"],
)
def test_compute_document_score_cases(case):
    result = score_doc(
        query=case["query"],
        parsed_query=case["parsed_query"],
        row=case["row"],
        fields=case["fields"],
        semantic_score=case["semantic_score"],
        best_chunk_text=case["best_chunk_text"],
        raw_ocr_text=case["raw_ocr_text"],
    )

    assert result["persona_match"] is case["expected_persona_match"]
    assert result["filename_only_match"] is case["expected_filename_only"]

    if "min_score" in case:
        assert result["score"] >= case["min_score"]

    if "max_score" in case:
        assert result["score"] <= case["max_score"]


# =========================================================
# 7. TEST DI PRIORITÀ / RANKING
# =========================================================
def test_real_match_beats_filename_only():
    query = "ticket tuline"
    parsed_query = {"tipo_documento": "ticket", "persona": "tuline", "nome_file": None}

    docs = [
        {
            "name": "filename_only",
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/tuline_ticket.pdf"},
            "fields": {"booking_reference": "ABC"},
            "semantic_score": 0.35,
            "best_chunk_text": "Passenger Makrem",
            "raw_ocr_text": "",
        },
        {
            "name": "real_match",
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc2.pdf"},
            "fields": {"passenger_name": "Tuline Slimani"},
            "semantic_score": 0.25,
            "best_chunk_text": "Passenger Tuline Slimani",
            "raw_ocr_text": "",
        },
    ]

    ranked = rank_documents(query, parsed_query, docs)
    assert ranked[0]["name"] == "real_match"


def test_person_match_should_outweigh_semantic_only():
    query = "ticket makrem"
    parsed_query = {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None}

    docs = [
        {
            "name": "semantic_only",
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc1.pdf"},
            "fields": {"route": "milano tunis"},
            "semantic_score": 0.85,
            "best_chunk_text": "biglietto aereo milano tunis",
            "raw_ocr_text": "",
        },
        {
            "name": "real_person_match",
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc2.pdf"},
            "fields": {"passenger_name": "Makrem Slimani"},
            "semantic_score": 0.45,
            "best_chunk_text": "Passenger Makrem Slimani",
            "raw_ocr_text": "",
        },
    ]

    ranked = rank_documents(query, parsed_query, docs)
    assert ranked[0]["name"] == "real_person_match"


def test_wrong_document_type_should_not_win_even_with_name():
    query = "fattura makrem"
    parsed_query = {"tipo_documento": "fattura", "persona": "makrem", "nome_file": None}

    docs = [
        {
            "name": "invoice",
            "row": {"tipo_documento": "fattura", "file_path": "src/uploads/fattura_makrem.pdf"},
            "fields": {"cliente": "Makrem Slimani", "totale": "450"},
            "semantic_score": 0.60,
            "best_chunk_text": "Fattura Makrem totale 450",
            "raw_ocr_text": "invoice makrem 450",
        },
        {
            "name": "ticket",
            "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/ticket_makrem.pdf"},
            "fields": {"passenger_name": "Makrem Slimani"},
            "semantic_score": 0.70,
            "best_chunk_text": "Passenger Makrem",
            "raw_ocr_text": "Passenger Makrem",
        },
    ]

    ranked = rank_documents(query, parsed_query, docs)
    assert ranked[0]["name"] == "invoice"


def test_same_person_different_routes_should_pick_route_match():
    query = "ticket makrem milano tunis"
    parsed_query = {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None}

    docs = [
        {
            "name": "wrong_route",
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc1.pdf"},
            "fields": {"passenger_name": "Makrem Slimani", "route": "roma palermo"},
            "semantic_score": 0.45,
            "best_chunk_text": "Passenger Makrem route roma palermo",
            "raw_ocr_text": "",
        },
        {
            "name": "correct_route",
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc2.pdf"},
            "fields": {"passenger_name": "Makrem Slimani", "route": "milano tunis"},
            "semantic_score": 0.45,
            "best_chunk_text": "Passenger Makrem route milano tunis",
            "raw_ocr_text": "",
        },
    ]

    ranked = rank_documents(query, parsed_query, docs)
    assert ranked[0]["name"] == "correct_route"


def test_passenger_name_should_rank_above_cliente_for_ticket_query():
    query = "ticket nesrine"
    parsed_query = {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None}

    docs = [
        {
            "name": "cliente_only",
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc1.pdf"},
            "fields": {"cliente": "Nesrine Ali"},
            "semantic_score": 0.45,
            "best_chunk_text": "",
            "raw_ocr_text": "",
        },
        {
            "name": "passenger_name",
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc2.pdf"},
            "fields": {"passenger_name": "Nesrine Ali"},
            "semantic_score": 0.40,
            "best_chunk_text": "",
            "raw_ocr_text": "",
        },
    ]

    ranked = rank_documents(query, parsed_query, docs)
    assert ranked[0]["name"] == "passenger_name"


# =========================================================
# 8. CASI AMBIGUI / REGRESSIONI
# =========================================================
@pytest.mark.parametrize(
    "case",
    [
        {
            "name": "same_surname_different_person_should_pick_fuller_match",
            "query": "ticket nesrine ali",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine ali", "nome_file": None},
            "docs": [
                {
                    "name": "partial_match",
                    "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc1.pdf"},
                    "fields": {"passenger_name": "Amal Ali"},
                    "semantic_score": 0.40,
                    "best_chunk_text": "Passenger Amal Ali",
                    "raw_ocr_text": "",
                },
                {
                    "name": "fuller_match",
                    "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc2.pdf"},
                    "fields": {"passenger_name": "Nesrine Ben Ali"},
                    "semantic_score": 0.35,
                    "best_chunk_text": "Passenger Nesrine Ben Ali",
                    "raw_ocr_text": "",
                },
            ],
            "winner": "fuller_match",
        },
        {
            "name": "filename_only_should_lose_against_real_content_match",
            "query": "ticket tuline",
            "parsed_query": {"tipo_documento": "ticket", "persona": "tuline", "nome_file": None},
            "docs": [
                {
                    "name": "filename_only",
                    "row": {"tipo_documento": "ticket", "file_path": "src/uploads/tuline_doc.pdf"},
                    "fields": {"booking_reference": "123"},
                    "semantic_score": 0.50,
                    "best_chunk_text": "Passenger Makrem",
                    "raw_ocr_text": "",
                },
                {
                    "name": "content_match",
                    "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc2.pdf"},
                    "fields": {"passenger_name": "Tuline"},
                    "semantic_score": 0.30,
                    "best_chunk_text": "Passenger Tuline",
                    "raw_ocr_text": "",
                },
            ],
            "winner": "content_match",
        },
        {
            "name": "contract_with_same_person_should_not_win_for_ticket_query",
            "query": "ticket ahmed",
            "parsed_query": {"tipo_documento": "ticket", "persona": "ahmed", "nome_file": None},
            "docs": [
                {
                    "name": "contract",
                    "row": {"tipo_documento": "contratto", "file_path": "src/uploads/doc1.pdf"},
                    "fields": {"nome": "Ahmed Trabelsi"},
                    "semantic_score": 0.60,
                    "best_chunk_text": "Contratto Ahmed Trabelsi",
                    "raw_ocr_text": "",
                },
                {
                    "name": "ticket",
                    "row": {"tipo_documento": "biglietto", "file_path": "src/uploads/doc2.pdf"},
                    "fields": {"passenger_name": "Ahmed Trabelsi"},
                    "semantic_score": 0.45,
                    "best_chunk_text": "Passenger Ahmed Trabelsi",
                    "raw_ocr_text": "",
                },
            ],
            "winner": "ticket",
        },
    ],
    ids=lambda c: c["name"],
)
def test_search_ambiguity_cases(case):
    ranked = rank_documents(case["query"], case["parsed_query"], case["docs"])
    assert ranked[0]["name"] == case["winner"]


# =========================================================
# 9. TEST QUERY BUSINESS STRUTTURATE
# =========================================================
def test_structured_field_match_should_boost():
    query = "cedolino lordo 2500"
    parsed_query = {
        "tipo_documento": "cedolino",
        "persona": None,
        "nome_file": None,
        "campo_target": "lordo",
        "operatore": "=",
        "valore_target": "2500",
        "tipo_valore": "number",
        "data_da": None,
        "data_a": None,
    }

    matching = score_doc(
        query=query,
        parsed_query=parsed_query,
        row={"tipo_documento": "cedolino", "file_path": "src/uploads/c1.pdf"},
        fields={"lordo": "2500"},
        semantic_score=0.20,
        best_chunk_text="lordo 2500",
        raw_ocr_text="",
    )

    not_matching = score_doc(
        query=query,
        parsed_query=parsed_query,
        row={"tipo_documento": "cedolino", "file_path": "src/uploads/c2.pdf"},
        fields={"lordo": "1800"},
        semantic_score=0.20,
        best_chunk_text="lordo 1800",
        raw_ocr_text="",
    )

    assert matching["structured_field_match"] is True
    assert matching["score"] > not_matching["score"]


def test_structured_field_wrong_should_penalize():
    query = "bonifico importo 900"
    parsed_query = {
        "tipo_documento": "bonifico",
        "persona": None,
        "nome_file": None,
        "campo_target": "importo",
        "operatore": "=",
        "valore_target": "900",
        "tipo_valore": "number",
        "data_da": None,
        "data_a": None,
    }

    result = score_doc(
        query=query,
        parsed_query=parsed_query,
        row={"tipo_documento": "bonifico", "file_path": "src/uploads/b1.pdf"},
        fields={"importo": "500"},
        semantic_score=0.20,
        best_chunk_text="importo 500",
        raw_ocr_text="",
    )

    assert result["structured_field_match"] is False


# =========================================================
# 10. TEST BENCHMARK MINI END-TO-END
# =========================================================
@pytest.mark.parametrize(
    "case",
    [
        {
            "name": "benchmark_ticket_makrem_should_be_top1",
            "query": "biglietto makrem milano tunis",
            "parsed_query": {"tipo_documento": "biglietto", "persona": "makrem", "nome_file": None},
            "docs": [
                {
                    "name": "correct_ticket",
                    "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/makrem_ticket.pdf"},
                    "fields": {"passenger_name": "Makrem Slimani", "route": "milano tunis"},
                    "semantic_score": 0.60,
                    "best_chunk_text": "Passenger Makrem Slimani Milano Tunis",
                    "raw_ocr_text": "Passenger Makrem Slimani",
                },
                {
                    "name": "wrong_invoice",
                    "row": {"tipo_documento": "fattura", "file_path": "src/uploads/f1.pdf"},
                    "fields": {"cliente": "Makrem Slimani", "totale": "500"},
                    "semantic_score": 0.55,
                    "best_chunk_text": "Invoice Makrem 500",
                    "raw_ocr_text": "",
                },
                {
                    "name": "wrong_ticket_route",
                    "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/t2.pdf"},
                    "fields": {"passenger_name": "Makrem Slimani", "route": "roma palermo"},
                    "semantic_score": 0.55,
                    "best_chunk_text": "Passenger Makrem Roma Palermo",
                    "raw_ocr_text": "",
                },
            ],
            "winner": "correct_ticket",
        },
        {
            "name": "benchmark_invoice_query_should_choose_invoice",
            "query": "fattura makrem",
            "parsed_query": {"tipo_documento": "fattura", "persona": "makrem", "nome_file": None},
            "docs": [
                {
                    "name": "invoice",
                    "row": {"tipo_documento": "fattura", "file_path": "src/uploads/fattura_makrem.pdf"},
                    "fields": {"cliente": "Makrem Slimani", "totale": "450"},
                    "semantic_score": 0.60,
                    "best_chunk_text": "Fattura Makrem totale 450",
                    "raw_ocr_text": "invoice makrem",
                },
                {
                    "name": "ticket",
                    "row": {"tipo_documento": "biglietto", "file_path": "src/uploads/ticket_makrem.pdf"},
                    "fields": {"passenger_name": "Makrem Slimani"},
                    "semantic_score": 0.70,
                    "best_chunk_text": "Passenger Makrem",
                    "raw_ocr_text": "Passenger Makrem",
                },
            ],
            "winner": "invoice",
        },
        {
            "name": "benchmark_same_person_different_doc_types_should_choose_passport",
            "query": "passaporto nesrine",
            "parsed_query": {"tipo_documento": "passaporto", "persona": "nesrine", "nome_file": None},
            "docs": [
                {
                    "name": "passport",
                    "row": {"tipo_documento": "passaporto", "file_path": "src/uploads/p1.pdf"},
                    "fields": {"nome": "Nesrine", "cognome": "Ben Ali", "numero_passaporto": "YA123456"},
                    "semantic_score": 0.55,
                    "best_chunk_text": "Passport Nesrine Ben Ali",
                    "raw_ocr_text": "Passport Nesrine Ben Ali",
                },
                {
                    "name": "ticket",
                    "row": {"tipo_documento": "biglietto", "file_path": "src/uploads/t1.pdf"},
                    "fields": {"passenger_name": "Nesrine Ben Ali"},
                    "semantic_score": 0.62,
                    "best_chunk_text": "Passenger Nesrine",
                    "raw_ocr_text": "",
                },
            ],
            "winner": "passport",
        },
    ],
    ids=lambda c: c["name"],
)
def test_search_benchmark_cases(case):
    ranked = rank_documents(case["query"], case["parsed_query"], case["docs"])
    assert ranked[0]["name"] == case["winner"]