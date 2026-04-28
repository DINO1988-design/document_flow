# tests/test_search_logic.py

import pytest

from src.api_server import (
    normalize_text,
    tokenize_text,
    count_similar_token_matches,
    raw_ocr_person_match,
    extract_field_groups,
    has_fuzzy_person_match,
    compute_document_score,
)


# =========================================================
# normalize_text
# 20 scenari
# =========================================================
@pytest.mark.parametrize(
    "value, expected",
    [
        ("  Nesrìne-Ben/ALI  ", "nesrine ben ali"),
        ("MAKREM", "makrem"),
        ("Biglietto_Aereo", "biglietto aereo"),
        ("A/B/C", "a b c"),
        ("Rif. ABC-123", "rif abc 123"),
        ("Málaga", "malaga"),
        ("Éléonore", "eleonore"),
        ("João da Silva", "joao da silva"),
        ("Nesrine   Ben    Ali", "nesrine ben ali"),
        ("Nesrine\tBen\nAli", "nesrine ben ali"),
        ("!Ticket@2026#", "ticket 2026"),
        ("", ""),
        ("   ", ""),
        (None, ""),
        ("12345", "12345"),
        ("AZ-123/ROMA", "az 123 roma"),
        ("Cognome_Nome", "cognome nome"),
        ("L'école", "l ecole"),
        ("محمد", "محمد"),
        ("Nesrine.Ben.Ali", "nesrine ben ali"),
    ],
)
def test_normalize_text_cases(value, expected):
    assert normalize_text(value) == expected


# =========================================================
# tokenize_text
# 20 scenari
# =========================================================
@pytest.mark.parametrize(
    "value, expected",
    [
        ("Ticket Nesrine", ["ticket", "nesrine"]),
        ("Biglietto aereo per Makrem", ["biglietto", "aereo", "per", "makrem"]),
        ("Nesrine-Ben/Ali", ["nesrine", "ben", "ali"]),
        ("", []),
        ("   ", []),
        (None, []),
        ("AZ123", ["az123"]),
        ("Rif. ABC-123", ["rif", "abc", "123"]),
        ("A B C", []),  # token da 1 carattere esclusi
        ("João da Silva", ["joao", "da", "silva"]),
        ("L'école de Rome", ["ecole", "de", "rome"]),
        ("123 456", ["123", "456"]),
        ("ticket/nesrine/roma", ["ticket", "nesrine", "roma"]),
        ("Nesrine\tBen\nAli", ["nesrine", "ben", "ali"]),
        ("biglietto_aereo", ["biglietto", "aereo"]),
        ("Cognome: Slimani", ["cognome", "slimani"]),
        ("Ref#ABC123", ["ref", "abc123"]),
        ("Passenger Name NESRINE", ["passenger", "name", "nesrine"]),
        ("Volo FR-1234", ["volo", "fr", "1234"]),
        ("Makrem 2026 Tunis", ["makrem", "2026", "tunis"]),
    ],
)
def test_tokenize_text_cases(value, expected):
    assert tokenize_text(value) == expected


# =========================================================
# count_similar_token_matches
# 20 scenari
# =========================================================
@pytest.mark.parametrize(
    "query_tokens, target_text, expected",
    [
        (["nesrine"], "biglietto aereo per nesrine ben ali", 1),
        (["nesrine", "ali"], "biglietto aereo per nesrine ben ali", 2),
        (["makrem"], "Passenger Name Makrem Slimani", 1),
        (["makrem"], "Passenger Name Makrm Slimani", 1),  # fuzzy
        (["nesrine"], "Passenger Name Makrem Slimani", 0),
        (["roma"], "volo da milano a roma", 1),
        (["milan"], "volo da milano a roma", 1),
        (["abc123"], "booking reference abc123", 1),
        (["abc123"], "booking reference xyz999", 0),
        (["nesrine", "ben", "ali"], "nesrine ben ali", 3),
        (["nesrine", "ben", "ali"], "nesrine ali", 2),
        (["nesrine", "ben", "ali"], "", 0),
        ([], "nesrine ben ali", 0),
        (["joao"], "joão da silva", 1),
        (["malaga"], "málaga airport", 1),
        (["ticket"], "e-ticket ryanair", 1),
        (["volo"], "biglietto traghetto", 0),
        (["fr1234"], "flight fr1234 rome tunis", 1),
        (["tunis"], "rome tunis ferry ticket", 1),
        (["nesrine", "makrem"], "nesrine makrem slimani", 2),
    ],
)
def test_count_similar_token_matches_cases(query_tokens, target_text, expected):
    assert count_similar_token_matches(query_tokens, target_text) == expected


# =========================================================
# raw_ocr_person_match
# 20 scenari
# =========================================================
@pytest.mark.parametrize(
    "persona, raw_text, expected",
    [
        ("Nesrine", "Passenger Name: NESRINE BEN ALI", True),
        ("Nesrine", "Passenger Name: Makrem Slimani", False),
        ("Makrem", "PAX MAKREM SLIMANI", True),
        ("Makrem", "PAX MAKRM SLIMANI", True),  # fuzzy OCR
        ("Nesrine Ben Ali", "Passenger Name NESRINE BEN ALI", True),
        ("Nesrine Ben Ali", "Passenger Name NESRINE ALI", True),  # 2 token su 3
        ("Nesrine Ben Ali", "Passenger Name BEN ALI", True),
        ("Nesrine Ben Ali", "Passenger Name NESRINE", False),
        ("Joao", "Passenger JOÃO DA SILVA", True),
        ("Malaga", "Route to MÁLAGA airport", True),
        ("Nesrine", "", False),
        ("", "Passenger NESRINE", False),
        ("Nesrine", None, False),
        (None, "Passenger NESRINE", False),
        ("Ali", "Passenger Name BEN ALI", True),
        ("Slimani", "Makrem Slimani ticket", True),
        ("Tuline", "Passenger Tulin", True),
        ("Tuline", "Passenger Tuline", True),
        ("Makrem", "This file is for Marrakech travel", False),
        ("Nesrine", "N E S R I N E", False),
    ],
)
def test_raw_ocr_person_match_cases(persona, raw_text, expected):
    assert raw_ocr_person_match(persona, raw_text) is expected


# =========================================================
# extract_field_groups
# 20 scenari
# =========================================================
@pytest.mark.parametrize(
    "fields, expected_strong_contains, expected_medium_contains, expected_generic_contains",
    [
        (
            {"passenger_name": "Nesrine Ben Ali", "booking_reference": "ABC123"},
            ["nesrine ben ali"],
            [],
            ["booking reference abc123"],
        ),
        (
            {"nome_passeggero": "Makrem Slimani"},
            ["makrem slimani"],
            [],
            ["nome passeggero makrem slimani"],
        ),
        (
            {"traveler": "Nesrine Ben Ali"},
            [],
            ["nesrine ben ali"],
            ["traveler nesrine ben ali"],
        ),
        (
            {"guest_name": "Mario Rossi"},
            [],
            ["mario rossi"],
            ["guest name mario rossi"],
        ),
        (
            {"customer_name": "Fatma Ali"},
            [],
            ["fatma ali"],
            ["customer name fatma ali"],
        ),
        (
            {"full_name": "Joao Silva"},
            ["joao silva"],
            [],
            ["full name joao silva"],
        ),
        (
            {"persona": "Nesrine"},
            ["nesrine"],
            [],
            ["persona nesrine"],
        ),
        (
            {"recipient": "Ahmed"},
            [],
            ["ahmed"],
            ["recipient ahmed"],
        ),
        (
            {"email": "test@example.com"},
            [],
            [],
            ["email test example com"],
        ),
        (
            {"booking_reference": "ABC123", "ticket_number": "999"},
            [],
            [],
            ["booking reference abc123", "ticket number 999"],
        ),
        (
            {"passenger_name": "", "email": "a@b.com"},
            [],
            [],
            ["email a b com"],
        ),
        (
            {"passenger_name": None, "email": "a@b.com"},
            [],
            [],
            ["email a b com"],
        ),
        (
            {"guest": "Sara"},
            ["sara"],
            [],
            ["guest sara"],
        ),
        (
            {"holder_name": "Karim"},
            ["karim"],
            [],
            ["holder name karim"],
        ),
        (
            {"first_name": "Nesrine", "last_name": "Ben Ali"},
            [],
            ["nesrine", "ben ali"],
            ["first name nesrine", "last name ben ali"],
        ),
        (
            {"contraente": "Luca Bianchi"},
            ["luca bianchi"],
            [],
            ["contraente luca bianchi"],
        ),
        (
            {"collaboratore": "Giulia"},
            ["giulia"],
            [],
            ["collaboratore giulia"],
        ),
        (
            {"destinatario": "Paolo"},
            [],
            ["paolo"],
            ["destinatario paolo"],
        ),
        (
            {"signed_by": "Admin User"},
            ["admin user"],
            [],
            ["signed by admin user"],
        ),
        (
            {"random_field": "xyz"},
            [],
            [],
            ["random field xyz"],
        ),
    ],
)
def test_extract_field_groups_cases(
    fields,
    expected_strong_contains,
    expected_medium_contains,
    expected_generic_contains,
):
    groups = extract_field_groups(fields)

    for item in expected_strong_contains:
        assert item in groups["person_strong_text"]

    for item in expected_medium_contains:
        assert item in groups["person_medium_text"]

    for item in expected_generic_contains:
        assert item in groups["generic_text"]


# =========================================================
# has_fuzzy_person_match
# 20 scenari
# =========================================================
@pytest.mark.parametrize(
    "fields, person_tokens, expected",
    [
        ({"passenger_name": "Nesrine Ben Ali"}, ["nesrine"], True),
        ({"passenger_name": "Makrem Slimani"}, ["nesrine"], False),
        ({"passenger_name": "Makrm Slimani"}, ["makrem"], True),
        ({"traveler": "Nesrine Ben Ali"}, ["nesrine"], True),
        ({"guest_name": "Nesrine Ben Ali"}, ["ali"], True),
        ({"booking_reference": "ABC123"}, ["nesrine"], False),
        ({"email": "nesrine@example.com"}, ["nesrine"], True),
        ({"random_field": "cliente nesrine ben ali"}, ["nesrine"], True),
        ({"passenger_name": "Nesrine Ben Ali"}, ["nesrine", "ali"], True),
        ({"passenger_name": "Nesrine Ben Ali"}, ["nesrine", "tunis"], True),
        ({"passenger_name": "Fatma Zahra"}, ["nesrine", "ali"], False),
        ({"customer_name": "João Silva"}, ["joao"], True),
        ({"customer_name": "Joao Silva"}, ["joão"], True),
        ({"recipient": "Ahmed"}, ["ahmed"], True),
        ({"recipient": "Ahmed"}, ["nesrine"], False),
        ({}, ["nesrine"], False),
        ({"passenger_name": "Nesrine"}, [], False),
        ({"first_name": "Nesrine", "last_name": "Ben Ali"}, ["nesrine", "ali"], True),
        ({"passenger_name": "Nsrine Ben Ali"}, ["nesrine"], True),
        ({"passenger_name": "Ben Ali"}, ["nesrine", "ali"], True),
    ],
)
def test_has_fuzzy_person_match_cases(fields, person_tokens, expected):
    assert has_fuzzy_person_match(fields, person_tokens) is expected


# =========================================================
# compute_document_score
# 20 scenari
# =========================================================
@pytest.mark.parametrize(
    "case",
    [
        {
            "name": "strong_person_match",
            "query": "ticket nesrine",
            "parsed_query": {"tipo_documento": "e-ticket", "persona": "nesrine", "nome_file": None},
            "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/ticket_nesrine.pdf"},
            "fields": {"passenger_name": "Nesrine Ben Ali", "booking_reference": "ABC123"},
            "semantic_score": 0.70,
            "best_chunk_text": "Passenger: Nesrine Ben Ali Flight AZ123",
            "raw_ocr_text": "Passenger Name NESRINE BEN ALI Flight AZ123",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.50,
        },
        {
            "name": "filename_only_should_be_weaker",
            "query": "ticket nesrine",
            "parsed_query": {"tipo_documento": "e-ticket", "persona": "nesrine", "nome_file": None},
            "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/ticket_nesrine.pdf"},
            "fields": {"passenger_name": "Makrem Slimani", "booking_reference": "ABC123"},
            "semantic_score": 0.10,
            "best_chunk_text": "Passenger: Makrem Slimani",
            "raw_ocr_text": "Passenger Name MAKREM SLIMANI",
            "expected_persona_match": False,
            "expected_filename_only": True,
            "max_score": 0.50,
        },
        {
            "name": "medium_field_match",
            "query": "ticket ahmed",
            "parsed_query": {"tipo_documento": "ticket", "persona": "ahmed", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc1.pdf"},
            "fields": {"traveler": "Ahmed Ali"},
            "semantic_score": 0.30,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.30,
        },
        {
            "name": "generic_text_match_only",
            "query": "ticket paolo",
            "parsed_query": {"tipo_documento": "ticket", "persona": "paolo", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc2.pdf"},
            "fields": {"note": "cliente paolo rossi"},
            "semantic_score": 0.20,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.10,
        },
        {
            "name": "chunk_match_only",
            "query": "ticket nesrine",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc3.pdf"},
            "fields": {"booking_reference": "ABC"},
            "semantic_score": 0.25,
            "best_chunk_text": "Passenger NESRINE BEN ALI flight AZ123",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.20,
        },
        {
            "name": "raw_ocr_match_only",
            "query": "ticket nesrine",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc4.pdf"},
            "fields": {"booking_reference": "ABC"},
            "semantic_score": 0.10,
            "best_chunk_text": "",
            "raw_ocr_text": "Passenger NESRINE BEN ALI",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.15,
        },
        {
            "name": "no_person_match_low_semantic",
            "query": "ticket nesrine",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc5.pdf"},
            "fields": {"passenger_name": "Makrem Slimani"},
            "semantic_score": 0.05,
            "best_chunk_text": "Passenger Makrem Slimani",
            "raw_ocr_text": "Passenger Makrem Slimani",
            "expected_persona_match": False,
            "expected_filename_only": False,
            "max_score": 0.20,
        },
        {
            "name": "tipo_match_ticket_alias",
            "query": "biglietto nesrine",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/doc6.pdf"},
            "fields": {"passenger_name": "Nesrine"},
            "semantic_score": 0.20,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.30,
        },
        {
            "name": "full_person_phrase_in_strong_text",
            "query": "ticket nesrine ben ali",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine ben ali", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc7.pdf"},
            "fields": {"passenger_name": "Nesrine Ben Ali"},
            "semantic_score": 0.20,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.45,
        },
        {
            "name": "full_person_phrase_in_medium_text",
            "query": "ticket ahmed ali",
            "parsed_query": {"tipo_documento": "ticket", "persona": "ahmed ali", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc8.pdf"},
            "fields": {"traveler": "Ahmed Ali"},
            "semantic_score": 0.20,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.25,
        },
        {
            "name": "full_person_phrase_in_generic_text",
            "query": "ticket mario rossi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "mario rossi", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc9.pdf"},
            "fields": {"note": "cliente mario rossi"},
            "semantic_score": 0.20,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": True,
            "expected_filename_only": False,
            "min_score": 0.20,
        },
        {
            "name": "nome_file_explicit_match",
            "query": "documento volo",
            "parsed_query": {"tipo_documento": None, "persona": None, "nome_file": "volo_roma"},
            "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/volo_roma_2026.pdf"},
            "fields": {"booking_reference": "ABC"},
            "semantic_score": 0.20,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": False,
            "expected_filename_only": False,
            "min_score": 0.10,
        },
        {
            "name": "text_query_found_in_searchable_text",
            "query": "booking abc123",
            "parsed_query": {"tipo_documento": None, "persona": None, "nome_file": None},
            "row": {"tipo_documento": "prenotazione", "file_path": "src/uploads/doc10.pdf"},
            "fields": {"booking_reference": "ABC123"},
            "semantic_score": 0.10,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": False,
            "expected_filename_only": False,
            "min_score": 0.10,
        },
        {
            "name": "high_semantic_without_person",
            "query": "ticket nesrine",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc11.pdf"},
            "fields": {"passenger_name": "Makrem"},
            "semantic_score": 0.90,
            "best_chunk_text": "boarding pass generic",
            "raw_ocr_text": "",
            "expected_persona_match": False,
            "expected_filename_only": False,
            "max_score": 0.50,
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
            "name": "person_only_in_filename_no_real_match",
            "query": "ticket tuline",
            "parsed_query": {"tipo_documento": "ticket", "persona": "tuline", "nome_file": None},
            "row": {"tipo_documento": "ticket", "file_path": "src/uploads/tuline_ticket.pdf"},
            "fields": {"booking_reference": "ABC"},
            "semantic_score": 0.39,
            "best_chunk_text": "Passenger Makrem",
            "raw_ocr_text": "",
            "expected_persona_match": False,
            "expected_filename_only": True,
            "max_score": 0.40,
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
        {
            "name": "no_signals_at_all",
            "query": "contratto milano",
            "parsed_query": {"tipo_documento": "contratto", "persona": None, "nome_file": None},
            "row": {"tipo_documento": "fattura", "file_path": "src/uploads/doc14.pdf"},
            "fields": {"random": "xyz"},
            "semantic_score": 0.0,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": False,
            "expected_filename_only": False,
            "max_score": 0.05,
        },
        {
            "name": "tipo_match_without_person_query",
            "query": "ticket ryanair",
            "parsed_query": {"tipo_documento": "ticket", "persona": None, "nome_file": None},
            "row": {"tipo_documento": "ticket ryanair", "file_path": "src/uploads/doc15.pdf"},
            "fields": {"booking_reference": "FR123"},
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
            "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/doc16.pdf"},
            "fields": {"route": "roma tunis"},
            "semantic_score": 0.20,
            "best_chunk_text": "",
            "raw_ocr_text": "",
            "expected_persona_match": False,
            "expected_filename_only": False,
            "min_score": 0.20,
        },
    ],
    ids=lambda c: c["name"],
)
def test_compute_document_score_cases(case):
    result = compute_document_score(
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