# tests/test_search_logic.py

from src.api_server import (
    normalize_text,
    tokenize_text,
    count_similar_token_matches,
    raw_ocr_person_match,
    extract_field_groups,
    has_fuzzy_person_match,
    compute_document_score,
)


def test_normalize_text_basic():
    assert normalize_text("  Nesrìne-Ben/ALI  ") == "nesrine ben ali"


def test_tokenize_text_basic():
    assert tokenize_text("Ticket Nesrine") == ["ticket", "nesrine"]


def test_count_similar_token_matches_exact():
    query_tokens = ["nesrine"]
    target_text = "biglietto aereo per nesrine ben ali"
    assert count_similar_token_matches(query_tokens, target_text) == 1


def test_raw_ocr_person_match_exact():
    persona = "Nesrine"
    raw_text = "Passenger Name: NESRINE BEN ALI"
    assert raw_ocr_person_match(persona, raw_text) is True


def test_raw_ocr_person_match_false():
    persona = "Nesrine"
    raw_text = "Passenger Name: Makrem Slimani"
    assert raw_ocr_person_match(persona, raw_text) is False


def test_extract_field_groups():
    fields = {
        "passenger_name": "Nesrine Ben Ali",
        "booking_reference": "ABC123",
        "email": "test@example.com"
    }

    groups = extract_field_groups(fields)

    assert "nesrine ben ali" in groups["person_strong_text"]
    assert "booking reference abc123" in groups["generic_text"] or "booking_reference abc123" in groups["generic_text"]


def test_has_fuzzy_person_match_true():
    fields = {
        "passenger_name": "Nesrine Ben Ali",
        "booking_reference": "ABC123",
    }
    person_tokens = ["nesrine"]

    assert has_fuzzy_person_match(fields, person_tokens) is True


def test_has_fuzzy_person_match_false():
    fields = {
        "passenger_name": "Makrem Slimani",
        "booking_reference": "ABC123",
    }
    person_tokens = ["nesrine"]

    assert has_fuzzy_person_match(fields, person_tokens) is False


def test_compute_document_score_with_person_match():
    query = "ticket nesrine"
    parsed_query = {
        "tipo_documento": "e-ticket",
        "persona": "nesrine",
        "nome_file": None
    }
    row = {
        "tipo_documento": "biglietto aereo",
        "file_path": "src/uploads/ticket_nesrine.pdf"
    }
    fields = {
        "passenger_name": "Nesrine Ben Ali",
        "booking_reference": "ABC123"
    }

    result = compute_document_score(
        query=query,
        parsed_query=parsed_query,
        row=row,
        fields=fields,
        semantic_score=0.70,
        best_chunk_text="Passenger: Nesrine Ben Ali Flight AZ123",
        raw_ocr_text="Passenger Name NESRINE BEN ALI Flight AZ123"
    )

    assert result["persona_match"] is True
    assert result["score"] > 0.5
    assert result["persona_token_matches_strong"] >= 1


def test_compute_document_score_filename_only_should_be_weaker():
    query = "ticket nesrine"
    parsed_query = {
        "tipo_documento": "e-ticket",
        "persona": "nesrine",
        "nome_file": None
    }
    row = {
        "tipo_documento": "biglietto aereo",
        "file_path": "src/uploads/ticket_nesrine.pdf"
    }
    fields = {
        "passenger_name": "Makrem Slimani",
        "booking_reference": "ABC123"
    }

    result = compute_document_score(
        query=query,
        parsed_query=parsed_query,
        row=row,
        fields=fields,
        semantic_score=0.10,
        best_chunk_text="Passenger: Makrem Slimani",
        raw_ocr_text="Passenger Name MAKREM SLIMANI"
    )

    assert result["persona_match"] is False
    assert result["filename_only_match"] is True
    assert result["score"] < 0.5