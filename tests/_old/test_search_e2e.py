# tests/test_search_e2e.py

import os
import pytest

from src.api_server import compute_document_score


def rank_documents(query, parsed_query, documents):
    ranked = []

    for doc in documents:
        result = compute_document_score(
            query=query,
            parsed_query=parsed_query,
            row=doc["row"],
            fields=doc["fields"],
            semantic_score=doc.get("semantic_score", 0.0),
            best_chunk_text=doc.get("best_chunk_text", ""),
            raw_ocr_text=doc.get("raw_ocr_text", ""),
        )

        ranked.append(
            {
                "doc_id": doc["doc_id"],
                "score": result["score"],
                "persona_match": result["persona_match"],
                "filename_only_match": result["filename_only_match"],
            }
        )

    ranked.sort(key=lambda x: x["score"], reverse=True)
    return ranked


@pytest.fixture
def sample_documents():
    return [
        {
            "doc_id": "doc_nesrine_ticket",
            "row": {
                "tipo_documento": "biglietto aereo",
                "file_path": "src/uploads/ticket_nesrine_roma_tunis.pdf",
            },
            "fields": {
                "passenger_name": "Nesrine Ben Ali",
                "route": "Roma Tunis",
                "booking_reference": "ABC123",
            },
            "semantic_score": 0.82,
            "best_chunk_text": "Passenger Nesrine Ben Ali flight Rome Tunis",
            "raw_ocr_text": "Passenger Name NESRINE BEN ALI Flight Rome Tunis",
        },
        {
            "doc_id": "doc_makrem_ticket",
            "row": {
                "tipo_documento": "biglietto aereo",
                "file_path": "src/uploads/ticket_makrem_milano_roma.pdf",
            },
            "fields": {
                "passenger_name": "Makrem Slimani",
                "route": "Milano Roma",
                "booking_reference": "XYZ999",
            },
            "semantic_score": 0.74,
            "best_chunk_text": "Passenger Makrem Slimani flight Milano Roma",
            "raw_ocr_text": "Passenger Name MAKREM SLIMANI Flight Milano Roma",
        },
        {
            "doc_id": "doc_tuline_ticket",
            "row": {
                "tipo_documento": "ticket",
                "file_path": "src/uploads/tuline_ticket.pdf",
            },
            "fields": {
                "passenger_name": "Tuline",
                "route": "Paris Tunis",
            },
            "semantic_score": 0.65,
            "best_chunk_text": "Passenger Tuline route Paris Tunis",
            "raw_ocr_text": "Passenger Tulin route Paris Tunis",
        },
        {
            "doc_id": "doc_ferry_amna",
            "row": {
                "tipo_documento": "biglietto traghetto",
                "file_path": "src/uploads/ferry_zaafouri_amna.pdf",
            },
            "fields": {
                "nome_passeggero": "Zaafouri Amna",
                "route": "Genova Tunisi",
            },
            "semantic_score": 0.68,
            "best_chunk_text": "Passenger Zaafouri Amna ferry Genova Tunisi",
            "raw_ocr_text": "PAX ZAAFOURI/AMNA",
        },
        {
            "doc_id": "doc_contract_ahmed",
            "row": {
                "tipo_documento": "contratto",
                "file_path": "src/uploads/contratto_ahmed.pdf",
            },
            "fields": {
                "contraente": "Ahmed Ali",
                "societa": "Altea S.p.A.",
            },
            "semantic_score": 0.58,
            "best_chunk_text": "Contratto Ahmed Ali Altea",
            "raw_ocr_text": "Ahmed Ali contratto",
        },
        {
            "doc_id": "doc_generic_noise",
            "row": {
                "tipo_documento": "fattura",
                "file_path": "src/uploads/fattura_random.pdf",
            },
            "fields": {
                "cliente": "Mario Rossi",
                "note": "documento amministrativo generico",
            },
            "semantic_score": 0.10,
            "best_chunk_text": "Administrative invoice",
            "raw_ocr_text": "Generic document",
        },
    ]


@pytest.mark.parametrize(
    "case",
    [
        {
            "name": "exact_person_ticket_should_rank_first",
            "query": "biglietto nesrine",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "expected_first": "doc_nesrine_ticket",
        },
        {
            "name": "ocr_variation_should_still_find_tuline",
            "query": "ticket tuline",
            "parsed_query": {"tipo_documento": "ticket", "persona": "tuline", "nome_file": None},
            "expected_first": "doc_tuline_ticket",
        },
        {
            "name": "route_and_person_should_find_makrem",
            "query": "biglietto makrem milano roma",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "expected_first": "doc_makrem_ticket",
        },
        {
            "name": "ferry_passenger_should_find_amna",
            "query": "traghetto amna tunisi",
            "parsed_query": {"tipo_documento": "biglietto", "persona": "amna", "nome_file": None},
            "expected_first": "doc_ferry_amna",
        },
        {
            "name": "contract_should_not_be_beaten_by_ticket_docs",
            "query": "contratto ahmed",
            "parsed_query": {"tipo_documento": "contratto", "persona": "ahmed", "nome_file": None},
            "expected_first": "doc_contract_ahmed",
        },
        {
            "name": "partial_name_should_still_find_nesrine_ticket",
            "query": "ticket ali",
            "parsed_query": {"tipo_documento": "ticket", "persona": "ali", "nome_file": None},
            "expected_first": "doc_nesrine_ticket",
        },
        {
            "name": "translation_like_milan_milano_should_find_makrem",
            "query": "ticket makrem milan roma",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "expected_first": "doc_makrem_ticket",
        },
        {
            "name": "filename_signal_should_help_but_not_overcome_real_match",
            "query": "ticket nesrine",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "expected_first": "doc_nesrine_ticket",
        },
        {
            "name": "generic_noise_should_stay_low",
            "query": "ticket nesrine roma tunis",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "expected_not_first": "doc_generic_noise",
        },
        {
            "name": "person_with_compound_name_should_match",
            "query": "ticket zaafouri amna",
            "parsed_query": {"tipo_documento": "biglietto", "persona": "zaafouri amna", "nome_file": None},
            "expected_first": "doc_ferry_amna",
        },
    ],
    ids=lambda c: c["name"],
)
def test_search_ranking_e2e(case, sample_documents):
    ranked = rank_documents(
        query=case["query"],
        parsed_query=case["parsed_query"],
        documents=sample_documents,
    )

    top_doc_id = ranked[0]["doc_id"]

    if "expected_first" in case:
        assert top_doc_id == case["expected_first"]

    if "expected_not_first" in case:
        assert top_doc_id != case["expected_not_first"]


def test_real_match_beats_filename_only():
    documents = [
        {
            "doc_id": "real_match",
            "row": {
                "tipo_documento": "ticket",
                "file_path": "src/uploads/doc_real.pdf",
            },
            "fields": {
                "passenger_name": "Nesrine Ben Ali",
            },
            "semantic_score": 0.45,
            "best_chunk_text": "Passenger Nesrine Ben Ali",
            "raw_ocr_text": "Passenger NESRINE BEN ALI",
        },
        {
            "doc_id": "filename_only",
            "row": {
                "tipo_documento": "ticket",
                "file_path": "src/uploads/nesrine_ticket_file.pdf",
            },
            "fields": {
                "passenger_name": "Makrem Slimani",
            },
            "semantic_score": 0.20,
            "best_chunk_text": "Passenger Makrem Slimani",
            "raw_ocr_text": "Passenger MAKREM SLIMANI",
        },
    ]

    ranked = rank_documents(
        query="ticket nesrine",
        parsed_query={"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
        documents=documents,
    )

    assert ranked[0]["doc_id"] == "real_match"
    assert ranked[1]["doc_id"] == "filename_only"


def test_person_match_should_outweigh_semantic_only():
    documents = [
        {
            "doc_id": "semantic_only",
            "row": {
                "tipo_documento": "ticket",
                "file_path": "src/uploads/doc_semantic.pdf",
            },
            "fields": {
                "passenger_name": "Other Person",
            },
            "semantic_score": 0.95,
            "best_chunk_text": "Generic travel ticket boarding document",
            "raw_ocr_text": "",
        },
        {
            "doc_id": "person_match",
            "row": {
                "tipo_documento": "ticket",
                "file_path": "src/uploads/doc_person.pdf",
            },
            "fields": {
                "passenger_name": "Makrem Slimani",
            },
            "semantic_score": 0.55,
            "best_chunk_text": "Passenger Makrem Slimani",
            "raw_ocr_text": "Passenger Name MAKREM SLIMANI",
        },
    ]

    ranked = rank_documents(
        query="ticket makrem",
        parsed_query={"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
        documents=documents,
    )

    assert ranked[0]["doc_id"] == "person_match"


def test_wrong_document_type_should_not_win_even_with_name():
    documents = [
        {
            "doc_id": "wrong_type",
            "row": {
                "tipo_documento": "fattura",
                "file_path": "src/uploads/nesrine_invoice.pdf",
            },
            "fields": {
                "cliente": "Nesrine Ben Ali",
            },
            "semantic_score": 0.60,
            "best_chunk_text": "Invoice Nesrine Ben Ali",
            "raw_ocr_text": "Nesrine Ben Ali",
        },
        {
            "doc_id": "correct_type",
            "row": {
                "tipo_documento": "biglietto aereo",
                "file_path": "src/uploads/ticket_nesrine.pdf",
            },
            "fields": {
                "passenger_name": "Nesrine Ben Ali",
            },
            "semantic_score": 0.55,
            "best_chunk_text": "Passenger Nesrine Ben Ali",
            "raw_ocr_text": "Passenger NESRINE BEN ALI",
        },
    ]

    ranked = rank_documents(
        query="biglietto nesrine",
        parsed_query={"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
        documents=documents,
    )

    assert ranked[0]["doc_id"] == "correct_type"