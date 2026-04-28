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
                "tipo_match": result["tipo_match"],
                "filename_only_match": result["filename_only_match"],
            }
        )

    ranked.sort(key=lambda x: x["score"], reverse=True)
    return ranked


@pytest.mark.parametrize(
    "case",
    [
        {
            "name": "same_surname_different_person_should_pick_fuller_match",
            "query": "ticket nesrine ali",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine ali", "nome_file": None},
            "documents": [
                {
                    "doc_id": "doc_nesrine_ben_ali",
                    "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/doc1.pdf"},
                    "fields": {"passenger_name": "Nesrine Ben Ali"},
                    "semantic_score": 0.65,
                    "best_chunk_text": "Passenger Nesrine Ben Ali Rome Tunis",
                    "raw_ocr_text": "Passenger NESRINE BEN ALI",
                },
                {
                    "doc_id": "doc_fatma_ali",
                    "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/doc2.pdf"},
                    "fields": {"passenger_name": "Fatma Ali"},
                    "semantic_score": 0.60,
                    "best_chunk_text": "Passenger Fatma Ali Milan Paris",
                    "raw_ocr_text": "Passenger FATMA ALI",
                },
            ],
            "expected_first": "doc_nesrine_ben_ali",
        },
        {
            "name": "same_name_wrong_document_type_should_lose",
            "query": "biglietto ahmed",
            "parsed_query": {"tipo_documento": "ticket", "persona": "ahmed", "nome_file": None},
            "documents": [
                {
                    "doc_id": "doc_invoice_ahmed",
                    "row": {"tipo_documento": "fattura", "file_path": "src/uploads/ahmed_invoice.pdf"},
                    "fields": {"cliente": "Ahmed Ali"},
                    "semantic_score": 0.72,
                    "best_chunk_text": "Invoice Ahmed Ali",
                    "raw_ocr_text": "Ahmed Ali invoice",
                },
                {
                    "doc_id": "doc_ticket_ahmed",
                    "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/ahmed_ticket.pdf"},
                    "fields": {"passenger_name": "Ahmed Ali"},
                    "semantic_score": 0.60,
                    "best_chunk_text": "Passenger Ahmed Ali flight Rome Cairo",
                    "raw_ocr_text": "Passenger AHMED ALI",
                },
            ],
            "expected_first": "doc_ticket_ahmed",
        },
        {
            "name": "same_person_two_routes_should_pick_route_match",
            "query": "ticket makrem milano roma",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "documents": [
                {
                    "doc_id": "doc_route_match",
                    "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/makrem_milano_roma.pdf"},
                    "fields": {"passenger_name": "Makrem Slimani", "route": "Milano Roma"},
                    "semantic_score": 0.64,
                    "best_chunk_text": "Passenger Makrem Slimani route Milano Roma",
                    "raw_ocr_text": "Passenger MAKREM SLIMANI Milano Roma",
                },
                {
                    "doc_id": "doc_other_route",
                    "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/makrem_paris_tunis.pdf"},
                    "fields": {"passenger_name": "Makrem Slimani", "route": "Paris Tunis"},
                    "semantic_score": 0.58,
                    "best_chunk_text": "Passenger Makrem Slimani route Paris Tunis",
                    "raw_ocr_text": "Passenger MAKREM SLIMANI Paris Tunis",
                },
            ],
            "expected_first": "doc_route_match",
        },
        {
            "name": "filename_only_should_lose_against_real_content_match",
            "query": "ticket tuline",
            "parsed_query": {"tipo_documento": "ticket", "persona": "tuline", "nome_file": None},
            "documents": [
                {
                    "doc_id": "doc_filename_only",
                    "row": {"tipo_documento": "ticket", "file_path": "src/uploads/tuline_old_name.pdf"},
                    "fields": {"passenger_name": "Makrem Slimani"},
                    "semantic_score": 0.40,
                    "best_chunk_text": "Passenger Makrem Slimani",
                    "raw_ocr_text": "Passenger MAKREM SLIMANI",
                },
                {
                    "doc_id": "doc_real_match",
                    "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc_real.pdf"},
                    "fields": {"passenger_name": "Tuline"},
                    "semantic_score": 0.36,
                    "best_chunk_text": "Passenger Tulin route Paris Tunis",
                    "raw_ocr_text": "Passenger Tulin",
                },
            ],
            "expected_first": "doc_real_match",
        },
        {
            "name": "partial_name_should_not_beat_better_full_match",
            "query": "ticket ali",
            "parsed_query": {"tipo_documento": "ticket", "persona": "ali", "nome_file": None},
            "documents": [
                {
                    "doc_id": "doc_nesrine_ben_ali",
                    "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc_a.pdf"},
                    "fields": {"passenger_name": "Nesrine Ben Ali"},
                    "semantic_score": 0.66,
                    "best_chunk_text": "Passenger Nesrine Ben Ali Rome Tunis",
                    "raw_ocr_text": "Passenger NESRINE BEN ALI",
                },
                {
                    "doc_id": "doc_ali_only_weak",
                    "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc_b.pdf"},
                    "fields": {"note": "cliente ali"},
                    "semantic_score": 0.25,
                    "best_chunk_text": "Generic ticket info",
                    "raw_ocr_text": "",
                },
            ],
            "expected_first": "doc_nesrine_ben_ali",
        },
        {
            "name": "ocr_dirty_query_should_find_best_person",
            "query": "ticket makrm",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrm", "nome_file": None},
            "documents": [
                {
                    "doc_id": "doc_makrem",
                    "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc_makrem.pdf"},
                    "fields": {"passenger_name": "Makrem Slimani"},
                    "semantic_score": 0.57,
                    "best_chunk_text": "Passenger Makrem Slimani",
                    "raw_ocr_text": "Passenger MAKRM SLIMANI",
                },
                {
                    "doc_id": "doc_mariam",
                    "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc_mariam.pdf"},
                    "fields": {"passenger_name": "Mariam Slimani"},
                    "semantic_score": 0.56,
                    "best_chunk_text": "Passenger Mariam Slimani",
                    "raw_ocr_text": "Passenger MARIAM SLIMANI",
                },
            ],
            "expected_first": "doc_makrem",
        },
        {
            "name": "city_alias_should_help_but_not_override_person",
            "query": "ticket nesrine milan roma",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "documents": [
                {
                    "doc_id": "doc_nesrine_milano",
                    "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc_nesrine.pdf"},
                    "fields": {"passenger_name": "Nesrine Ben Ali", "route": "Milano Roma"},
                    "semantic_score": 0.60,
                    "best_chunk_text": "Passenger Nesrine Ben Ali route Milano Roma",
                    "raw_ocr_text": "Passenger NESRINE BEN ALI",
                },
                {
                    "doc_id": "doc_other_person_milano",
                    "row": {"tipo_documento": "ticket", "file_path": "src/uploads/doc_other.pdf"},
                    "fields": {"passenger_name": "Fatma Zahra", "route": "Milano Roma"},
                    "semantic_score": 0.63,
                    "best_chunk_text": "Passenger Fatma Zahra route Milano Roma",
                    "raw_ocr_text": "Passenger FATMA ZAHRA",
                },
            ],
            "expected_first": "doc_nesrine_milano",
        },
        {
            "name": "contract_with_same_person_should_not_win_for_ticket_query",
            "query": "ticket nesrine",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "documents": [
                {
                    "doc_id": "doc_contract",
                    "row": {"tipo_documento": "contratto", "file_path": "src/uploads/contract_nesrine.pdf"},
                    "fields": {"contraente": "Nesrine Ben Ali"},
                    "semantic_score": 0.70,
                    "best_chunk_text": "Contratto Nesrine Ben Ali",
                    "raw_ocr_text": "Nesrine Ben Ali contratto",
                },
                {
                    "doc_id": "doc_ticket",
                    "row": {"tipo_documento": "biglietto aereo", "file_path": "src/uploads/ticket_nesrine.pdf"},
                    "fields": {"passenger_name": "Nesrine Ben Ali"},
                    "semantic_score": 0.58,
                    "best_chunk_text": "Passenger Nesrine Ben Ali flight",
                    "raw_ocr_text": "Passenger NESRINE BEN ALI",
                },
            ],
            "expected_first": "doc_ticket",
        },
    ],
    ids=lambda c: c["name"],
)
def test_search_ambiguity_cases(case):
    ranked = rank_documents(
        query=case["query"],
        parsed_query=case["parsed_query"],
        documents=case["documents"],
    )

    assert ranked[0]["doc_id"] == case["expected_first"]