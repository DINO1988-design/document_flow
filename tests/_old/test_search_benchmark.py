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
                "score_breakdown": result.get("score_breakdown"),
            }
        )

    ranked.sort(key=lambda x: x["score"], reverse=True)
    return ranked


@pytest.fixture
def benchmark_documents():
    return [
        {
            "doc_id": "ticket_makrem_milano_tunisi",
            "row": {
                "tipo_documento": "biglietto aereo",
                "file_path": "src/uploads/ticket_makrem_milano_tunisi.pdf",
            },
            "fields": {
                "passenger_name": "Makrem Slimani",
                "route": "Milano Tunisi",
                "booking_reference": "FR7621",
                "compagnia": "Ryanair",
            },
            "semantic_score": 0.84,
            "best_chunk_text": "Passenger Makrem Slimani flight Milano Tunisi Ryanair FR7621",
            "raw_ocr_text": "Passenger MAKREM SLIMANI Milano Tunisi FR7621",
        },
        {
            "doc_id": "ticket_makrem_roma_parigi",
            "row": {
                "tipo_documento": "ticket",
                "file_path": "src/uploads/ticket_makrem_roma_parigi.pdf",
            },
            "fields": {
                "passenger_name": "Makrem Slimani",
                "route": "Roma Parigi",
                "booking_reference": "AZ9912",
                "compagnia": "ITA Airways",
            },
            "semantic_score": 0.77,
            "best_chunk_text": "Passenger Makrem Slimani flight Roma Parigi AZ9912",
            "raw_ocr_text": "Passenger MAKREM SLIMANI Roma Parigi",
        },
        {
            "doc_id": "ticket_nesrine_roma_tunisi",
            "row": {
                "tipo_documento": "biglietto aereo",
                "file_path": "src/uploads/ticket_nesrine_roma_tunisi.pdf",
            },
            "fields": {
                "passenger_name": "Nesrine Ben Ali",
                "route": "Roma Tunisi",
                "booking_reference": "TU1455",
                "compagnia": "Tunisair",
            },
            "semantic_score": 0.82,
            "best_chunk_text": "Passenger Nesrine Ben Ali Roma Tunisi Tunisair TU1455",
            "raw_ocr_text": "Passenger NESRINE BEN ALI Roma Tunisi TU1455",
        },
        {
            "doc_id": "ticket_fatma_parigi_tunisi",
            "row": {
                "tipo_documento": "e-ticket",
                "file_path": "src/uploads/ticket_fatma_parigi_tunisi.pdf",
            },
            "fields": {
                "passenger_name": "Fatma Zahra",
                "route": "Parigi Tunisi",
                "booking_reference": "BJ3310",
                "compagnia": "Nouvelair",
            },
            "semantic_score": 0.75,
            "best_chunk_text": "Passenger Fatma Zahra Parigi Tunisi Nouvelair BJ3310",
            "raw_ocr_text": "Passenger FATMA ZAHRA Parigi Tunisi BJ3310",
        },
        {
            "doc_id": "ferry_amna_genova_tunisi",
            "row": {
                "tipo_documento": "biglietto traghetto",
                "file_path": "src/uploads/ferry_amna_genova_tunisi.pdf",
            },
            "fields": {
                "nome_passeggero": "Zaafouri Amna",
                "route": "Genova Tunisi",
                "compagnia": "GNV",
            },
            "semantic_score": 0.73,
            "best_chunk_text": "Passeggero Zaafouri Amna traghetto Genova Tunisi GNV",
            "raw_ocr_text": "PAX ZAAFOURI/AMNA GENOVA TUNISI",
        },
        {
            "doc_id": "ferry_ahmed_palermo_tunisi",
            "row": {
                "tipo_documento": "traghetto",
                "file_path": "src/uploads/ferry_ahmed_palermo_tunisi.pdf",
            },
            "fields": {
                "nome_passeggero": "Ahmed Ali",
                "route": "Palermo Tunisi",
                "compagnia": "Grimaldi",
            },
            "semantic_score": 0.71,
            "best_chunk_text": "Ahmed Ali traghetto Palermo Tunisi Grimaldi",
            "raw_ocr_text": "PAX AHMED ALI PALERMO TUNISI",
        },
        {
            "doc_id": "passport_makrem",
            "row": {
                "tipo_documento": "passaporto",
                "file_path": "src/uploads/passport_makrem.pdf",
            },
            "fields": {
                "nome": "Makrem",
                "cognome": "Slimani",
                "numero_passaporto": "XK123456",
                "nazionalita": "Tunisina",
            },
            "semantic_score": 0.72,
            "best_chunk_text": "Passport Makrem Slimani XK123456 Tunisina",
            "raw_ocr_text": "PASSPORT MAKREM SLIMANI XK123456",
        },
        {
            "doc_id": "passport_nesrine",
            "row": {
                "tipo_documento": "passport",
                "file_path": "src/uploads/passport_nesrine.pdf",
            },
            "fields": {
                "nome": "Nesrine",
                "cognome": "Ben Ali",
                "numero_passaporto": "YA987654",
                "nazionalita": "Tunisina",
            },
            "semantic_score": 0.71,
            "best_chunk_text": "Passport Nesrine Ben Ali YA987654 Tunisina",
            "raw_ocr_text": "PASSPORT NESRINE BEN ALI YA987654",
        },
        {
            "doc_id": "contract_ahmed_altea",
            "row": {
                "tipo_documento": "contratto",
                "file_path": "src/uploads/contratto_ahmed_altea.pdf",
            },
            "fields": {
                "contraente": "Ahmed Ali",
                "societa": "Altea S.p.A.",
                "tipo_contratto": "tempo indeterminato",
            },
            "semantic_score": 0.69,
            "best_chunk_text": "Contratto Ahmed Ali Altea tempo indeterminato",
            "raw_ocr_text": "Ahmed Ali contratto Altea",
        },
        {
            "doc_id": "contract_nesrine_delta",
            "row": {
                "tipo_documento": "contratto",
                "file_path": "src/uploads/contratto_nesrine_delta.pdf",
            },
            "fields": {
                "contraente": "Nesrine Ben Ali",
                "societa": "Studio Delta",
                "tipo_contratto": "consulenza",
            },
            "semantic_score": 0.66,
            "best_chunk_text": "Contratto consulenza Nesrine Ben Ali Studio Delta",
            "raw_ocr_text": "Nesrine Ben Ali contratto consulenza",
        },
        {
            "doc_id": "invoice_ahmed",
            "row": {
                "tipo_documento": "fattura",
                "file_path": "src/uploads/fattura_ahmed.pdf",
            },
            "fields": {
                "cliente": "Ahmed Ali",
                "totale": "1250.00",
                "fornitore": "Studio Delta",
            },
            "semantic_score": 0.63,
            "best_chunk_text": "Fattura Ahmed Ali totale 1250 Studio Delta",
            "raw_ocr_text": "Ahmed Ali invoice 1250",
        },
        {
            "doc_id": "invoice_nesrine",
            "row": {
                "tipo_documento": "fattura",
                "file_path": "src/uploads/fattura_nesrine.pdf",
            },
            "fields": {
                "cliente": "Nesrine Ben Ali",
                "totale": "890.00",
                "fornitore": "Travel Group",
            },
            "semantic_score": 0.61,
            "best_chunk_text": "Fattura Nesrine Ben Ali totale 890 Travel Group",
            "raw_ocr_text": "Nesrine Ben Ali invoice 890",
        },
        {
            "doc_id": "generic_internal_doc",
            "row": {
                "tipo_documento": "documento generico",
                "file_path": "src/uploads/admin_doc.pdf",
            },
            "fields": {
                "note": "documento amministrativo interno",
                "cliente": "Mario Rossi",
            },
            "semantic_score": 0.18,
            "best_chunk_text": "Administrative internal generic document",
            "raw_ocr_text": "generic document",
        },
    ]


@pytest.mark.parametrize(
    "case",
    [
        {
            "name": "benchmark_ticket_makrem_should_be_top1",
            "query": "biglietto makrem milano tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "expected_top1": "ticket_makrem_milano_tunisi",
        },
        {
            "name": "benchmark_ticket_makrem_route_specific_should_prefer_correct_route",
            "query": "ticket makrem roma parigi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "expected_top1": "ticket_makrem_roma_parigi",
        },
        {
            "name": "benchmark_nesrine_ticket_should_not_be_beaten_by_invoice",
            "query": "ticket nesrine",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "expected_top1": "ticket_nesrine_roma_tunisi",
            "must_not_top1": "invoice_nesrine",
        },
        {
            "name": "benchmark_amna_ferry_should_be_top1",
            "query": "traghetto amna genova tunisi",
            "parsed_query": {"tipo_documento": "biglietto", "persona": "amna", "nome_file": None},
            "expected_top1": "ferry_amna_genova_tunisi",
        },
        {
            "name": "benchmark_ahmed_contract_should_be_top1_for_contract_query",
            "query": "contratto ahmed altea",
            "parsed_query": {"tipo_documento": "contratto", "persona": "ahmed", "nome_file": None},
            "expected_top1": "contract_ahmed_altea",
        },
        {
            "name": "benchmark_ahmed_ferry_should_be_in_top3_for_palermo_tunisi",
            "query": "biglietto ahmed palermo tunisi",
            "parsed_query": {"tipo_documento": "biglietto", "persona": "ahmed", "nome_file": None},
            "expected_in_top3": "ferry_ahmed_palermo_tunisi",
        },
        {
            "name": "benchmark_makrem_passport_should_be_top1",
            "query": "passaporto makrem",
            "parsed_query": {"tipo_documento": "passaporto", "persona": "makrem", "nome_file": None},
            "expected_top1": "passport_makrem",
        },
        {
            "name": "benchmark_nesrine_passport_number_should_be_in_top3",
            "query": "passport nesrine YA987654",
            "parsed_query": {"tipo_documento": "passport", "persona": "nesrine", "nome_file": None},
            "expected_in_top3": "passport_nesrine",
        },
        {
            "name": "benchmark_fatma_ticket_should_be_top1",
            "query": "e ticket fatma parigi tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "fatma", "nome_file": None},
            "expected_top1": "ticket_fatma_parigi_tunisi",
        },
        {
            "name": "benchmark_generic_doc_should_not_be_in_top3",
            "query": "ticket makrem",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "must_not_be_in_top3": "generic_internal_doc",
        },
        {
            "name": "benchmark_same_person_different_doc_types_should_choose_passport",
            "query": "passaporto nesrine",
            "parsed_query": {"tipo_documento": "passaporto", "persona": "nesrine", "nome_file": None},
            "expected_top1": "passport_nesrine",
        },
        {
            "name": "benchmark_same_person_different_doc_types_should_choose_contract",
            "query": "contratto nesrine",
            "parsed_query": {"tipo_documento": "contratto", "persona": "nesrine", "nome_file": None},
            "expected_top1": "contract_nesrine_delta",
        },
        {
            "name": "benchmark_partial_query_should_keep_right_doc_high",
            "query": "makrm tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrm", "nome_file": None},
            "expected_in_top3": "ticket_makrem_milano_tunisi",
        },
        {
            "name": "benchmark_milan_alias_should_keep_correct_ticket_high",
            "query": "ticket makrem milan tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "expected_in_top3": "ticket_makrem_milano_tunisi",
        },
        {
            "name": "benchmark_invoice_query_should_choose_invoice",
            "query": "fattura ahmed",
            "parsed_query": {"tipo_documento": "fattura", "persona": "ahmed", "nome_file": None},
            "expected_top1": "invoice_ahmed",
        },
    ],
    ids=lambda c: c["name"],
)
def test_search_benchmark(case, benchmark_documents):
    ranked = rank_documents(
        query=case["query"],
        parsed_query=case["parsed_query"],
        documents=benchmark_documents,
    )

    top_ids = [r["doc_id"] for r in ranked[:3]]

    if "expected_top1" in case:
        assert ranked[0]["doc_id"] == case["expected_top1"]

    if "must_not_top1" in case:
        assert ranked[0]["doc_id"] != case["must_not_top1"]

    if "expected_in_top3" in case:
        assert case["expected_in_top3"] in top_ids

    if "must_not_be_in_top3" in case:
        assert case["must_not_be_in_top3"] not in top_ids