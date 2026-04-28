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


@pytest.fixture
def realistic_documents():
    return [
        {
            "doc_id": "ticket_nesrine_rome_tunis",
            "row": {
                "tipo_documento": "biglietto aereo",
                "file_path": "src/uploads/flight_nesrine_rome_tunis.pdf",
            },
            "fields": {
                "passenger_name": "Nesrine Ben Ali",
                "route": "Roma Tunisi",
                "booking_reference": "AZ2451",
                "compagnia": "ITA Airways",
            },
            "semantic_score": 0.83,
            "best_chunk_text": "Passenger Nesrine Ben Ali flight Roma Tunisi booking AZ2451",
            "raw_ocr_text": "Passenger Name NESRINE BEN ALI Roma Tunisi AZ2451",
        },
        {
            "doc_id": "ticket_makrem_milan_rome",
            "row": {
                "tipo_documento": "biglietto aereo",
                "file_path": "src/uploads/ticket_makrem_milano_roma.pdf",
            },
            "fields": {
                "passenger_name": "Makrem Slimani",
                "route": "Milano Roma",
                "booking_reference": "FR1234",
                "compagnia": "Ryanair",
            },
            "semantic_score": 0.79,
            "best_chunk_text": "Passenger Makrem Slimani flight Milano Roma Ryanair FR1234",
            "raw_ocr_text": "Passenger MAKREM SLIMANI Milano Roma FR1234",
        },
        {
            "doc_id": "ticket_fatma_paris_tunis",
            "row": {
                "tipo_documento": "e-ticket",
                "file_path": "src/uploads/fatma_paris_tunis.pdf",
            },
            "fields": {
                "passenger_name": "Fatma Zahra",
                "route": "Parigi Tunisi",
                "booking_reference": "TU8891",
                "compagnia": "Tunisair",
            },
            "semantic_score": 0.76,
            "best_chunk_text": "Passenger Fatma Zahra Paris Tunis Tunisair TU8891",
            "raw_ocr_text": "Passenger FATMA ZAHRA Paris Tunis TU8891",
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
            "semantic_score": 0.74,
            "best_chunk_text": "Passeggero Zaafouri Amna traghetto Genova Tunisi GNV",
            "raw_ocr_text": "PAX ZAAFOURI/AMNA GENOVA TUNISI",
        },
        {
            "doc_id": "ferry_ahmed_palermo_tunis",
            "row": {
                "tipo_documento": "traghetto",
                "file_path": "src/uploads/ferry_ahmed_palermo_tunis.pdf",
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
            "best_chunk_text": "Contratto Ahmed Ali Altea S.p.A. tempo indeterminato",
            "raw_ocr_text": "Ahmed Ali contratto Altea",
        },
        {
            "doc_id": "contract_nesrine_consulenza",
            "row": {
                "tipo_documento": "contratto",
                "file_path": "src/uploads/contratto_nesrine_consulenza.pdf",
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
            "doc_id": "invoice_ahmed_consulting",
            "row": {
                "tipo_documento": "fattura",
                "file_path": "src/uploads/fattura_ahmed_consulting.pdf",
            },
            "fields": {
                "cliente": "Ahmed Ali",
                "totale": "1250.00",
                "fornitore": "Studio Delta",
            },
            "semantic_score": 0.64,
            "best_chunk_text": "Fattura Ahmed Ali totale 1250 Studio Delta",
            "raw_ocr_text": "Ahmed Ali invoice 1250",
        },
        {
            "doc_id": "invoice_nesrine_travel",
            "row": {
                "tipo_documento": "fattura",
                "file_path": "src/uploads/fattura_nesrine_travel.pdf",
            },
            "fields": {
                "cliente": "Nesrine Ben Ali",
                "totale": "890.00",
                "fornitore": "Travel Group",
            },
            "semantic_score": 0.62,
            "best_chunk_text": "Fattura Nesrine Ben Ali totale 890 Travel Group",
            "raw_ocr_text": "Nesrine Ben Ali invoice 890",
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
            "semantic_score": 0.73,
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
            "semantic_score": 0.72,
            "best_chunk_text": "Passport Nesrine Ben Ali YA987654 Tunisina",
            "raw_ocr_text": "PASSPORT NESRINE BEN ALI YA987654",
        },
        {
            "doc_id": "generic_admin_doc",
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
            "name": "air_ticket_nesrine_should_be_top1",
            "query": "biglietto nesrine roma tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "expected_top1": "ticket_nesrine_rome_tunis",
        },
        {
            "name": "makrem_ryanair_should_be_top1",
            "query": "ticket makrem ryanair milano roma",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "expected_top1": "ticket_makrem_milan_rome",
        },
        {
            "name": "amna_ferry_should_be_top1",
            "query": "traghetto amna genova tunisi",
            "parsed_query": {"tipo_documento": "biglietto", "persona": "amna", "nome_file": None},
            "expected_top1": "ferry_amna_genova_tunisi",
        },
        {
            "name": "ahmed_contract_should_beat_ticket_and_invoice_for_contract_query",
            "query": "contratto ahmed altea",
            "parsed_query": {"tipo_documento": "contratto", "persona": "ahmed", "nome_file": None},
            "expected_top1": "contract_ahmed_altea",
        },
        {
            "name": "makrem_passport_should_be_top1",
            "query": "passaporto makrem",
            "parsed_query": {"tipo_documento": "passaporto", "persona": "makrem", "nome_file": None},
            "expected_top1": "passport_makrem",
        },
        {
            "name": "nesrine_passport_number_should_find_passport_in_top3",
            "query": "passport nesrine YA987654",
            "parsed_query": {"tipo_documento": "passport", "persona": "nesrine", "nome_file": None},
            "expected_in_top3": "passport_nesrine",
        },
        {
            "name": "ahmed_ticket_vs_contract_should_keep_ferry_high_for_palermo_tunisi",
            "query": "biglietto ahmed palermo tunisi",
            "parsed_query": {"tipo_documento": "biglietto", "persona": "ahmed", "nome_file": None},
            "expected_in_top3": "ferry_ahmed_palermo_tunis",
        },
        {
            "name": "nesrine_invoice_should_not_win_for_ticket_query",
            "query": "ticket nesrine",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "expected_top1": "ticket_nesrine_rome_tunis",
            "must_not_top1": "invoice_nesrine_travel",
        },
        {
            "name": "generic_doc_should_not_appear_in_top3_for_person_query",
            "query": "ticket makrem",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "must_not_be_in_top3": "generic_admin_doc",
        },
        {
            "name": "fatma_ticket_should_be_found_with_route",
            "query": "e ticket fatma parigi tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "fatma", "nome_file": None},
            "expected_top1": "ticket_fatma_paris_tunis",
        },
    ],
    ids=lambda c: c["name"],
)
def test_search_realistic_dataset(case, realistic_documents):
    ranked = rank_documents(
        query=case["query"],
        parsed_query=case["parsed_query"],
        documents=realistic_documents,
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