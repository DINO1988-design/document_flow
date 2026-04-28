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


def test_noisy_queries_should_keep_correct_documents_high():
    documents = [
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
            "raw_ocr_text": "Passenger MAKRM SLIMANI Milano Tunisi FR7621",
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
            "raw_ocr_text": "Passenger NSRINE BEN ALI Roma Tunisi TU1455",
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
            "raw_ocr_text": "PAX AHMD ALI PALERMO TUNISI",
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
            "doc_id": "invoice_makrem",
            "row": {
                "tipo_documento": "fattura",
                "file_path": "src/uploads/fattura_makrem.pdf",
            },
            "fields": {
                "cliente": "Makrem Slimani",
                "totale": "450.00",
            },
            "semantic_score": 0.60,
            "best_chunk_text": "Fattura Makrem Slimani totale 450",
            "raw_ocr_text": "Makrem Slimani invoice",
        },
    ]

    cases = [
        {
            "query": "biglietti makrm milno tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrm", "nome_file": None},
            "expected_in_top3": "ticket_makrem_milano_tunisi",
            "must_not_top1": "invoice_makrem",
        },
        {
            "query": "ticket nesrin tunis",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrin", "nome_file": None},
            "expected_in_top3": "ticket_nesrine_roma_tunisi",
        },
        {
            "query": "traghetto ahmd palermo tunisi",
            "parsed_query": {"tipo_documento": "traghetto", "persona": "ahmd", "nome_file": None},
            "expected_in_top3": "ferry_ahmed_palermo_tunisi",
        },
        {
            "query": "passport makrem xk123456",
            "parsed_query": {"tipo_documento": "passaporto", "persona": "makrem", "nome_file": None},
            "expected_in_top3": "passport_makrem",
            "must_not_top1": "ticket_makrem_milano_tunisi",
        },
    ]

    for case in cases:
        ranked = rank_documents(case["query"], case["parsed_query"], documents)
        top_ids = [r["doc_id"] for r in ranked[:3]]

        assert case["expected_in_top3"] in top_ids

        if "must_not_top1" in case:
            assert ranked[0]["doc_id"] != case["must_not_top1"]