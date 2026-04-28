from src.api_server import compute_document_score


def test_invoice_should_rank_above_ticket_when_query_is_invoice():
    query = "fattura makrem"
    parsed_query = {
        "tipo_documento": "fattura",
        "persona": "makrem",
        "nome_file": None,
    }

    doc_invoice = {
        "row": {
            "tipo_documento": "fattura",
            "file_path": "src/uploads/fattura_makrem.pdf",
        },
        "fields": {
            "cliente": "Makrem Slimani",
            "totale": "450.00",
        },
        "semantic_score": 0.65,
        "best_chunk_text": "Fattura Makrem Slimani totale 450",
        "raw_ocr_text": "Makrem Slimani invoice 450",
    }

    doc_ticket = {
        "row": {
            "tipo_documento": "biglietto aereo",
            "file_path": "src/uploads/ticket_makrem.pdf",
        },
        "fields": {
            "passenger_name": "Makrem Slimani",
            "route": "Milano Tunisi",
        },
        "semantic_score": 0.70,
        "best_chunk_text": "Passenger Makrem Milano Tunisi",
        "raw_ocr_text": "Passenger MAKREM SLIMANI",
    }

    result_invoice = compute_document_score(
        query=query,
        parsed_query=parsed_query,
        row=doc_invoice["row"],
        fields=doc_invoice["fields"],
        semantic_score=doc_invoice["semantic_score"],
        best_chunk_text=doc_invoice["best_chunk_text"],
        raw_ocr_text=doc_invoice["raw_ocr_text"],
    )

    result_ticket = compute_document_score(
        query=query,
        parsed_query=parsed_query,
        row=doc_ticket["row"],
        fields=doc_ticket["fields"],
        semantic_score=doc_ticket["semantic_score"],
        best_chunk_text=doc_ticket["best_chunk_text"],
        raw_ocr_text=doc_ticket["raw_ocr_text"],
    )

    # 🔥 Regola chiave: la fattura deve vincere
    assert result_invoice["score"] > result_ticket["score"]

    # 🔍 Controllo che il tipo abbia influenzato
    assert result_invoice["score_breakdown"]["tipo_score_adjustment"] > 0
    assert result_ticket["score_breakdown"]["tipo_score_adjustment"] < 0