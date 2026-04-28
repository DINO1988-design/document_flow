from src.api_server import compute_document_score


def test_passenger_name_should_rank_above_cliente_for_ticket_query():
    query = "ticket nesrine"
    parsed_query = {
        "tipo_documento": "ticket",
        "persona": "nesrine",
        "nome_file": None,
    }

    doc_ticket = {
        "row": {
            "tipo_documento": "biglietto aereo",
            "file_path": "src/uploads/ticket_nesrine.pdf",
        },
        "fields": {
            "passenger_name": "Nesrine Ben Ali",
            "route": "Roma Tunisi",
        },
        "semantic_score": 0.58,
        "best_chunk_text": "Passenger Nesrine Ben Ali flight Roma Tunisi",
        "raw_ocr_text": "Passenger NESRINE BEN ALI",
    }

    doc_invoice = {
        "row": {
            "tipo_documento": "fattura",
            "file_path": "src/uploads/fattura_nesrine.pdf",
        },
        "fields": {
            "cliente": "Nesrine Ben Ali",
            "totale": "890.00",
        },
        "semantic_score": 0.63,
        "best_chunk_text": "Fattura Nesrine Ben Ali totale 890",
        "raw_ocr_text": "Nesrine Ben Ali invoice 890",
    }

    result_ticket = compute_document_score(
        query=query,
        parsed_query=parsed_query,
        row=doc_ticket["row"],
        fields=doc_ticket["fields"],
        semantic_score=doc_ticket["semantic_score"],
        best_chunk_text=doc_ticket["best_chunk_text"],
        raw_ocr_text=doc_ticket["raw_ocr_text"],
    )

    result_invoice = compute_document_score(
        query=query,
        parsed_query=parsed_query,
        row=doc_invoice["row"],
        fields=doc_invoice["fields"],
        semantic_score=doc_invoice["semantic_score"],
        best_chunk_text=doc_invoice["best_chunk_text"],
        raw_ocr_text=doc_invoice["raw_ocr_text"],
    )

    assert result_ticket["score"] > result_invoice["score"]

    # Il ticket deve avere match forte sulla persona
    assert result_ticket["score_breakdown"]["person_strong_bonus"] > 0

    # La fattura con "cliente" non deve più avere match strong
    assert result_invoice["score_breakdown"]["person_strong_bonus"] == 0.0

    # Ma deve comunque avere match medium
    assert result_invoice["score_breakdown"]["person_medium_bonus"] > 0