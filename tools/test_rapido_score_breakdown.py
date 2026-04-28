from src.api_server import compute_document_score

query = "ticket nesrine"
parsed_query = {
    "tipo_documento": "ticket",
    "persona": "nesrine",
    "nome_file": None,
}

doc_correct = {
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

doc_wrong = {
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

r1 = compute_document_score(
    query=query,
    parsed_query=parsed_query,
    row=doc_correct["row"],
    fields=doc_correct["fields"],
    semantic_score=doc_correct["semantic_score"],
    best_chunk_text=doc_correct["best_chunk_text"],
    raw_ocr_text=doc_correct["raw_ocr_text"],
)

r2 = compute_document_score(
    query=query,
    parsed_query=parsed_query,
    row=doc_wrong["row"],
    fields=doc_wrong["fields"],
    semantic_score=doc_wrong["semantic_score"],
    best_chunk_text=doc_wrong["best_chunk_text"],
    raw_ocr_text=doc_wrong["raw_ocr_text"],
)

print("DOC CORRETTO")
print("score:", r1["score"])
print("breakdown:", r1["score_breakdown"])
print()

print("DOC SBAGLIATO")
print("score:", r2["score"])
print("breakdown:", r2["score_breakdown"])