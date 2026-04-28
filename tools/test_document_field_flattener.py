from src.document_field_flattener import flatten_document_fields

sample = {
    "data": {
        "date": "10 dicembre 2024",
        "recipient": {
            "name": "Slimani Noureddine",
            "code": "SLMNDD88M23Z352E"
        },
        "contract": {
            "start_date": "1 gennaio 2025",
            "salary": {
                "annual_gross": "45.075,20€"
            }
        }
    }
}

print(flatten_document_fields(sample))