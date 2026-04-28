from src.document_reclassifier import classify_document_from_fields_with_gpt

BONIFICO_FIELDS = {
    "banca_beneficiario": "REVOLUT BANK UAB - SUCURSALE IT ALIANA UAB",
    "beneficiario": "Adem Hadda",
    "conto_accredito": "IT 55X03 66901600887179444498",
    "cro_trn": "1101260570638274",
    "data_esecuzione": "26.02.2026",
    "divisa": "EUR",
    "filiale": "MILAN BRANCH OFFICE",
    "importo": "2000,00",
    "ordinante": "SLIMANI NOUREDDINE",
    "tipo_bonifico": "Ordinario"
}

def test_bonifico_fields_should_not_be_payslip():
    decision = classify_document_from_fields_with_gpt(
        BONIFICO_FIELDS,
        tipo_gpt="cedolino"
    )

    print("\nTIPO FINALE:", decision["tipo_documento_finale"])
    print("CONFIDENCE:", decision["confidence"])
    print("REASON:", decision["reason"])

    assert decision["tipo_documento_finale"] == "bonifico"