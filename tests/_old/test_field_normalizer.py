from src.document_field_normalizer import normalize_document_fields


def test_cedolino_base():
    raw_fields = {
        "totale_netto": "934,36",
        "periodo_paga": "FEBBRAIO 2026",
        "nome": "YASMINE",
        "cognome": "AZRI",
        "campo_random": "xxx"
    }

    result = normalize_document_fields("cedolino", raw_fields)

    assert result == {
        "netto": "934,36",
        "periodo_paga": "FEBBRAIO 2026",
        "nome": "YASMINE",
        "cognome": "AZRI"
    }


def test_alias_inglese():
    raw_fields = {
        "net pay": "1200"
    }

    result = normalize_document_fields("cedolino", raw_fields)

    assert result == {
        "netto": "1200"
    }


def test_ocr_sporco():
    raw_fields = {
        "Totale Netto €": "1.200,50"
    }

    result = normalize_document_fields("cedolino", raw_fields)

    assert result == {
        "netto": "1.200,50"
    }


def test_campo_non_valido():
    raw_fields = {
        "random_field": "123"
    }

    result = normalize_document_fields("cedolino", raw_fields)

    assert result == {}


def test_biglietto():
    raw_fields = {
        "departure": "Milano",
        "arrival": "Roma",
        "total_amount": "89€"
    }

    result = normalize_document_fields("biglietto", raw_fields)

    assert result == {
        "partenza": "Milano",
        "arrivo": "Roma",
        "totale": "89€"
    }