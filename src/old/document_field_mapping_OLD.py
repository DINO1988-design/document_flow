#document_field_mapping.py
from __future__ import annotations

FIELD_CANONICAL_ALIASES = {
    "cedolino": {
        "nome": ["nome", "first_name"],
        "cognome": ["cognome", "surname", "last_name"],
        "data_assunzione": ["data_assunzione", "hire_date"],
        "periodo_paga": ["periodo_paga", "periodo_retribuzione", "periodo_riferimento"],
        "lordo": ["lordo", "totale_lordo", "retribuzione_lorda", "compenso_lordo"],
        "netto": ["netto", "totale_netto", "retribuzione_netto", "compenso_netto", "net pay"],
        "ore_lavorate": ["ore_lavorate", "ore", "ore lavorate"],
        "datore_lavoro": ["datore_lavoro", "datore_di_lavoro", "azienda", "ditta"]
    },
    "bonifico": {
        "ordinante": ["ordinante"],
        "beneficiario": ["beneficiario", "recipient", "destinatario"],
        "importo": ["importo", "amount"],
        "data_esecuzione": ["data_esecuzione", "execution_date"],
        "iban": ["iban", "conto_accredito", "coordinate_bancarie_iban"],
        "banca_beneficiario": ["banca_beneficiario"]
    },
    "biglietto": {
        "nome_passeggero": ["nome_passeggero", "passenger", "passenger_name", "traveller_name"],
        "partenza": ["partenza", "departure", "from", "departure_city", "aeroporto_partenza"],
        "arrivo": ["arrivo", "arrival", "to", "destination"],
        "data_partenza": ["data_partenza", "date", "data_volo", "departure_date"],
        "orario_partenza": ["orario_partenza", "departure_time"],
        "orario_arrivo": ["orario_arrivo", "arrival_time"],
        "totale": ["totale", "total_amount", "grand_total", "prezzo_totale", "costo_totale"],
        "numero_volo": ["numero_volo", "flight_number", "flight"]
    },
    "passaporto": {
        "nome": ["nome", "given_name", "first_name"],
        "cognome": ["cognome", "surname", "last_name"],
        "numero_passaporto": ["numero_passaporto", "passport_number", "document_number"],
        "data_nascita": ["data_nascita", "birth_date"],
        "data_scadenza": ["data_scadenza", "expiry_date", "date_of_expiry", "scadenza"]
    },
    "fattura": {
        "fornitore": ["fornitore", "supplier", "vendor", "azienda"],
        "numero_fattura": ["numero_fattura", "invoice_number"],
        "data_fattura": ["data_fattura", "invoice_date", "data"],
        "totale": ["totale", "totale_fattura", "importo_totale", "grand_total"]
    }
}