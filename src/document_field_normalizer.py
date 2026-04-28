# src/document_field_normalizer.py
from __future__ import annotations

import json
import re
import unicodedata
from typing import Any


# =========================================================
# UTIL TESTO
# =========================================================
def normalize_text(value: str | None) -> str:
    if value is None:
        return ""

    value = str(value).strip().lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(c for c in value if not unicodedata.combining(c))
    value = re.sub(r"[_\-/\\]", " ", value)
    value = re.sub(r"[^\w\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


# =========================================================
# REGISTRO TIPI DOCUMENTO
# =========================================================
DOCUMENT_TYPE_ALIASES = {
    "cedolino": [
        "cedolino",
        "busta paga",
        "cedolino paga",
        "cedolino stipendio",
        "pay slip",
        "payslip",
    ],
    "bonifico": [
        "bonifico",
        "ricevuta di bonifico",
        "contabile bancaria",
        "documento bancario",
        "ricevuta bancaria",
        "movimenti bancari",
        "movimento bancario",
        "trasferimenti bancari",
        "soldi inviati",
        "pagamenti",
    ],
    
    "biglietto": [
        "biglietto",
        "e-ticket",
        "eticket",
        "ticket",
        "biglietto aereo",
        "boarding pass",
        "prenotazione",
        "biglietto traghetto",
        "viaggio",
        "documento di viaggio",
        "travel document",
        "voli",
        "volo",
    ],
    "passaporto": [
        "passaporto",
        "passport",
        "passeport",
        "documento di identita",
        "documento di identità",
    ],
    "fattura": [
        "fattura",
        "invoice",
    ],
    "contratto": [
        "contratto",
        "contract",
        "lettera assunzione",
        "employment contract",
    ],
    "ordine_acquisto": [
        "ordine_acquisto",
        "ordine acquisto",
        "ordine di acquisto",
        "oda",
        "o d a",
        "purchase order",
    ],

"curriculum": [
    "curriculum",
    "cv",
    "curriculum vitae",
    "resume",
    "résumé",
    "profilo professionale",
],
}


# =========================================================
# REGISTRO CAMPI CANONICI
# =========================================================
DOCUMENT_FIELD_ALIASES = {
    "cedolino": {
        "canonical_fields": {
            "nome": [
                "nome", "first_name", "given_name", "employee_name", "lavoratore_nome"
            ],
            "cognome": [
                "cognome", "surname", "last_name", "family_name", "lavoratore_cognome"
            ],
            "data_assunzione": [
                "data_assunzione", "hiring_date", "hire_date"
            ],
            "periodo_paga": [
                "periodo_paga", "periodo_retribuzione", "periodo_riferimento",
                "periodo_referenza", "pay_period"
            ],
            "lordo": [
                "lordo", "totale_lordo", "retribuzione_lorda", "importo_lordo",
                "gross", "gross_pay", "compenso.lordo",
                "retribuzioni_corrisposte.retribuzione_lorda",
                "totale_retribuzione"
            ],
            "netto": [
                "netto", "totale_netto", "retribuzione_netto", "importo_netto",
                "netto_a_pagare", "net pay", "compenso.netto",
                "retribuzioni_corrisposte.retribuzione_netto",
                "netto_busta", "netto_bust", "tota_retribuzione", 
            ],
            "datore_lavoro": [
                "datore_lavoro", "datore_di_lavoro", "azienda", "ditta", "employer"
            ],
            "ore_lavorate_totale": [
                "ore_lavorate_totale", "totale_ore_lavorate", "totale_ore",
                "totale_ore_lavoro", "working_hours_total", "hours_total",
                "ore_lavorate"
            ],
            "ore_lavorate_lavoro_ordinario": [
                "ore_lavorate.lavoro_ordinario", "lavoro_ordinario"
            ],
            "ore_lavorate_assenze": [
                "ore_lavorate.assenze", "assenze"
            ],
            "ore_lavorate_ferie_godute": [
                "ore_lavorate.ferie_godute", "ferie_godute"
            ],
            "ore_lavorate_rol_goduti": [
                "ore_lavorate.rol_goduti", "rol_goduti"
            ],
            "ore_lavorate_patto_stabilita": [
                "ore_lavorate.patto_stabilita", "patto_stabilita"
            ],
            "ore_lavorate_buoni_pasto": [
                "ore_lavorate.buoni_pasto", "buoni_pasto"
            ],
            "trattenute_irpef": [
                "trattenute.irpef", "trattenute.rpef", "irpef", "rpef"
            ],
            "trattenute_conguaglio": [
                "trattenute.conguaglio", "conguaglio"
            ],
            "trattenute_addizionale_regionale": [
                "trattenute.addizionale_regionale",
                "addizionale_regionale",
                "trattenute_addizionale_regionale"
            ],
            "trattenute_addizionale_comunale": [
                "trattenute.addizionale_comunale",
                "addizionale_comunale",
                "trattenute_addizionale_comunale"
            ],
            "iban": [
                "iban", "conto_accredito", "coordinate_bancarie_iban"
            ],
            "banca": [
                "banca", "istituto_bancario", "bank"
            ],
            "codice_fiscale": [
                "codice_fiscale", "codice_fiscale_ditta", "fiscal_code", "tax_code"
            ],
            "data_nascita": [
                "data_nascita", "data_nascita_lavoratore", "birth_date"
            ],
            "luogo_residenza": [
                "luogo_residenza", "residenza", "citta_residenza", "città_residenza"
            ],
            "posizione": [
                "posizione", "matricola", "employee_id", "position_id"
            ],
            "retribuzione_base": [
                "retribuzione_base", "base_salary", "paga_base"
            ],
            "data_emissione": [
                "data_emissione", "issue_date", "data_documento"
            ],
            "numero_foglio": [
                "numero_foglio", "foglio", "sheet_number"
            ],
            "trattenute_tfr": [
                "tfr", "quota_tfr", "quota_tfr_mese", "trattenute_tfr"
            ],
            "trattenute_esonero_contr_iv": [
                "esonero_contr_iv", "trattenute_esonero_contr_iv"
            ],
            "competenze_lavoro_ordinario": [
                "competenze_lavoro_ordinario", "lavoro_ordinario"
            ],
            "competenze_assenze": [
                "competenze_assenze", "assenze"
            ],
            "competenze_festivita_godute": [
                "competenze_festivita_godute", "festivita_godute"
            ],
            "competenze_ferie_godute": [
                "competenze_lavoro_ferie_godute", "ferie_godute"
            ],
            "indirizzo_ditta": [
                "indirizzo_ditta", "indirizzo_azienda", "indirizzo_company"
            ],
            "autorizzazione_inail": [
                "autorizzazione_inail", "inail"
            ],
        },
        "field_types": {
            "nome": "text",
            "cognome": "text",
            "data_assunzione": "date",
            "periodo_paga": "text",
            "lordo": "number",
            "netto": "number",
            "datore_lavoro": "text",
            "ore_lavorate_totale": "number",
            "ore_lavorate_lavoro_ordinario": "number",
            "ore_lavorate_assenze": "number",
            "ore_lavorate_ferie_godute": "number",
            "ore_lavorate_rol_goduti": "number",
            "ore_lavorate_patto_stabilita": "number",
            "ore_lavorate_buoni_pasto": "number",
            "trattenute_irpef": "number",
            "trattenute_conguaglio": "number",
            "trattenute_addizionale_regionale": "number",
            "trattenute_addizionale_comunale": "number",
            "iban": "text",
            "banca": "text",
            "codice_fiscale": "text",
            "data_nascita": "date",
            "luogo_residenza": "text",
            "posizione": "text",
            "retribuzione_base": "number",
            "data_emissione": "date",
            "numero_foglio": "text",
            "trattenute_tfr": "number",
            "trattenute_esonero_contr_iv": "number",
            "competenze_lavoro_ordinario": "number",
            "competenze_assenze": "number",
            "competenze_festivita_godute": "number",
            "competenze_ferie_godute": "number",
            "indirizzo_ditta": "text",
            "autorizzazione_inail": "text",
        },
        "query_rules": {
            "default_numeric_field": "netto",
            "default_numeric_operator": "="
        },
        "required_fields": [],
    },

    "bonifico": {
        "canonical_fields": {
            "ordinante": [
                "ordinante", "payer", "sender"
            ],
            "beneficiario": [
                "beneficiario", "recipient", "destinatario", "payee"
            ],
            "importo": [
                "importo", "amount", "importo_totale", "totale",
                "totale_bonifico", "importo_bonifico", "somma", "valore",
            ],
            "data_esecuzione": [
                   "data_esecuzione", "data", "data_bonifico", "data_operazione", "data_transazione", "execution_date", "value_date",  "data_disposizione", "data_valuta"
            ],
            "iban": [
                "iban", "conto_accredito", "coordinate_bancarie_iban"
            ],
            "banca_beneficiario": [
                "banca_beneficiario", "beneficiary_bank"
            ],
        },
        "field_types": {
            "ordinante": "text",
            "beneficiario": "text",
            "importo": "number",
            "data_esecuzione": "date",
            "iban": "text",
            "banca_beneficiario": "text",
        },
        "query_rules": {
            "default_numeric_field": "importo",
            "default_numeric_operator": "="
        },
        "required_fields": [],
    },

    "biglietto": {
        "canonical_fields": {
            "nome_passeggero": [
                "nome_passeggero", "passenger", "passenger_name", "traveller_name",
                "guest_name", "holder_name"
            ],
            "partenza": [
                "partenza", "departure", "from", "origin", "departure_city",
                "departure_airport", "aeroporto_partenza", "porto_partenza",
                "abfahrtshafen"
            ],
            "arrivo": [
                "arrivo", "arrival", "to", "destination", "arrival_city",
                "arrival_airport", "aeroporto_arrivo", "porto_arrivo",
                "ankunftshafen"
            ],
            "data_partenza": [
                "data_partenza", "departure_date", "data_volo", "date"
            ],
            "orario_partenza": [
                "orario_partenza", "departure_time", "ora_partenza", "abfahrtszeit"
            ],
            "orario_arrivo": [
                "orario_arrivo", "arrival_time", "ora_arrivo", "ankunftszeit"
            ],
            "totale": [
                "totale", "total_amount", "grand_total", "costo_totale",
                "prezzo_totale", "total_payment_amount", "importo_totale", "total_cost"
            ],
            "numero_volo": [
                "numero_volo", "flight_number", "flight"
            ],
        },
        "field_types": {
            "nome_passeggero": "text",
            "partenza": "text",
            "arrivo": "text",
            "data_partenza": "date",
            "orario_partenza": "text",
            "orario_arrivo": "text",
            "totale": "number",
            "numero_volo": "text",
        },
        "query_rules": {
            "default_numeric_field": "totale",
            "default_numeric_operator": "="
        },
        "required_fields": [],
    },

    "passaporto": {
        "canonical_fields": {
            "nome": [
                "nome", "given_name", "first_name"
            ],
            "cognome": [
                "cognome", "surname", "last_name", "family_name"
            ],
            "numero_passaporto": [
                "numero_passaporto", "passport_number", "document_number"
            ],
            "data_nascita": [
                "data_nascita", "birth_date"
            ],
            "data_scadenza": [
                "data_scadenza", "scadenza", "expiry_date", "date_of_expiry", "expiry"
            ],
        },
        "field_types": {
            "nome": "text",
            "cognome": "text",
            "numero_passaporto": "text",
            "data_nascita": "date",
            "data_scadenza": "date",
        },
        "query_rules": {
            "default_numeric_field": None,
            "default_numeric_operator": "="
        },
        "required_fields": [],
    },

    "fattura": {
        "canonical_fields": {
            "fornitore": [
                "fornitore", "supplier", "vendor", "azienda", "ditta"
            ],
            "numero_fattura": [
                "numero_fattura", "invoice_number", "numero_documento"
            ],
            "data_fattura": [
                "data_fattura", "invoice_date", "data_emissione", "issue_date"
            ],
            "totale": [
                "totale", "totale_fattura", "importo_totale", "total_amount", "grand_total"
            ],
        },
        "field_types": {
            "fornitore": "text",
            "numero_fattura": "text",
            "data_fattura": "date",
            "totale": "number",
        },
        "query_rules": {
            "default_numeric_field": "totale",
            "default_numeric_operator": "="
        },
        "required_fields": [],
    },

    "contratto": {
        "canonical_fields": {
            "nome": [
                "nome",
                "nome_dipendente",
                "nome_lavoratore",
                "recipient.name",
                "lavoratore.nome",
                "dipendente.nome"
            ],
            "codice_fiscale": [
                "codice_fiscale",
                "recipient.code",
                "lavoratore.codice_fiscale",
                "dipendente.codice_fiscale"
            ],
            "data_inizio": [
                "data_inizio",
                "decorrenza",
                "decorrenza.inizio",
                "contract.start_date",
                "condizioni.decorrenza"
            ],
            "ral": [
                "ral",
                "retribuzione_lorda_annuale",
                "contract.salary.annual_gross",
                "trattamento_retributivo.retribuzione_lorda_annuale",
                "retribuzione.annuale_lorda"
            ],
            "datore_lavoro": [
                "datore_lavoro",
                "datore_lavoro.nome",
                "nome_datore_lavoro",
                "azienda",
                "societa",
                "società",
                "employer"
            ],
            "sede_lavoro": [
                "sede_lavoro",
                "sede_lavoro.indirizzo",
                "contract.workplace.location",
                "condizioni.sede_lavoro"
            ],
        },
        "field_types": {
            "nome": "text",
            "codice_fiscale": "text",
            "data_inizio": "date",
            "ral": "number",
            "datore_lavoro": "text",
            "sede_lavoro": "text",
        },
        "query_rules": {
            "default_numeric_field": "ral",
            "default_numeric_operator": "="
        },
        "required_fields": [],
    },
    
    "ordine_acquisto": {
        "canonical_fields": {
            "numero_ordine_acquisto": [
                "numero_ordine_acquisto",
                "ordine_acquisto",
                "numero_oda",
                "numero ordine",
                "numero ordine acquisto",
                "oda",
                "purchase_order_number"
            ],
            "accordo_quadro_data": [
                "accordo_quadro_data",
                "data_accordo_quadro",
                "accordo quadro data"
            ],
            "fornitore": [
                "fornitore",
                "nome_fornitore",
                "supplier",
                "vendor"
            ],
            "partita_iva_fornitore": [
                "partita_iva_fornitore",
                "piva_fornitore",
                "pivafornitore",
                "vat_supplier"
            ],
            "data_documento": [
                "data_documento",
                "data_oda",
                "data ordine",
                "data ordine acquisto"
            ],
            "importo": [
                "importo",
                "totale",
                "importo_totale",
                "valore_ordine"
            ],
            "oggetto": [
                "oggetto",
                "descrizione",
                "prestazione_servizi",
                "richiesta_fornitura"
            ],
        },
        "field_types": {
            "numero_ordine_acquisto": "text",
            "accordo_quadro_data": "date",
            "fornitore": "text",
            "partita_iva_fornitore": "text",
            "data_documento": "date",
            "importo": "number",
            "oggetto": "text",
        },
        "query_rules": {
            "default_numeric_field": "importo",
            "default_numeric_operator": "="
        },
        "required_fields": [],
    }, 

"curriculum": {
    "canonical_fields": {
        "nome": [
            "nome",
            "first_name",
            "given_name",
            "candidate_name",
            "candidato_nome"
        ],
        "cognome": [
            "cognome",
            "surname",
            "last_name",
            "family_name",
            "candidato_cognome"
        ],
        "email": [
            "email",
            "mail",
            "e-mail",
            "indirizzo_email",
            "email_address"
        ],
        "telefono": [
            "telefono",
            "cellulare",
            "numero_telefono",
            "phone",
            "mobile",
            "phone_number"
        ],
        "indirizzo": [
            "indirizzo",
            "residenza",
            "domicilio",
            "address",
            "location"
        ],
        "data_nascita": [
            "data_nascita",
            "birth_date",
            "date_of_birth"
        ],
        "nazionalita": [
            "nazionalita",
            "nazionalità",
            "nationality"
        ],
        "titolo_studio": [
            "titolo_studio",
            "istruzione",
            "education",
            "degree",
            "qualification"
        ],
        "esperienza": [
            "esperienza",
            "esperienze_lavorative",
            "work_experience",
            "professional_experience",
            "employment_history"
        ],
        "competenze": [
            "competenze",
            "skills",
            "technical_skills",
            "hard_skills"
        ],
        "lingue": [
            "lingue",
            "languages",
            "language_skills"
        ],
        "certificazioni": [
            "certificazioni",
            "certificates",
            "certifications"
        ],
        "anni_esperienza": [
            "anni_esperienza",
            "years_experience",
            "years_of_experience",
            "esperienza_anni"
        ],
        "ruolo": [
            "ruolo",
            "posizione",
            "job_title",
            "profile",
            "profession"
        ],
    },
    "field_types": {
        "nome": "text",
        "cognome": "text",
        "email": "text",
        "telefono": "text",
        "indirizzo": "text",
        "data_nascita": "date",
        "nazionalita": "text",
        "titolo_studio": "text",
        "esperienza": "text",
        "competenze": "text",
        "lingue": "text",
        "certificazioni": "text",
        "anni_esperienza": "number",
        "ruolo": "text",
    },
    "query_rules": {
        "default_numeric_field": "anni_esperienza",
        "default_numeric_operator": "="
    },
    "required_fields": [],
},
    
}

NUMERIC_OPERATOR_ALIASES = {
    ">": [
        "sopra", "oltre",
        "maggiore di", "maggiori di",
        "superiore a", "superiori a",
        "piu di", "più di",
        "oltre a"
    ],
    "<": [
        "sotto",
        "inferiore a", "inferiori a",
        "minore di", "minori di",
        "meno di",
        "inferio a",  # errore comune OCR / typing
    ],
    ">=": [
        "almeno",
        "maggiore o uguale a", "maggiori o uguali a",
        "superiore o uguale a", "superiori o uguali a",
        ">= ",
        "non inferiore a", "non inferiori a"
    ],
    "<=": [
        "al massimo",
        "minore o uguale a", "minori o uguali a",
        "inferiore o uguale a", "inferiori o uguali a",
        "<= ",
        "non superiore a", "non superiori a"
    ],
    "between": [
        "tra",
        "da"
    ],

}

# =========================================================
# TIPO DOCUMENTO
# =========================================================
def get_allowed_document_types() -> list[str]:
    return list(dict.fromkeys([*DOCUMENT_FIELD_ALIASES.keys(), "generico"]))

def get_allowed_document_types_for_prompt() -> str:
    return "\n".join(f"- {doc_type}" for doc_type in get_allowed_document_types())
    
def normalize_document_type(tipo_documento: str | None) -> str:
    tipo_norm = normalize_text(tipo_documento)

    if not tipo_norm:
        return "generico"

    for canonical_type, aliases in DOCUMENT_TYPE_ALIASES.items():
        normalized_aliases = [normalize_text(a) for a in aliases]

        # match esatto sul canonico
        if tipo_norm == canonical_type:
            return canonical_type

        # match esatto su alias
        if tipo_norm in normalized_aliases:
            return canonical_type

        # match contenuto: es. "ticket ryanair" -> "biglietto"
        if canonical_type in tipo_norm:
            return canonical_type

        for alias in normalized_aliases:
            if alias and alias in tipo_norm:
                return canonical_type

    return tipo_norm if tipo_norm in get_allowed_document_types() else "generico"


def document_type_matches(query_tipo: str | None, row_tipo: str | None) -> bool:
    q = normalize_document_type(query_tipo)
    r = normalize_document_type(row_tipo)

    if not q or not r:
        return False

    if q == "generico" or r == "generico":
        return False

    return q == r


# =========================================================
# SCHEMA DOCUMENTO
# =========================================================
def get_document_schema(document_type: str | None) -> dict[str, Any]:
    t = normalize_document_type(document_type)
    schema = DOCUMENT_FIELD_ALIASES.get(t, {})

    canonical_fields_map = schema.get("canonical_fields", {})
    required_fields = schema.get("required_fields", [])
    field_types = schema.get("field_types", {})
    query_rules = schema.get("query_rules", {})

    return {
        "document_type": t,
        "canonical_fields": list(canonical_fields_map.keys()),
        "required_fields": required_fields,
        "field_types": field_types,
        "query_rules": query_rules,
    }


def get_canonical_fields(document_type: str | None) -> list[str]:
    return get_document_schema(document_type).get("canonical_fields", [])


def get_required_fields(document_type: str | None) -> list[str]:
    return get_document_schema(document_type).get("required_fields", [])

def get_document_query_rules(document_type: str | None) -> dict[str, Any]:
    t = normalize_document_type(document_type)
    schema = DOCUMENT_FIELD_ALIASES.get(t, {})
    return schema.get("query_rules", {}) or {}


def get_document_field_types(document_type: str | None) -> dict[str, str]:
    t = normalize_document_type(document_type)
    schema = DOCUMENT_FIELD_ALIASES.get(t, {})
    return schema.get("field_types", {}) or {}


def get_default_numeric_field_for_document_type(tipo_documento: str | None) -> str | None:
    rules = get_document_query_rules(tipo_documento)
    return rules.get("default_numeric_field")


def get_default_numeric_operator_for_document_type(tipo_documento: str | None) -> str | None:
    rules = get_document_query_rules(tipo_documento)
    return rules.get("default_numeric_operator")


def get_field_type_for_document(document_type: str | None, field_name: str | None) -> str | None:
    if not field_name:
        return None

    normalized_field = normalize_query_field_name(document_type, field_name)
    field_types = get_document_field_types(document_type)
    return field_types.get(normalized_field)
# =========================================================
# PARSING / FLATTEN
# =========================================================
def try_parse_json_like(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    s = value.strip()
    if not s:
        return value

    if not ((s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]"))):
        return value

    try:
        return json.loads(s)
    except Exception:
        return value


def flatten_value(prefix: str, value: Any) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    parsed = try_parse_json_like(value)

    if isinstance(parsed, dict):
        for k, v in parsed.items():
            child_key = f"{prefix}.{k}" if prefix else str(k)
            flat.update(flatten_value(child_key, v))
        return flat

    if isinstance(parsed, list):
        scalar_items = []
        has_dict = False

        for item in parsed:
            if isinstance(item, dict):
                has_dict = True
                for k, v in item.items():
                    child_key = f"{prefix}.{k}" if prefix else str(k)
                    flat.update(flatten_value(child_key, v))
            else:
                scalar_items.append(str(item))

        if scalar_items:
            flat[prefix] = "; ".join(scalar_items)

        return flat

    flat[prefix] = parsed
    return flat


def expand_input_fields(fields: dict[str, Any]) -> dict[str, Any]:
    expanded: dict[str, Any] = {}

    for raw_key, raw_value in fields.items():
        key_str = str(raw_key).strip()
        parsed_value = try_parse_json_like(raw_value)

        if not isinstance(parsed_value, (dict, list)):
            expanded[key_str] = raw_value

        flattened = flatten_value(key_str, raw_value)
        for fk, fv in flattened.items():
            expanded[fk] = fv

    return expanded


def build_normalized_lookup(expanded_fields: dict[str, Any]) -> dict[str, Any]:
    lookup: dict[str, Any] = {}

    for key, value in expanded_fields.items():
        key_norm = normalize_text(key)
        if key_norm and key_norm not in lookup:
            lookup[key_norm] = value

    return lookup


def find_value_by_aliases(expanded_lookup: dict[str, Any], aliases: list[str]) -> Any | None:
    alias_norms = [normalize_text(a) for a in aliases if normalize_text(a)]

    # 1. match esatto
    for alias_norm in alias_norms:
        if alias_norm in expanded_lookup:
            value = expanded_lookup[alias_norm]
            if value not in [None, "", [], {}]:
                return value

    # 2. match suffix / nested key
    # esempio: total_cost -> travel_details.total_cost
    for alias_norm in alias_norms:
        for key_norm, value in expanded_lookup.items():
            if value in [None, "", [], {}]:
                continue

            if key_norm.endswith(alias_norm):
                return value

    # 3. fallback per contenimento
    for alias_norm in alias_norms:
        for key_norm, value in expanded_lookup.items():
            if value in [None, "", [], {}]:
                continue

            if alias_norm in key_norm or key_norm in alias_norm:
                return value

    return None


# =========================================================
# NORMALIZZAZIONE CAMPI
# =========================================================
def normalize_document_fields(tipo_documento: str | None, fields: dict[str, Any]) -> dict[str, Any]:
    tipo_norm = normalize_document_type(tipo_documento)
    schema = DOCUMENT_FIELD_ALIASES.get(tipo_norm)

    if not schema:
        return fields

    canonical_fields = schema.get("canonical_fields", {})

    expanded_fields = expand_input_fields(fields)
    normalized_lookup = build_normalized_lookup(expanded_fields)

    normalized: dict[str, Any] = {}
    used_aliases = set()

    # 1. mappa i campi canonici
    for canonical_name, aliases in canonical_fields.items():
        search_aliases = [canonical_name] + aliases
        found_value = find_value_by_aliases(normalized_lookup, search_aliases)

        if found_value not in [None, "", [], {}]:
            normalized[canonical_name] = found_value

            for alias in search_aliases:
                alias_norm = normalize_text(alias)
                if alias_norm:
                    used_aliases.add(alias_norm)

    # 2. conserva anche i campi extra non già mappati
    for original_key, original_value in expanded_fields.items():
        if original_value in [None, "", [], {}]:
            continue

        key_norm = normalize_text(original_key)

        if key_norm in used_aliases:
            continue

        if original_key in normalized:
            continue

        normalized[original_key] = original_value

    return normalized

def extract_canonical_fields_only(tipo_documento: str | None, fields: dict[str, Any]) -> dict[str, Any]:
    tipo_norm = normalize_document_type(tipo_documento)
    schema = DOCUMENT_FIELD_ALIASES.get(tipo_norm)

    if not schema:
        return {}

    canonical_fields = schema.get("canonical_fields", {})

    expanded_fields = expand_input_fields(fields)
    normalized_lookup = build_normalized_lookup(expanded_fields)

    canonical_only: dict[str, Any] = {}

    for canonical_name, aliases in canonical_fields.items():
        search_aliases = [canonical_name] + aliases
        found_value = find_value_by_aliases(normalized_lookup, search_aliases)

        if found_value not in [None, "", [], {}]:
            canonical_only[canonical_name] = found_value

    return canonical_only
    
def normalize_query_field_name(tipo_documento: str | None, campo_target: str | None) -> str | None:
    if not campo_target:
        return None

    tipo_norm = normalize_document_type(tipo_documento)
    schema = DOCUMENT_FIELD_ALIASES.get(tipo_norm, {})
    canonical_fields = schema.get("canonical_fields", {})

    campo_norm = normalize_text(campo_target)

    for canonical_name, aliases in canonical_fields.items():
        all_aliases = [canonical_name, *aliases]
        alias_norms = [normalize_text(a) for a in all_aliases if normalize_text(a)]

        if campo_norm in alias_norms:
            return canonical_name

    return campo_target  
    
def infer_numeric_operator_from_query(query: str | None) -> str | None:
    # 🔥 AGGIUNTA 1: controllo simboli PRIMA della normalizzazione
    if query:
        if ">=" in query:
            return ">="
        if "<=" in query:
            return "<="
        if ">" in query:
            return ">"
        if "<" in query:
            return "<"
        if "=" in query:
            return "="

    # ⬇️ TUO CODICE ORIGINALE (INTOCCATO)
    q = normalize_text(query)

    if not q:
        return None

    # pattern espliciti, con confini parola
    operator_patterns = {
        ">=": [
            r"\balmeno\b",
            r"\bmaggiore o uguale a\b",
            r"\bmaggiori o uguali a\b",
            r"\bsuperiore o uguale a\b",
            r"\bsuperiori o uguali a\b",
            r"\bnon inferiore a\b",
            r"\bnon inferiori a\b",
            r">="
        ],
        "<=": [
            r"\bal massimo\b",
            r"\bminore o uguale a\b",
            r"\bminori o uguali a\b",
            r"\binferiore o uguale a\b",
            r"\binferiori o uguali a\b",
            r"\bnon superiore a\b",
            r"\bnon superiori a\b",
            r"<="
        ],
        ">": [
            r"\bsopra\b",
            r"\boltre\b",
            r"\bmaggiore di\b",
            r"\bmaggiori di\b",
            r"\bsuperiore a\b",
            r"\bsuperiori a\b",
            r"\bpiu di\b",
            r"\bpiù di\b",
            r"\boltre a\b"
        ],
        "<": [
            r"\bsotto\b",
            r"\binferiore a\b",
            r"\binferiori a\b",
            r"\bminore di\b",
            r"\bminori di\b",
            r"\bmeno di\b",
            r"\binferio a\b"
        ],
        "between": [
            r"\btra\b.+\be\b",
            r"\bda\b.+\ba\b"
        ],
    }

    for operator, patterns in operator_patterns.items():
        for pattern in patterns:
            if re.search(pattern, q):
                return operator

    return None
    
def extract_numeric_values_from_query(query: str | None) -> list[str]:
    if not query:
        return []

    matches = re.findall(r"\d+(?:[.,]\d+)?", str(query))
    return [m.replace(",", ".") for m in matches]

def extract_explicit_dates_from_query(query: str | None) -> list[str]:
    if not query:
        return []

    q = str(query)

    # dd/mm/yyyy o dd-mm-yyyy
    matches = re.findall(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{4})\b", q)
    normalized = []

    for m in matches:
        parts = re.split(r"[/-]", m)
        if len(parts) == 3:
            dd, mm, yyyy = parts
            normalized.append(f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}")

    return normalized
    
def normalize_structured_query_filters(query: str, parsed_query: dict) -> dict:
    if not isinstance(parsed_query, dict):
        return parsed_query

    query_norm = normalize_text(query)
    tipo_documento = parsed_query.get("tipo_documento")
    campo_target = parsed_query.get("campo_target")

    values = extract_numeric_values_from_query(query)
    explicit_dates = extract_explicit_dates_from_query(query)

    numeric_values = []
    date_values = []

    for v in values:
        if is_likely_year(v, query):
            date_values.append(v)
        else:
            numeric_values.append(v)
            
    inferred_operator = infer_numeric_operator_from_query(query)
    inferred_date_operator = infer_date_operator_from_query(query)
    

    # 🔥 COSTRUZIONE DATA AUTOMATICA
    if explicit_dates:
        if inferred_date_operator in [">", ">="]:
            parsed_query["data_da"] = explicit_dates[0]
        elif inferred_date_operator in ["<", "<="]:
            parsed_query["data_a"] = explicit_dates[0]
        elif inferred_date_operator == "between" and len(explicit_dates) >= 2:
            parsed_query["data_da"] = explicit_dates[0]
            parsed_query["data_a"] = explicit_dates[1]

    elif date_values:
        year = int(date_values[0])

        if inferred_date_operator in [">", ">="]:
            parsed_query["data_da"] = f"{year+1}-01-01"
        elif inferred_date_operator in ["<", "<="]:
            parsed_query["data_a"] = f"{year-1}-12-31"
        elif inferred_date_operator == "between" and len(date_values) >= 2:
            parsed_query["data_da"] = f"{date_values[0]}-01-01"
            parsed_query["data_a"] = f"{date_values[1]}-12-31"  

    has_range_pattern = (
        len(values) >= 2
        and (
            re.search(r"\bda\s+\d+(?:[.,]\d+)?\s+a\s+\d+(?:[.,]\d+)?\b", query_norm)
            or re.search(r"\btra\s+\d+(?:[.,]\d+)?\s+e\s+\d+(?:[.,]\d+)?\b", query_norm)
        )
    )

    print("[NUMERIC DEBUG] query =", query)
    print("[NUMERIC DEBUG] query_norm =", query_norm)
    print("[NUMERIC DEBUG] inferred_operator =", inferred_operator)
    print("[NUMERIC DEBUG] inferred_date_operator =", inferred_date_operator)
    print("[NUMERIC DEBUG] values =", values)
    print("[NUMERIC DEBUG] has_range_pattern =", has_range_pattern)

    # 1. normalizza il nome campo
    if campo_target:
        parsed_query["campo_target"] = normalize_query_field_name(tipo_documento, campo_target)

    # 2. se c'è un numero ma non un campo, usa il campo numerico di default dello schema
    default_numeric_field = get_default_numeric_field_for_document_type(tipo_documento)

    if values and not parsed_query.get("campo_target") and default_numeric_field:
        # NON forzare il campo numerico se la query è chiaramente una query data
        if not parsed_query.get("data_da") and not parsed_query.get("data_a"):
            parsed_query["campo_target"] = default_numeric_field

    # 3. determina dinamicamente il tipo del campo
    resolved_field = parsed_query.get("campo_target")
    resolved_field_type = get_field_type_for_document(tipo_documento, resolved_field)

    if resolved_field_type == "number":
        parsed_query["tipo_valore"] = "number"

        # 🔥 FIX: NON cancellare la data se già presente
        if not parsed_query.get("data_da") and not parsed_query.get("data_a"):
            parsed_query["data_da"] = None
            parsed_query["data_a"] = None
        
    elif resolved_field_type == "date":
        parsed_query["tipo_valore"] = "date"

    # Se la query è una data ma GPT ha scelto un campo non-date o non valido
    # per quel tipo documento, riallinea dinamicamente al primo campo date disponibile
    if (
        resolved_field_type != "number"
        and (
            parsed_query.get("tipo_valore") == "date"
            or (
                inferred_date_operator in {">", ">=", "<", "<=", "between"}
                and not values
            )
            or (
                (parsed_query.get("data_da") or parsed_query.get("data_a"))
                and not values
            )
        )
    ):
        field_types = get_document_field_types(tipo_documento)
        current_field = parsed_query.get("campo_target")
        current_field_type = get_field_type_for_document(tipo_documento, current_field)

        if current_field_type != "date":
            first_date_field = next(
                (fname for fname, ftype in field_types.items() if ftype == "date"),
                None
            )
            if first_date_field:
                parsed_query["campo_target"] = first_date_field
                parsed_query["tipo_valore"] = "date"
                
            
    # 4. logica numerica dinamica guidata dallo schema
    if parsed_query.get("tipo_valore") == "number":
        if has_range_pattern and len(numeric_values) >= 2:
            parsed_query["operatore"] = "between"
            parsed_query["valore_target"] = numeric_values[:2]
        else:
            default_numeric_operator = (
                get_default_numeric_operator_for_document_type(tipo_documento) or None
            )

            if inferred_operator in {">", "<", ">=", "<=", "between"}:
                parsed_query["operatore"] = inferred_operator
            elif numeric_values:
                parsed_query["operatore"] = default_numeric_operator
            else:
                parsed_query["operatore"] = default_numeric_operator

            if len(numeric_values) >= 1:
                parsed_query["valore_target"] = numeric_values[0]

    # pulisci la data solo se davvero non ci sono date nella query
    if not date_values:
        parsed_query["data_da"] = None
        parsed_query["data_a"] = None

    # 5. logica date dinamica
    if parsed_query.get("tipo_valore") == "date":
        if inferred_date_operator == "between":
            parsed_query["operatore"] = "between"
        elif inferred_date_operator in {">", ">=", "<", "<="}:
            parsed_query["operatore"] = inferred_date_operator
        elif not parsed_query.get("operatore"):
            if parsed_query.get("data_da") and parsed_query.get("data_a"):
                parsed_query["operatore"] = "between"
            elif parsed_query.get("data_da"):
                parsed_query["operatore"] = ">="
            elif parsed_query.get("data_a"):
                parsed_query["operatore"] = "<="
            elif parsed_query.get("valore_target"):
                parsed_query["operatore"] = "="

    # 6. riallinea anche le conditions
    normalized_conditions = []
    
    for cond in parsed_query.get("conditions", []):
        if not isinstance(cond, dict):
            normalized_conditions.append(cond)
            continue

        cond_copy = dict(cond)

        field_name = cond_copy.get("field")
        target_name = cond_copy.get("target")

        target_norm = normalize_text(str(target_name or ""))
        field_norm = normalize_text(str(field_name or ""))

        is_tipo_condition = (
            target_norm in {"tipo_documento", "tipo documento"}
            or field_norm in {"tipo_documento", "tipo documento"}
        )

        if field_name and not is_tipo_condition:
            cond_copy["field"] = normalize_query_field_name(tipo_documento, field_name)

        cond_field_type = None if is_tipo_condition else get_field_type_for_document(
            tipo_documento,
            cond_copy.get("field")
        )

        if cond_field_type == "number" or cond_copy.get("value_type") == "number":
            cond_copy["value_type"] = "number"

            if has_range_pattern and len(numeric_values) >= 2:
                cond_copy["operator"] = "between"
                cond_copy["value"] = numeric_values[:2]
            else:
                cond_copy["operator"] = parsed_query.get("operatore")
                cond_copy["value"] = parsed_query.get("valore_target")

        elif cond_field_type == "date" or cond_copy.get("value_type") == "date":
            cond_copy["value_type"] = "date"
            cond_copy["operator"] = parsed_query.get("operatore")

            raw_value = None

            if parsed_query.get("operatore") == "between":
                if parsed_query.get("data_da") and parsed_query.get("data_a"):
                    raw_value = [parsed_query.get("data_da"), parsed_query.get("data_a")]
            elif parsed_query.get("operatore") in {">", ">="}:
                raw_value = parsed_query.get("data_da") or parsed_query.get("valore_target")
            elif parsed_query.get("operatore") in {"<", "<="}:
                raw_value = parsed_query.get("data_a") or parsed_query.get("valore_target")
            else:
                raw_value = parsed_query.get("valore_target")

            if isinstance(raw_value, list):
                cond_copy["value"] = [
                    normalize_date_value(v, parsed_query.get("operatore")) for v in raw_value
                ]
            else:
                cond_copy["value"] = normalize_date_value(
                    raw_value,
                    parsed_query.get("operatore")
                )

        else:
            normalized_conditions.append(cond_copy)
            continue

        normalized_conditions.append(cond_copy)
    # 7. se manca una condition numerica, creala
    has_numeric_condition = any(
        isinstance(cond, dict) and cond.get("value_type") == "number"
        for cond in normalized_conditions
    )

    if (
        parsed_query.get("tipo_valore") == "number"
        and parsed_query.get("campo_target")
        and parsed_query.get("valore_target") not in [None, "", []]
        and not has_numeric_condition
    ):
        normalized_conditions.append({
            "target": "business_field",
            "field": parsed_query.get("campo_target"),
            "operator": parsed_query.get("operatore"),
            "value": parsed_query.get("valore_target"),
            "value_type": "number"
        })

    # 8. se manca una condition date, creala
    has_date_condition = any(
        isinstance(cond, dict) and cond.get("value_type") == "date"
        for cond in normalized_conditions
    )

    if (
        parsed_query.get("tipo_valore") == "date"
        and parsed_query.get("campo_target")
        and not has_date_condition
    ):
        date_value = None

        if parsed_query.get("operatore") == "between":
            if parsed_query.get("data_da") and parsed_query.get("data_a"):
                date_value = [parsed_query.get("data_da"), parsed_query.get("data_a")]
        elif parsed_query.get("operatore") in {">", ">="}:
            date_value = parsed_query.get("data_da") or parsed_query.get("valore_target")
        elif parsed_query.get("operatore") in {"<", "<="}:
            date_value = parsed_query.get("data_a") or parsed_query.get("valore_target")
        else:
            date_value = normalize_date_value(
                parsed_query.get("valore_target"),
                parsed_query.get("operatore")
            )

        normalized_conditions.append({
            "target": "business_field",
            "field": parsed_query.get("campo_target"),
            "operator": parsed_query.get("operatore"),
            "value": date_value,
            "value_type": "date"
        })

    print("[NORMALIZE FINAL OPERATOR]", parsed_query.get("operatore"))
    print("[NORMALIZE FINAL VALUE]", parsed_query.get("valore_target"))
    print("[NORMALIZE FINAL CONDITIONS]", normalized_conditions)

    parsed_query["conditions"] = normalized_conditions
    return parsed_query

def normalize_date_value(value: str, operator: str | None) -> str | None:
    from datetime import date

    # caso: anno puro
    if isinstance(value, str) and len(value) == 4 and value.isdigit():
        year = int(value)

        if operator in [">", ">="]:
            return f"{year}-12-31"
        elif operator in ["<", "<="]:
            return f"{year}-01-01"
        elif operator == "between":
            return [f"{year}-01-01", f"{year}-12-31"]
        else:
            return f"{year}-01-01"

    return value
    
def infer_date_operator_from_query(query: str | None) -> str | None:
    q = normalize_text(query)

    if not q:
        return None

    date_operator_patterns = {
        ">": [
            r"\bdopo\b",
            r"\bsuccessivo a\b",
            r"\bsuccessiva a\b",
            r"\boltre il\b",
        ],
        ">=": [
            r"\bdal\b",
            r"\ba partire dal\b",
            r"\bda\b\s+\d{1,2}[/-]\d{1,2}[/-]\d{2,4}",
        ],
        "<": [
            r"\bprima di\b",
             r"\bprima del\b",
            r"\bprecedente a\b",
            r"\bantecedente a\b",
        ],
        "<=": [
            r"\bfino al\b",
            r"\bentro il\b",
            r"\bnon oltre il\b",
        ],
        "between": [
            r"\btra\b.+\be\b",
            r"\bda\b.+\ba\b",
        ],
    }

    for operator, patterns in date_operator_patterns.items():
        for pattern in patterns:
            if re.search(pattern, q):
                return operator

    return None

def is_likely_year(value: str, query: str) -> bool:
    if not value.isdigit():
        return False

    q = normalize_text(query)

    if len(value) == 4 and 1900 <= int(value) <= 2100:
        if re.search(r"\b(dopo|prima|dal|entro|tra|fino)\b", q):
            return True

    return False