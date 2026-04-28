#document_schema_registry.py
from __future__ import annotations

import re
import unicodedata

from src.document_field_normalizer import DOCUMENT_FIELD_ALIASES


DOCUMENT_TYPE_ALIASES = {
    "cedolino": [
        "cedolino", "busta paga", "cedolino paga", "cedolino stipendio", "payslip"
    ],
    "bonifico": [
        "bonifico", "ricevuta di bonifico", "contabile bancaria", "documento bancario", "ricevuta bancaria"
    ],
    "biglietto": [
        "biglietto", "biglietto aereo", "prenotazione", "ticket", "boarding pass"
    ],
    "passaporto": [
        "passaporto", "passport", "documento di identita", "documento di identità"
    ],
    "fattura": [
        "fattura", "invoice"
    ],
    "contratto": [
        "contratto", "contract", "lettera assunzione"
    ]
}


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""

    value = str(value).strip().lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(c for c in value if not unicodedata.combining(c))
    value = re.sub(r"[^\w\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def get_allowed_document_types() -> list[str]:
    return list(DOCUMENT_FIELD_ALIASES.keys()) + ["generico"]


def normalize_document_type(document_type: str | None) -> str:
    t = normalize_text(document_type)

    if not t:
        return "generico"

    for canonical, aliases in DOCUMENT_TYPE_ALIASES.items():
        normalized_aliases = [normalize_text(a) for a in aliases]
        if t == canonical or t in normalized_aliases:
            return canonical

    return t if t in get_allowed_document_types() else "generico"


def get_document_schema(document_type: str | None) -> dict:
    t = normalize_document_type(document_type)
    schema = DOCUMENT_FIELD_ALIASES.get(t, {})

    canonical_fields_map = schema.get("canonical_fields", {})
    required_fields = schema.get("required_fields", [])

    return {
        "canonical_fields": list(canonical_fields_map.keys()),
        "required_fields": required_fields
    }


def get_canonical_fields(document_type: str | None) -> list[str]:
    return get_document_schema(document_type).get("canonical_fields", [])


def get_required_fields(document_type: str | None) -> list[str]:
    return get_document_schema(document_type).get("required_fields", [])