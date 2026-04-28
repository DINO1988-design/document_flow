#document_field_flattern.py
from __future__ import annotations

import json
from typing import Any


ROOT_WRAPPER_KEYS = {
    "data",
    "document",
    "documento",
    "contratto_assunzione",
    "contratto",
    "cedolino",
    "fattura",
    "passaporto",
    "biglietto",
    "bonifico",
}


def try_parse_json_like(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    s = value.strip()
    if not s:
        return value

    if not (
        (s.startswith("{") and s.endswith("}"))
        or (s.startswith("[") and s.endswith("]"))
    ):
        return value

    try:
        return json.loads(s)
    except Exception:
        return value


def unwrap_single_root(fields: dict[str, Any]) -> dict[str, Any]:
    """
    Se GPT restituisce un wrapper root inutile tipo:
    {
        "data": {...}
    }
    oppure
    {
        "contratto_assunzione": {...}
    }
    allora usa direttamente il contenuto interno.
    """
    if not isinstance(fields, dict) or not fields:
        return {}

    if len(fields) != 1:
        return fields

    only_key, only_value = next(iter(fields.items()))

    if not isinstance(only_value, dict):
        return fields

    if str(only_key).strip().lower() in ROOT_WRAPPER_KEYS:
        return only_value

    return fields


def flatten_value(prefix: str, value: Any) -> dict[str, Any]:
    flat: dict[str, Any] = {}
    parsed = try_parse_json_like(value)

    # dict annidato
    if isinstance(parsed, dict):
        for k, v in parsed.items():
            child_key = f"{prefix}.{k}" if prefix else str(k)
            flat.update(flatten_value(child_key, v))
        return flat

    # lista
    if isinstance(parsed, list):
        # lista di dict -> indicizza
        if any(isinstance(item, dict) for item in parsed):
            for idx, item in enumerate(parsed):
                if isinstance(item, dict):
                    for k, v in item.items():
                        child_key = f"{prefix}[{idx}].{k}" if prefix else f"[{idx}].{k}"
                        flat.update(flatten_value(child_key, v))
                else:
                    child_key = f"{prefix}[{idx}]" if prefix else f"[{idx}]"
                    flat[child_key] = item
            return flat

        # lista di scalari -> join
        scalar_items = [str(x) for x in parsed if x not in [None, ""]]
        if scalar_items:
            flat[prefix] = "; ".join(scalar_items)
        return flat

    # valore foglia
    flat[prefix] = parsed
    return flat


def flatten_document_fields(fields: dict[str, Any]) -> dict[str, Any]:
    """
    Trasforma qualsiasi struttura annidata in campi piatti.

    Esempio:
    {
        "data": {
            "recipient": {
                "name": "Mario Rossi"
            }
        }
    }

    ->
    {
        "recipient.name": "Mario Rossi"
    }
    """
    flat: dict[str, Any] = {}

    if not fields:
        return flat

    fields = unwrap_single_root(fields)

    for raw_key, raw_value in fields.items():
        key_str = str(raw_key).strip()
        flat.update(flatten_value(key_str, raw_value))

    return flat