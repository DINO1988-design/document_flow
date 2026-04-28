#search_business_rules.py
from __future__ import annotations
import unicodedata
import json
import re
from datetime import datetime, date
from difflib import SequenceMatcher

from src.document_field_normalizer import (
    normalize_text,
    DOCUMENT_TYPE_ALIASES,
    DOCUMENT_FIELD_ALIASES
)

from src.document_field_flattener import flatten_document_fields


def tokenize_text(value: str | None) -> list[str]:
    norm = normalize_text(value)
    if not norm:
        return []
    return [t for t in norm.split() if len(t) > 1]


def extract_meaningful_keyword_tokens(keyword: str) -> list[str]:
    generic_tokens = {
        "documento", "documenti", "doc", "docs", "file", "files",
        "di", "del", "della", "dello", "dei", "degli", "delle",
        "con", "da", "a", "in", "su", "per", "tra", "fra",
        "che", "sono", "sia", "ma", "parlano"
    }

    return [
        t for t in tokenize_text(keyword)
        if len(t) >= 3 and t not in generic_tokens
    ]
    
from collections import Counter

def build_document_frequency(texts):
    df = Counter()

    for text in texts or []:
        tokens = set(tokenize_text(text))
        for token in tokens:
            df[token] += 1

    return dict(df)


def compute_weighted_keyword_coverage(keyword, source_text, document_frequency):
    kw_tokens = extract_meaningful_keyword_tokens(keyword)

    if not kw_tokens:
        return 0.0

    src_norm = normalize_text(source_text)
    if not src_norm:
        return 0.0

    total_weight = 0.0
    matched_weight = 0.0

    for token in kw_tokens:
        df = document_frequency.get(token, 1)
        weight = 1 / (1 + df)

        total_weight += weight

        if token in src_norm:
            matched_weight += weight

    if total_weight == 0:
        return 0.0

    return matched_weight / total_weight
    
def similarity(a: str | None, b: str | None) -> float:
    a_norm = normalize_text(a)
    b_norm = normalize_text(b)

    if not a_norm or not b_norm:
        return 0.0

    if a_norm == b_norm:
        return 1.0

    return SequenceMatcher(None, a_norm, b_norm).ratio()


# =========================
# ALIAS CAMPI
# =========================

FIELD_ALIASES = {
    "ral": [
        "ral",
        "retribuzione",
        "retribuzione annua lorda",
        "retribuzione_annua_lorda",
        "compenso_annuo",
        "stipendio_annuo",
        "reddito_annuo",
        "salario_annuo"
    ],
    "netto": [
        "netto",
        "netto_a_pagare",
        "totale_netto",
        "importo_netto",
        "netto da pagare",
        "netto pagato",
        "net pay"
    ],
    "lordo": [
        "lordo",
        "retribuzione_lorda",
        "totale_lordo",
        "importo_lordo",
        "gross",
        "gross_pay"
    ],
    "totale": [
        "totale",
        "totale_fattura",
        "importo_totale",
        "total_amount",
        "grand_total",
        "amount_total",
        "totale documento"
    ],
    "beneficiario": [
        "beneficiario",
        "recipient",
        "destinatario",
        "payee",
        "intestatario",
        "nome beneficiario"
    ],

    # Campo generico tenuto per compatibilità
    "partenza": [
        "partenza",
        "from",
        "departure",
        "departure_city",
        "departure airport",
        "origin",
        "route_from",
        "da",
        "aeroporto_partenza",
        "citta_partenza"
    ],

    # Nuovi campi più specifici
    "partenza_luogo": [
        "partenza",
        "from",
        "departure",
        "origin",
        "aeroporto_partenza",
        "citta_partenza",
        "porto_partenza",
        "departure_city",
        "departure_airport",
        "abfahrtshafen",
        "porto"
    ],
    "partenza_data": [
        "data_partenza",
        "departure_date",
        "data_volo",
        "departure_day"
    ],
    "partenza_ora": [
        "ora_partenza",
        "departure_time",
        "abfahrtszeit"
    ],

    "arrivo_luogo": [
        "arrivo",
        "to",
        "arrival",
        "destination",
        "aeroporto_arrivo",
        "citta_arrivo",
        "porto_arrivo",
        "arrival_city",
        "arrival_airport",
        "ankunftshafen",
        "porto"
    ],
    "arrivo_data": [
        "data_arrivo",
        "arrival_date"
    ],
    "arrivo_ora": [
        "ora_arrivo",
        "arrival_time",
        "ankunftszeit"
    ],

    "data_scadenza": [
        "data_scadenza",
        "data scadenza",
        "expiry",
        "expiration_date",
        "date_of_expiry",
        "expiry_date",
        "scadenza"
    ],
}




def get_field_aliases(canonical_field: str | None) -> list[str]:
    if not canonical_field:
        return []

    canonical_norm = normalize_text(canonical_field)
    aliases = FIELD_ALIASES.get(canonical_norm, [])

    if canonical_norm not in aliases:
        aliases = [canonical_norm] + aliases

    return [normalize_text(a) for a in aliases if a]


def try_parse_json_like(value):
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


def is_alias_related(alias: str, key_path_norm: str) -> bool:
    if not alias or not key_path_norm:
        return False

    alias = normalize_text(alias)
    key_path_norm = normalize_text(key_path_norm)

    if alias == key_path_norm:
        return True

    key_tokens = set(tokenize_text(key_path_norm))
    alias_tokens = set(tokenize_text(alias))

    if not key_tokens or not alias_tokens:
        return False

    if alias_tokens.issubset(key_tokens):
        return True

    if alias in key_path_norm or key_path_norm in alias:
        return True

    return False


# =========================
# NUMERI
# =========================

def parse_number(value: str | int | float | None) -> float | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    s = str(value).strip().lower()
    if not s:
        return None

    s = s.replace("€", "")
    s = s.replace("eur", "")
    s = s.replace("euro", "")
    s = s.replace("h", "")
    s = s.replace("ore", "")
    s = s.replace(" ", "")

    s = re.sub(r"[^0-9,.\-]", "", s)

    if not s:
        return None

    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        if s.count(",") == 1:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "." in s:
        if s.count(".") > 1:
            s = s.replace(".", "")

    try:
        return float(s)
    except Exception:
        return None


# =========================
# DATE
# =========================

def parse_date_value(value: str | None) -> date | None:
    if not value:
        return None

    s = str(value).strip()
    if not s:
        return None

    formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y/%m/%d",
        "%Y%m%d",
        "%d%m%Y",
        "%y%m%d",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)

            if fmt == "%y%m%d":
                yy = int(s[:2])
                current_yy = int(datetime.utcnow().strftime("%y"))
                year = 1900 + yy if yy > current_yy + 20 else 2000 + yy
                dt = dt.replace(year=year)

            return dt.date()
        except Exception:
            pass

    return None


# =========================
# TIPI FOGLIA
# =========================

def detect_leaf_kind(field_name: str, field_path: str, raw_value) -> str:
    field_name_norm = normalize_text(field_name)
    field_path_norm = normalize_text(field_path)
    combined = f"{field_name_norm} {field_path_norm}"

    # PRIMA guarda il valore reale
    if parse_number(raw_value) is not None:
        return "number"

    if parse_date_value(str(raw_value)) is not None:
        return "date"

    # POI usa gli indizi del nome campo/path
    if any(x in combined for x in ["ora", "time", "hour", "zeit"]):
        return "time"

    if any(x in combined for x in ["data", "date", "giorno", "scadenza", "expiry"]):
        return "date"

    if any(x in combined for x in [
        "city", "citta", "aeroporto", "airport", "porto",
        "from", "departure", "origin", "to", "arrival", "destination",
        "hafen"
    ]):
        return "location"

    return "string"


def expected_leaf_kinds(canonical_field: str | None, value_type: str | None) -> set[str]:
    field = normalize_text(canonical_field)

    mapping = {
        "ral": {"number"},
        "netto": {"number"},
        "lordo": {"number"},
        "totale": {"number"},

        "beneficiario": {"string"},

        "data_scadenza": {"date"},

        "partenza": {"location", "string"},
        "partenza_luogo": {"location", "string"},
        "partenza_data": {"date"},
        "partenza_ora": {"time", "string"},

        "arrivo_luogo": {"location", "string"},
        "arrivo_data": {"date"},
        "arrivo_ora": {"time", "string"},
    }

    if field in mapping:
        return mapping[field]

    if value_type == "number":
        return {"number"}
    if value_type == "date":
        return {"date"}

    return {"string", "location", "time"}


# =========================
# FLATTEN CAMPI
# =========================

def flatten_field_value(raw_key: str, raw_value, parent_path: str = "") -> list[dict]:
    """
    Trasforma un campo anche annidato in foglie confrontabili.
    Esempi:
    - compenso -> {"lordo": "...", "netto": "..."}
      => compenso.lordo, compenso.netto
    - partenza -> {"city": "Milano", "time": "10:00"}
      => partenza.city, partenza.time
    """
    items = []

    base_key = normalize_text(raw_key)
    current_path = f"{parent_path}.{base_key}".strip(".") if parent_path else base_key

    raw_value = try_parse_json_like(raw_value)

    if isinstance(raw_value, dict):
        for sub_key, sub_val in raw_value.items():
            items.extend(flatten_field_value(str(sub_key), sub_val, current_path))
        return items

    if isinstance(raw_value, list):
        for item in raw_value:
            if isinstance(item, dict):
                for sub_key, sub_val in item.items():
                    items.extend(flatten_field_value(str(sub_key), sub_val, current_path))
            else:
                items.append({
                    "field_name": raw_key,
                    "field_path": current_path,
                    "field_name_norm": normalize_text(raw_key),
                    "field_path_norm": normalize_text(current_path),
                    "raw_value": item,
                    "leaf_kind": detect_leaf_kind(raw_key, current_path, item),
                })
        return items

    items.append({
        "field_name": raw_key,
        "field_path": current_path,
        "field_name_norm": normalize_text(raw_key),
        "field_path_norm": normalize_text(current_path),
        "raw_value": raw_value,
        "leaf_kind": detect_leaf_kind(raw_key, current_path, raw_value),
    })
    return items


# =========================
# RISOLUZIONE CAMPI DOCUMENTO
# =========================

def resolve_matching_fields(fields: dict, canonical_field: str) -> list[dict]:
    """
    Trova tutti i campi candidati compatibili con un campo canonico.
    Supporta:
    - match esatto
    - nested keys / suffix match (es. travel_details.total_cost -> total_cost)
    - match prudente per contenimento
    """
    if not fields or not canonical_field:
        return []

    flat_fields = flatten_document_fields(fields or {})
    canonical_norm = normalize_text(canonical_field)

    # prova a trovare il tipo documento più probabile dal campo canonico
    candidate_aliases = [canonical_field]

    for doc_type, schema in DOCUMENT_FIELD_ALIASES.items():
        canonical_fields = schema.get("canonical_fields", {})
        if canonical_field in canonical_fields:
            candidate_aliases.extend(canonical_fields.get(canonical_field, []))

    alias_norms = []
    seen_aliases = set()

    for alias in candidate_aliases:
        alias_norm = normalize_text(alias)
        if alias_norm and alias_norm not in seen_aliases:
            seen_aliases.add(alias_norm)
            alias_norms.append(alias_norm)

    results = []

    for field_name, raw_value in flat_fields.items():
        field_norm = normalize_text(field_name)
        if not field_norm:
            continue

        matched = False

        # 1. match esatto
        if field_norm == canonical_norm or field_norm in alias_norms:
            matched = True

        # 2. suffix match per campi nested
        if not matched:
            for alias_norm in alias_norms:
                if field_norm.endswith(alias_norm):
                    matched = True
                    break

        # 3. contenimento prudente
        if not matched:
            for alias_norm in alias_norms:
                if alias_norm in field_norm:
                    matched = True
                    break

        if not matched:
            continue

        leaf_kind = None

        if isinstance(raw_value, (int, float)):
            leaf_kind = "number"
        elif isinstance(raw_value, str):
            raw_value_norm = normalize_text(raw_value)

            # il kind reale verrà poi confermato da parse_number/parse_date_value
            leaf_kind = "text"

            if parse_number(raw_value) is not None:
                leaf_kind = "number"
            elif parse_date_value(raw_value) is not None:
                leaf_kind = "date"

        elif isinstance(raw_value, dict):
            leaf_kind = "object"
        elif isinstance(raw_value, list):
            leaf_kind = "array"

        results.append({
            "field_name": field_name,
            "field_path": field_name,
            "raw_value": raw_value,
            "leaf_kind": leaf_kind,
        })

    return results
# =========================
# MATCH STRINGA
# =========================

def string_contains_match(target_value: str | None, doc_value: str | None) -> tuple[bool, float]:
    target_norm = normalize_text(target_value)
    doc_norm = normalize_text(doc_value)

    if not target_norm or not doc_norm:
        return False, 0.0

    if target_norm in doc_norm:
        return True, 1.0

    target_tokens = tokenize_text(target_norm)
    doc_tokens = tokenize_text(doc_norm)

    if not target_tokens or not doc_tokens:
        return False, 0.0

    matched = 0
    used = set()

    for token in target_tokens:
        for i, dtoken in enumerate(doc_tokens):
            if i in used:
                continue

            sim = similarity(token, dtoken)
            if sim >= 0.84:
                matched += 1
                used.add(i)
                break

    coverage = matched / max(len(target_tokens), 1)
    return coverage >= 0.6, coverage


# =========================
# MATCH OPERATORI
# =========================

def compare_numbers(doc_number: float, operator: str | None, target_number: float) -> bool:
    if operator in (None, "", "=", "=="):
        return doc_number == target_number
    if operator == ">":
        return doc_number > target_number
    if operator == "<":
        return doc_number < target_number
    if operator == ">=":
        return doc_number >= target_number
    if operator == "<=":
        return doc_number <= target_number
    return False

def compare_number_between(doc_number: float, min_value: float, max_value: float) -> bool:
    if min_value > max_value:
        min_value, max_value = max_value, min_value
    return min_value <= doc_number <= max_value
    
def compare_dates(
    doc_date: date,
    operator: str | None,
    target_date: date | None = None,
    date_from: date | None = None,
    date_to: date | None = None
) -> bool:
    if operator == "between":
        if date_from and doc_date < date_from:
            return False
        if date_to and doc_date > date_to:
            return False
        return True

    if target_date is None:
        return False

    if operator in (None, "", "=", "=="):
        return doc_date == target_date
    if operator == ">":
        return doc_date > target_date
    if operator == "<":
        return doc_date < target_date
    if operator == ">=":
        return doc_date >= target_date
    if operator == "<=":
        return doc_date <= target_date

    return False


# =========================
# MOTORE GENERICO
# =========================

def evaluate_structured_field_match(
    fields: dict,
    canonical_field: str | None,
    operator: str | None,
    target_value: str | int | float | None,
    value_type: str | None,
    date_from: str | None = None,
    date_to: str | None = None
) -> dict:
    """
    Motore unico di valutazione campi:
    - numeri
    - date
    - stringhe

    Output standard per scoring/UI/debug.
    """
    result = {
        "matched": False,
        "match_strength": 0.0,
        "matched_field": None,
        "raw_value": None,
        "normalized_value": None,
        "operator": operator,
        "target_value": target_value,
        "value_type": value_type,
        "reason": "Nessun match strutturato",
    }

    if not canonical_field or not fields:
        result["reason"] = "Campo target assente"
        return result

    candidates = resolve_matching_fields(fields, canonical_field)
    if not candidates:
        result["reason"] = "Nessun campo alias trovato nel documento"
        return result

    allowed_kinds = expected_leaf_kinds(canonical_field, value_type)

    filtered_candidates = []
    for c in candidates:
        raw_val = c.get("raw_value")
        leaf_kind = c.get("leaf_kind")

        if value_type == "number":
            if leaf_kind == "number" or parse_number(raw_val) is not None:
                filtered_candidates.append(c)

        elif value_type == "date":
            if leaf_kind == "date" or parse_date_value(str(raw_val)) is not None:
                filtered_candidates.append(c)

        else:
            if leaf_kind in allowed_kinds:
                filtered_candidates.append(c)

    candidates = filtered_candidates

    if not candidates:
        result["reason"] = "Campi alias trovati ma nessun sotto-campo coerente col tipo richiesto"
        return result

    best = None

    # -------------------------
    # NUMERIC
    # -------------------------
    if value_type == "number":
        target_number = None
        min_number = None
        max_number = None

        if operator == "between":
            if isinstance(target_value, (list, tuple)) and len(target_value) >= 2:
                min_number = parse_number(target_value[0])
                max_number = parse_number(target_value[1])

            if min_number is None or max_number is None:
                result["reason"] = "Valori target numerici range non validi"
                return result
        else:
            target_number = parse_number(target_value)
            if target_number is None:
                result["reason"] = "Valore target numerico non valido"
                return result

        for candidate in candidates:
            raw_candidate_value = candidate["raw_value"]

            if isinstance(raw_candidate_value, (dict, list)):
                continue

            doc_number = parse_number(raw_candidate_value)
            if doc_number is None:
                continue

            if operator == "between":
                matched = compare_number_between(doc_number, min_number, max_number)
                target_repr = [min_number, max_number]
                reason_ok = "Campo numerico nel range"
                reason_ko = "Campo numerico trovato ma fuori range"
            else:
                matched = compare_numbers(doc_number, operator, target_number)
                target_repr = target_number
                reason_ok = "Valore numerico coerente"
                reason_ko = "Campo numerico trovato ma fuori soglia"

            strength = 1.0 if matched else 0.0

            current = {
                "matched": matched,
                "match_strength": strength,
                "matched_field": candidate.get("field_path") or candidate["field_name"],
                "raw_value": raw_candidate_value,
                "normalized_value": doc_number,
                "operator": operator,
                "target_value": target_repr,
                "value_type": value_type,
                "reason": reason_ok if matched else reason_ko,
            }

            if best is None:
                best = current
            else:
                if current["matched"] and not best["matched"]:
                    best = current
                elif current["matched"] == best["matched"]:
                    if current["match_strength"] > best["match_strength"]:
                        best = current

    # -------------------------
    # DATE
    # -------------------------
    elif value_type == "date":
        target_date = parse_date_value(target_value) if target_value else None
        parsed_date_from = parse_date_value(date_from) if date_from else None
        parsed_date_to = parse_date_value(date_to) if date_to else None

        for candidate in candidates:
            raw_candidate_value = candidate["raw_value"]

            if isinstance(raw_candidate_value, (dict, list)):
                continue

            doc_date = parse_date_value(raw_candidate_value)
            if doc_date is None:
                continue

            matched = compare_dates(
                doc_date=doc_date,
                operator=operator,
                target_date=target_date,
                date_from=parsed_date_from,
                date_to=parsed_date_to
            )
            strength = 1.0 if matched else 0.0

            current = {
                "matched": matched,
                "match_strength": strength,
                "matched_field": candidate.get("field_path") or candidate["field_name"],
                "raw_value": raw_candidate_value,
                "normalized_value": doc_date.isoformat(),
                "operator": operator,
                "target_value": target_value,
                "value_type": value_type,
                "reason": "Data coerente con il filtro" if matched else "Campo data trovato ma fuori range",
            }

            if best is None:
                best = current
            else:
                if current["matched"] and not best["matched"]:
                    best = current
                elif current["match_strength"] > best["match_strength"]:
                    best = current

    # -------------------------
    # STRING
    # -------------------------
    else:
        for candidate in candidates:
            raw_candidate_value = candidate["raw_value"]

            if isinstance(raw_candidate_value, (dict, list)):
                continue

            matched, coverage = string_contains_match(target_value, raw_candidate_value)

            current = {
                "matched": matched,
                "match_strength": coverage,
                "matched_field": candidate.get("field_path") or candidate["field_name"],
                "raw_value": raw_candidate_value,
                "normalized_value": normalize_text(raw_candidate_value),
                "operator": operator or "contains",
                "target_value": target_value,
                "value_type": "string",
                "reason": "Valore testuale coerente" if matched else "Campo testuale trovato ma non coerente",
            }

            if best is None:
                best = current
            else:
                if current["matched"] and not best["matched"]:
                    best = current
                elif current["match_strength"] > best["match_strength"]:
                    best = current

    if best:
        return best

    result["reason"] = "Campi trovati ma valori non confrontabili"
    return result