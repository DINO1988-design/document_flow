import json
import openai

from src.search_business_rules import FIELD_ALIASES
from src.document_field_normalizer import (
    normalize_text,
    normalize_document_type,
    DOCUMENT_TYPE_ALIASES
)

GENERIC_DOCUMENT_TYPES = {
    "documento",
    "documenti",
    "doc",
    "docs",
    "file",
    "files",
    "atto",
    "atti",
    "pratica",
    "pratiche",
    "modulo",
    "moduli",
    "generico",
    "altro",
}

OPERATOR_ALIASES = {
    "=": "=",
    "==": "=",
    "equals": "=",
    "equal": "=",
    "is": "=",

    "!=": "!=",
    "<>": "!=",
    "not_equals": "!=",
    "different": "!=",

    ">": ">",
    "gt": ">",
    "greater_than": ">",

    "<": "<",
    "lt": "<",
    "less_than": "<",

    ">=": ">=",
    "gte": ">=",
    "greater_or_equal": ">=",

    "<=": "<=",
    "lte": "<=",
    "less_or_equal": "<=",

    "between": "between",
    "contains": "contains",
    "not_contains": "not_contains",
    "in": "in",
    "not_in": "not_in",
}

def sanitize_operator(value: str | None) -> str | None:
    if not value:
        return None

    v = normalize_text(value)
    return OPERATOR_ALIASES.get(v)


def sanitize_tipo_documento(value: str | None) -> str | None:
    if not value:
        return None

    normalized = normalize_document_type(value)

    if not normalized or normalized in GENERIC_DOCUMENT_TYPES or normalized == "generico":
        return None

    return normalized


def sanitize_campo_target(value: str | None) -> str | None:
    if not value:
        return None

    v = normalize_text(value)
    if not v:
        return None

    # se il campo coincide con un alias noto, restituisco il canonico
    for canonical, aliases in FIELD_ALIASES.items():
        variants = [canonical] + aliases
        for alias in variants:
            if normalize_text(alias) == v:
                return canonical

    return v


def normalize_keywords(
    keywords: list[str] | None,
    tipo_documento: str | None = None,
    persona: str | None = None,
    nome_file: str | None = None,
    campo_target: str | None = None,
    valore_target: str | None = None,
) -> list[str]:
    cleaned = []
    seen = set()

    blocked_values = {
        normalize_text(tipo_documento),
        normalize_text(persona),
        normalize_text(nome_file),
        normalize_text(campo_target),
        normalize_text(valore_target),
    }

    for kw in (keywords or []):
        kw_raw = str(kw).strip()
        kw_norm = normalize_text(kw_raw)

        if not kw_norm:
            continue

        if kw_norm in blocked_values:
            continue

        if len(kw_norm) < 2:
            continue

        if kw_norm not in seen:
            seen.add(kw_norm)
            cleaned.append(kw_raw)

    return cleaned


def normalize_conditions(conditions: list[dict] | None) -> list[dict]:
    normalized = []

    for cond in (conditions or []):
        if not isinstance(cond, dict):
            continue

        target = normalize_text(cond.get("target"))
        field = normalize_text(cond.get("field"))
        operator = sanitize_operator(cond.get("operator"))
        value = cond.get("value")
        value_type = cond.get("value_type")

        if target in {"tipo documento", "tipo_documento"}:
            target = "tipo_documento"

        if field in {"tipo documento", "tipo_documento"}:
            field = "tipo_documento"

        if value_type == "document_type":
            if isinstance(value, list):
                value = [
                    normalize_document_type(v)
                    for v in value
                    if normalize_document_type(v) != "generico"
                ]
            else:
                value = normalize_document_type(value)
                if value == "generico":
                    value = None

        normalized.append({
            "target": target,
            "field": field,
            "operator": operator,
            "value": value,
            "value_type": value_type,
        })

    return [
        c for c in normalized
        if c.get("field") and c.get("operator") and c.get("value") not in [None, "", []]
    ]
    
def normalize_parsed_query(data: dict) -> dict:
    result = {
        "intent": data.get("intent", "search_documents"),
        "tipo_documento": data.get("tipo_documento"),
        "exclude_tipo_documento": data.get("exclude_tipo_documento", []),
        "persona": data.get("persona"),
        "nome_file": data.get("nome_file"),
        "campo_target": data.get("campo_target"),
        "operatore": data.get("operatore"),
        "valore_target": data.get("valore_target"),
        "tipo_valore": data.get("tipo_valore"),
        "data_da": data.get("data_da"),
        "data_a": data.get("data_a"),
        "campi_rilevanti": data.get("campi_rilevanti", []),
        "keywords": data.get("keywords", []),
        "exclude_keywords": data.get("exclude_keywords", []),
        "conditions": data.get("conditions", []),
        "confidence": data.get("confidence", 0.0),
    }

    result["tipo_documento"] = sanitize_tipo_documento(result["tipo_documento"])
    result["operatore"] = sanitize_operator(result["operatore"])

    result["exclude_tipo_documento"] = [
        x for x in (
            normalize_document_type(v)
            for v in (result.get("exclude_tipo_documento") or [])
        )
        if x and x != "generico"
    ]

    result["campo_target"] = sanitize_campo_target(result["campo_target"])

    result["keywords"] = normalize_keywords(
        result["keywords"],
        tipo_documento=result["tipo_documento"],
        persona=result["persona"],
        nome_file=result["nome_file"],
        campo_target=result["campo_target"],
        valore_target=result["valore_target"],
    )

    result["exclude_keywords"] = normalize_keywords(
        result["exclude_keywords"],
        tipo_documento=result["tipo_documento"],
        persona=result["persona"],
        nome_file=result["nome_file"],
        campo_target=result["campo_target"],
        valore_target=result["valore_target"],
    )

    if not result["campo_target"]:
        result["operatore"] = None
        result["valore_target"] = None
        result["tipo_valore"] = None

    result["conditions"] = normalize_conditions(result.get("conditions"))

    # fallback compatibilità con formato attuale
    if not result["conditions"]:
        if result["tipo_documento"]:
            result["conditions"].append({
                "target": "tipo_documento",
                "field": "tipo_documento",
                "operator": "=",
                "value": result["tipo_documento"],
                "value_type": "document_type",
            })

        if result["exclude_tipo_documento"]:
            result["conditions"].append({
                "target": "tipo_documento",
                "field": "tipo_documento",
                "operator": "not_in" if len(result["exclude_tipo_documento"]) > 1 else "!=",
                "value": result["exclude_tipo_documento"] if len(result["exclude_tipo_documento"]) > 1 else result["exclude_tipo_documento"][0],
                "value_type": "document_type",
            })

        if result["campo_target"] and result["operatore"]:
            if result["operatore"] == "between":
                result["conditions"].append({
                    "target": "business_field",
                    "field": result["campo_target"],
                    "operator": "between",
                    "value": [result["data_da"], result["data_a"]],
                    "value_type": result["tipo_valore"],
                })
            else:
                result["conditions"].append({
                    "target": "business_field",
                    "field": result["campo_target"],
                    "operator": result["operatore"],
                    "value": result["valore_target"],
                    "value_type": result["tipo_valore"],
                })

    return result
    
def parse_search_query_with_gpt(query: str):
    system_prompt = """
Sei un assistente che interpreta query in linguaggio naturale per cercare documenti.

Devi capire:
- il tipo documento
- eventuale persona
- eventuale nome file
- eventuale campo target della ricerca
- eventuale operatore
- eventuale valore target
- eventuali filtri data
- parole chiave utili

Rispondi SOLO con JSON valido nel formato:

{
  "intent": "search_documents",
  "tipo_documento": null,
  "exclude_tipo_documento": [],
  "persona": null,
  "nome_file": null,
  "campo_target": null,
  "operatore": null,
  "valore_target": null,
  "tipo_valore": null,
  "data_da": null,
  "data_a": null,
  "campi_rilevanti": [],
  "keywords": [],
  "exclude_keywords": [],
  "conditions": [],
  "confidence": 0.0
}

Regole di interpretazione:

1. "tipo_documento" va valorizzato solo se la query identifica chiaramente una classe documentale specifica.
   Esempi di classi specifiche: passaporto, contratto, cedolino, fattura, ricevuta di bonifico, biglietto.
   Se invece il riferimento è generico (es. documento, documenti, file, pratica, modulo), "tipo_documento" deve essere null.

2. "persona" va valorizzato solo se la query cerca chiaramente una persona fisica, intestatario, beneficiario o nominativo.

3. "nome_file" va valorizzato solo se la query cerca chiaramente un file specifico o una parte riconoscibile del nome file.

4. "campo_target" va valorizzato solo se la query esprime chiaramente un filtro strutturato su un campo business canonico.
   Esempi tipici: ral, netto, lordo, totale, beneficiario, partenza, partenza_data, arrivo_luogo, data_scadenza.

5. "operatore" deve essere uno tra >, <, >=, <=, =, between, contains, oppure null.

6. "valore_target" e "tipo_valore" vanno valorizzati solo se esiste davvero un filtro strutturato coerente con "campo_target".

7. "data_da" e "data_a" vanno valorizzati solo se la query contiene un range temporale chiaro oppure un anno che rappresenta un intervallo.

8. Se una parte della query rappresenta contenuto libero o tema del documento, e non è riconducibile a:
   - tipo documento
   - persona
   - nome file
   - campo business canonico
   allora NON forzarla in "campo_target".
   Inseriscila invece in "keywords".

9. Una query può contenere contemporaneamente:
   - tipo documento
   - persona
   - nome file
   - filtro strutturato
   - contenuto libero
   - filtri temporali

10. I campi devono essere coerenti tra loro:
   - se "campo_target" è null, allora "operatore", "valore_target" e "tipo_valore" devono essere null
   - se il vincolo principale è solo testuale o tematico, lascia "campo_target" a null
   - non usare "tipo_documento" per valori generici

11. "keywords" deve contenere solo il contenuto libero utile alla ricerca testuale o semantica del documento, evitando duplicati o valori già espressi in altri campi.

12. Se la query contiene esclusioni di tipo documento, valorizza "exclude_tipo_documento".
    Esempi:
    - "non cedolino" -> ["cedolino"]
    - "esclusi contratto e bonifico" -> ["contratto", "bonifico"]

13. Se la query contiene esclusioni testuali, valorizza "exclude_keywords".

14. Se la query esprime una condizione logica chiara, valorizza anche "conditions".
    Esempi:
    - tipo documento uguale a contratto
    - tipo documento diverso da cedolino
    - tipo documento in [contratto, bonifico]
    - ral > 40000
    - data_scadenza between X e Y

15. "conditions" è la forma strutturata preferita, ma per compatibilità continua a valorizzare anche i campi legacy quando possibile.

Esempi:

Query: contratti con RAL superiore a 40000
{
  "intent": "search_documents",
  "tipo_documento": "contratto",
  "persona": null,
  "nome_file": null,
  "campo_target": "ral",
  "operatore": ">",
  "valore_target": "40000",
  "tipo_valore": "number",
  "data_da": null,
  "data_a": null,
  "campi_rilevanti": ["ral", "retribuzione_annua_lorda"],
  "keywords": [],
  "confidence": 0.96
}

Query: passaporti in scadenza nel 2026
{
  "intent": "search_documents",
  "tipo_documento": "passaporto",
  "persona": null,
  "nome_file": null,
  "campo_target": "data_scadenza",
  "operatore": "between",
  "valore_target": null,
  "tipo_valore": "date",
  "data_da": "2026-01-01",
  "data_a": "2026-12-31",
  "campi_rilevanti": ["data_scadenza", "expiry"],
  "keywords": [],
  "confidence": 0.97
}

Query: contratti con clausola di non concorrenza
{
  "intent": "search_documents",
  "tipo_documento": "contratto",
  "persona": null,
  "nome_file": null,
  "campo_target": null,
  "operatore": null,
  "valore_target": null,
  "tipo_valore": null,
  "data_da": null,
  "data_a": null,
  "campi_rilevanti": [],
  "keywords": ["clausola di non concorrenza"],
  "confidence": 0.95
}

Query: documenti con rimborso chilometrico
{
  "intent": "search_documents",
  "tipo_documento": null,
  "persona": null,
  "nome_file": null,
  "campo_target": null,
  "operatore": null,
  "valore_target": null,
  "tipo_valore": null,
  "data_da": null,
  "data_a": null,
  "campi_rilevanti": [],
  "keywords": ["rimborso chilometrico"],
  "confidence": 0.95
}

Query: documenti di noureddine slimani che non sono cedolino
{
  "intent": "search_documents",
  "tipo_documento": null,
  "exclude_tipo_documento": ["cedolino"],
  "persona": "noureddine slimani",
  "nome_file": null,
  "campo_target": null,
  "operatore": null,
  "valore_target": null,
  "tipo_valore": null,
  "data_da": null,
  "data_a": null,
  "campi_rilevanti": [],
  "keywords": [],
  "exclude_keywords": [],
  "conditions": [
    {
      "target": "tipo_documento",
      "field": "tipo_documento",
      "operator": "!=",
      "value": "cedolino",
      "value_type": "document_type"
    }
  ],
  "confidence": 0.96
}
Query: contratti o bonifici di noureddine slimani
{
  "intent": "search_documents",
  "tipo_documento": null,
  "exclude_tipo_documento": [],
  "persona": "noureddine slimani",
  "nome_file": null,
  "campo_target": null,
  "operatore": null,
  "valore_target": null,
  "tipo_valore": null,
  "data_da": null,
  "data_a": null,
  "campi_rilevanti": [],
  "keywords": [],
  "exclude_keywords": [],
  "conditions": [
    {
      "target": "tipo_documento",
      "field": "tipo_documento",
      "operator": "in",
      "value": ["contratto", "bonifico"],
      "value_type": "document_type"
    }
  ],
  "confidence": 0.95
}

REGOLE IMPORTANTI SUL TIPO DOCUMENTO:

- Se nella query compare un tipo documento (es. bonifico, contratto, cedolino, fattura, passaporto, biglietto),
  allora devi SEMPRE valorizzare il campo "tipo_documento" (se positivo)
  oppure "exclude_tipo_documento" (se negativo).

- "bonifico", "contratto", "cedolino", ecc NON sono keywords generiche:
  sono SEMPRE tipi documento.

- NON lasciare mai "tipo_documento" a null se è presente un tipo documento nella query.

- Esempi:

Query: documenti che sono bonifici
→ tipo_documento = "bonifico"

Query: documenti solo contratti
→ tipo_documento = "contratto"

Query: documenti che non sono cedolini
→ exclude_tipo_documento = ["cedolino"]
"""

    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": query}
            ]
        )

        content = response.choices[0].message.content.strip()

        if content.startswith("```"):
            content = content.replace("```json", "").replace("```", "").strip()

        data = json.loads(content)
        normalized = normalize_parsed_query(data)
        print("[QUERY GPT NORMALIZED]", normalized)
        return normalized

    except Exception as e:
        print(f"[QUERY GPT ERROR]: {e}")
        return normalize_parsed_query({
            "intent": "search_documents",
            "tipo_documento": None,
            "exclude_tipo_documento": [],
            "persona": None,
            "nome_file": None,
            "campo_target": None,
            "operatore": None,
            "valore_target": None,
            "tipo_valore": None,
            "data_da": None,
            "data_a": None,
            "campi_rilevanti": [],
            "keywords": [],
            "exclude_keywords": [],
            "confidence": 0.0
        })