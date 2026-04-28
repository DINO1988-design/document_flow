#document_reclassifier.py
import json
import openai

from src.document_field_normalizer import (
    get_allowed_document_types,
    normalize_document_type,
    get_document_schema,
)
def build_classification_system_prompt() -> str:
    allowed_types = get_allowed_document_types()

    schema_lines = []
    for doc_type in allowed_types:
        if doc_type == "generico":
            continue

        schema = get_document_schema(doc_type)
        canonical_fields = schema.get("canonical_fields", [])
        required_fields = schema.get("required_fields", [])

        schema_lines.append(
            "\n".join([
                f"TIPO: {doc_type}",
                f"- campi canonici tipici: {', '.join(canonical_fields) if canonical_fields else 'nessuno'}",
                f"- campi più indicativi: {', '.join(required_fields) if required_fields else 'nessuno'}",
            ])
        )

    schema_lines.append(
        "\n".join([
            "TIPO: generico",
            "- usa questo tipo solo se i dati non sono sufficienti o non corrispondono chiaramente a nessuno schema",
        ])
    )

    schema_text = "\n\n".join(schema_lines)

    return f"""
Sei un classificatore di documenti business.

Riceverai:
1. il tipo documento proposto da un precedente parsing AI
2. i campi strutturati estratti dal documento

Il tuo compito è determinare il tipo documento finale basandoti PRINCIPALMENTE sui campi estratti.

Puoi usare SOLO uno di questi tipi canonici:
{json.dumps(allowed_types, ensure_ascii=False)}

Schemi documentali di riferimento:
{schema_text}

Regole:
- NON inventare nuovi tipi documento.
- Usa SOLO uno dei tipi canonici ammessi.
- Basati PRINCIPALMENTE sui campi strutturati, non sul testo OCR grezzo.
- Il tipo documento proposto in precedenza è solo un indizio, non è vincolante.
- Se i dati non sono sufficienti o non corrispondono chiaramente a nessuno schema, usa "generico".
- Se non sei sicuro, restituisci il tipo più probabile con confidence più bassa.
- Non usare sinonimi nel valore finale: restituisci sempre il tipo canonico.

Rispondi SOLO con JSON valido:
{{
  "tipo_documento_finale": "uno dei tipi canonici ammessi",
  "confidence": 0.0,
  "reason": "breve motivazione"
}}
""".strip()
def classify_document_from_fields_with_gpt(fields: dict, tipo_gpt: str | None = None) -> dict:
    system_prompt = build_classification_system_prompt()

    user_payload = {
        "tipo_documento_proposto": tipo_gpt,
        "campi_estratti": fields,
    }

    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
        )

        content = response.choices[0].message.content.strip()

        if content.startswith("```"):
            content = content.replace("```json", "").replace("```", "").strip()

        data = json.loads(content)

        tipo_finale = normalize_document_type(
            data.get("tipo_documento_finale", tipo_gpt or "generico")
        )

        confidence = data.get("confidence", 0.0)
        reason = data.get("reason", "")

        try:
            confidence = float(confidence)
        except Exception:
            confidence = 0.0

        return {
            "tipo_documento_finale": tipo_finale,
            "confidence": confidence,
            "reason": reason,
        }

    except Exception as e:
        return {
            "tipo_documento_finale": normalize_document_type(tipo_gpt),
            "confidence": 0.0,
            "reason": f"fallback per errore classificazione GPT: {e}",
        }