import re

from src.document_field_normalizer import (
    normalize_text,
    normalize_document_type,
    DOCUMENT_TYPE_ALIASES,
)

from src.search_ranking import (
    tokenize_text,
    are_similar_tokens,
    is_generic_document_type,
)

from src.search_business_rules import (
    extract_meaningful_keyword_tokens,
)


   
def infer_tipo_documento_from_keywords(
    parsed_query: dict,
    min_alias_coverage: float = 0.6
) -> dict:
    """
    Promuove dinamicamente una keyword contenutistica a tipo_documento
    se i suoi token significativi matchano bene gli alias di un tipo documento.
    """

    if not isinstance(parsed_query, dict):
        return parsed_query

    # Se GPT ha già trovato un tipo documento vero, non tocchiamo nulla
    existing_tipo = normalize_document_type(parsed_query.get("tipo_documento"))
    if existing_tipo and not is_generic_document_type(existing_tipo):
        return parsed_query

    keywords = parsed_query.get("keywords") or []
    if not keywords:
        return parsed_query

    best_tipo = None
    best_score = 0.0
    consumed_keywords = set()

    for kw in keywords:
        kw_norm = normalize_text(kw)
        kw_tokens = extract_meaningful_keyword_tokens(kw_norm)

        if not kw_tokens:
            continue

        for canonical_tipo, aliases in DOCUMENT_TYPE_ALIASES.items():
            alias_scores = []

            all_aliases = [canonical_tipo, *(aliases or [])]

            for alias in all_aliases:
                alias_norm = normalize_text(alias)
                alias_tokens = extract_meaningful_keyword_tokens(alias_norm)

                if not alias_tokens:
                    continue

                matched = 0
                for kw_token in kw_tokens:
                    for alias_token in alias_tokens:
                        if are_similar_tokens(kw_token, alias_token, threshold=0.84):
                            matched += 1
                            break

                coverage = matched / max(len(kw_tokens), 1)
                alias_scores.append(coverage)

            alias_best = max(alias_scores) if alias_scores else 0.0

            if alias_best > best_score and alias_best >= min_alias_coverage:
                best_score = alias_best
                best_tipo = canonical_tipo
                consumed_keywords = {kw}

    if best_tipo:
        parsed_query["tipo_documento"] = best_tipo
        parsed_query["keywords"] = [k for k in keywords if k not in consumed_keywords]

    return parsed_query


def enrich_tipo_conditions_from_query_text(query: str, parsed_query: dict) -> dict:
    if not isinstance(parsed_query, dict):
        return parsed_query

    query_norm = normalize_text(query)

    detected_exclude = None
    detected_include = None

    print("[ENRICH QUERY] query_norm =", query_norm)    

    # se esiste già un tipo positivo vero, non tocchiamo nulla
    existing_tipo = normalize_document_type(parsed_query.get("tipo_documento"))
    if existing_tipo and not is_generic_document_type(existing_tipo):
        return parsed_query

    print("[ENRICH QUERY] detected_exclude =", detected_exclude)
    print("[ENRICH QUERY] detected_include =", detected_include)

    def detect_tipo_in_text(text: str) -> str | None:
        text_norm = normalize_text(text)
        if not text_norm:
            return None

        # 1) tentativo diretto sulla stringa intera
        normalized_tipo = normalize_document_type(text_norm)
        if normalized_tipo and not is_generic_document_type(normalized_tipo):
            return normalized_tipo

        # 2) prova dinamica su finestre di token
        tokens = tokenize_text(text_norm)
        if not tokens:
            return None

        candidates = []

        # unigrammi, bigrammi, trigrammi
        max_n = min(3, len(tokens))
        for n in range(1, max_n + 1):
            for i in range(len(tokens) - n + 1):
                phrase = " ".join(tokens[i:i+n]).strip()
                if phrase:
                    candidates.append(phrase)

        # prima prova col normalizer sui candidati
        for cand in candidates:
            cand_tipo = normalize_document_type(cand)
            if cand_tipo and not is_generic_document_type(cand_tipo):
                return cand_tipo

        # 3) fallback dinamico fuzzy sugli alias
        best_tipo = None
        best_score = 0.0

        for cand in candidates:
            cand_tokens = extract_meaningful_keyword_tokens(cand)
            if not cand_tokens:
                continue

            for canonical_tipo, aliases in DOCUMENT_TYPE_ALIASES.items():
                alias_list = [canonical_tipo, *(aliases or [])]

                for alias in alias_list:
                    alias_norm = normalize_text(alias)
                    alias_tokens = extract_meaningful_keyword_tokens(alias_norm)
                    if not alias_tokens:
                        continue

                    matched = 0
                    used = set()

                    for ct in cand_tokens:
                        for idx, at in enumerate(alias_tokens):
                            if idx in used:
                                continue
                            if are_similar_tokens(ct, at, threshold=0.84):
                                matched += 1
                                used.add(idx)
                                break

                    coverage = matched / max(len(alias_tokens), 1)

                    if coverage > best_score:
                        best_score = coverage
                        best_tipo = canonical_tipo

        if best_tipo and best_score >= 0.8:
            return best_tipo


        print("[DETECT TIPO TEXT] raw =", text)
        print("[DETECT TIPO TEXT] norm =", text_norm)
        print("[DETECT TIPO TEXT] candidates =", candidates)

        return None

    # pattern principali
    patterns = [
        r"non sono (.+?) ma sono (.+)",
        r"che non sono (.+?) ma sono (.+)",
        r"non sono (.+?) pero sono (.+)",
        r"che non sono (.+?) pero sono (.+)",
        r"esclusi (.+?) ma sono (.+)",
        r"tranne (.+?) ma sono (.+)",
    ]

    detected_exclude = None
    detected_include = None

    for pattern in patterns:
        m = re.search(pattern, query_norm)
        if not m:
            continue

        left = m.group(1).strip()
        right = m.group(2).strip()

        detected_exclude = detect_tipo_in_text(left)
        detected_include = detect_tipo_in_text(right)

        if detected_include:
            break

    if detected_include:
        parsed_query["tipo_documento"] = detected_include

        conditions = parsed_query.setdefault("conditions", [])
        already_has_include = any(
            normalize_text(c.get("target")) in {"tipo documento", "tipo_documento"}
            and normalize_text(c.get("field")) in {"tipo documento", "tipo_documento"}
            and str(c.get("operator") or "").strip() in {"=", "=="}
            and (normalize_document_type(c.get("value")) or normalize_text(c.get("value"))) == detected_include
            for c in conditions
            if isinstance(c, dict)
        )

        if not already_has_include:
            conditions.append({
                "target": "tipo_documento",
                "field": "tipo_documento",
                "operator": "=",
                "value": detected_include,
                "value_type": "document_type"
            })

    if detected_exclude:
        exclude_list = parsed_query.setdefault("exclude_tipo_documento", [])
        if detected_exclude not in exclude_list:
            exclude_list.append(detected_exclude)

        conditions = parsed_query.setdefault("conditions", [])
        already_has_exclude = any(
            normalize_text(c.get("target")) in {"tipo documento", "tipo_documento"}
            and normalize_text(c.get("field")) in {"tipo documento", "tipo_documento"}
            and str(c.get("operator") or "").strip() in {"!=", "<>", "not_in"}
            and (normalize_document_type(c.get("value")) or normalize_text(c.get("value"))) == detected_exclude
            for c in conditions
            if isinstance(c, dict)
        )

        if not already_has_exclude:
            conditions.append({
                "target": "tipo_documento",
                "field": "tipo_documento",
                "operator": "!=",
                "value": detected_exclude,
                "value_type": "document_type"
            })

    return parsed_query
 

def has_positive_tipo_condition(conditions: list[dict] | None) -> bool:
    if not conditions:
        return False

    for cond in conditions:
        if not isinstance(cond, dict):
            continue

        target_norm = normalize_text(str(cond.get("target") or ""))
        field_norm = normalize_text(str(cond.get("field") or ""))
        operator = str(cond.get("operator") or "").strip().lower()

        is_tipo_condition = (
            target_norm in {"tipo documento", "tipo_documento"}
            and field_norm in {"tipo documento", "tipo_documento"}
        )

        if is_tipo_condition and operator in {"=", "==", "in"}:
            return True

    return False

  

