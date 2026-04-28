from __future__ import annotations

from copy import deepcopy
from itertools import product
from typing import Any

from src.api_server import compute_document_score, SEARCH_SCORE_WEIGHTS


def rank_documents(
    query: str,
    parsed_query: dict,
    documents: list[dict],
    weights: dict,
) -> list[dict]:
    ranked = []

    for doc in documents:
        result = compute_document_score(
            query=query,
            parsed_query=parsed_query,
            row=doc["row"],
            fields=doc["fields"],
            semantic_score=doc.get("semantic_score", 0.0),
            best_chunk_text=doc.get("best_chunk_text", ""),
            raw_ocr_text=doc.get("raw_ocr_text", ""),
            weights=weights,
        )

        ranked.append(
            {
                "doc_id": doc["doc_id"],
                "score": result["score"],
                "persona_match": result["persona_match"],
                "tipo_match": result["tipo_match"],
                "filename_only_match": result["filename_only_match"],
            }
        )

    ranked.sort(key=lambda x: x["score"], reverse=True)
    return ranked


def get_benchmark_documents() -> list[dict]:
    return [
        {
            "doc_id": "ticket_makrem_milano_tunisi",
            "row": {
                "tipo_documento": "biglietto aereo",
                "file_path": "src/uploads/ticket_makrem_milano_tunisi.pdf",
            },
            "fields": {
                "passenger_name": "Makrem Slimani",
                "route": "Milano Tunisi",
                "booking_reference": "FR7621",
                "compagnia": "Ryanair",
            },
            "semantic_score": 0.84,
            "best_chunk_text": "Passenger Makrem Slimani flight Milano Tunisi Ryanair FR7621",
            "raw_ocr_text": "Passenger MAKREM SLIMANI Milano Tunisi FR7621",
        },
        {
            "doc_id": "ticket_makrem_roma_parigi",
            "row": {
                "tipo_documento": "ticket",
                "file_path": "src/uploads/ticket_makrem_roma_parigi.pdf",
            },
            "fields": {
                "passenger_name": "Makrem Slimani",
                "route": "Roma Parigi",
                "booking_reference": "AZ9912",
                "compagnia": "ITA Airways",
            },
            "semantic_score": 0.77,
            "best_chunk_text": "Passenger Makrem Slimani flight Roma Parigi AZ9912",
            "raw_ocr_text": "Passenger MAKREM SLIMANI Roma Parigi",
        },
        {
            "doc_id": "ticket_nesrine_roma_tunisi",
            "row": {
                "tipo_documento": "biglietto aereo",
                "file_path": "src/uploads/ticket_nesrine_roma_tunisi.pdf",
            },
            "fields": {
                "passenger_name": "Nesrine Ben Ali",
                "route": "Roma Tunisi",
                "booking_reference": "TU1455",
                "compagnia": "Tunisair",
            },
            "semantic_score": 0.82,
            "best_chunk_text": "Passenger Nesrine Ben Ali Roma Tunisi Tunisair TU1455",
            "raw_ocr_text": "Passenger NESRINE BEN ALI Roma Tunisi TU1455",
        },
        {
            "doc_id": "ticket_fatma_parigi_tunisi",
            "row": {
                "tipo_documento": "e-ticket",
                "file_path": "src/uploads/ticket_fatma_parigi_tunisi.pdf",
            },
            "fields": {
                "passenger_name": "Fatma Zahra",
                "route": "Parigi Tunisi",
                "booking_reference": "BJ3310",
                "compagnia": "Nouvelair",
            },
            "semantic_score": 0.75,
            "best_chunk_text": "Passenger Fatma Zahra Parigi Tunisi Nouvelair BJ3310",
            "raw_ocr_text": "Passenger FATMA ZAHRA Parigi Tunisi BJ3310",
        },
        {
            "doc_id": "ferry_amna_genova_tunisi",
            "row": {
                "tipo_documento": "biglietto traghetto",
                "file_path": "src/uploads/ferry_amna_genova_tunisi.pdf",
            },
            "fields": {
                "nome_passeggero": "Zaafouri Amna",
                "route": "Genova Tunisi",
                "compagnia": "GNV",
            },
            "semantic_score": 0.73,
            "best_chunk_text": "Passeggero Zaafouri Amna traghetto Genova Tunisi GNV",
            "raw_ocr_text": "PAX ZAAFOURI/AMNA GENOVA TUNISI",
        },
        {
            "doc_id": "ferry_ahmed_palermo_tunisi",
            "row": {
                "tipo_documento": "traghetto",
                "file_path": "src/uploads/ferry_ahmed_palermo_tunisi.pdf",
            },
            "fields": {
                "nome_passeggero": "Ahmed Ali",
                "route": "Palermo Tunisi",
                "compagnia": "Grimaldi",
            },
            "semantic_score": 0.71,
            "best_chunk_text": "Ahmed Ali traghetto Palermo Tunisi Grimaldi",
            "raw_ocr_text": "PAX AHMED ALI PALERMO TUNISI",
        },
        {
            "doc_id": "passport_makrem",
            "row": {
                "tipo_documento": "passaporto",
                "file_path": "src/uploads/passport_makrem.pdf",
            },
            "fields": {
                "nome": "Makrem",
                "cognome": "Slimani",
                "numero_passaporto": "XK123456",
                "nazionalita": "Tunisina",
            },
            "semantic_score": 0.72,
            "best_chunk_text": "Passport Makrem Slimani XK123456 Tunisina",
            "raw_ocr_text": "PASSPORT MAKREM SLIMANI XK123456",
        },
        {
            "doc_id": "passport_nesrine",
            "row": {
                "tipo_documento": "passport",
                "file_path": "src/uploads/passport_nesrine.pdf",
            },
            "fields": {
                "nome": "Nesrine",
                "cognome": "Ben Ali",
                "numero_passaporto": "YA987654",
                "nazionalita": "Tunisina",
            },
            "semantic_score": 0.71,
            "best_chunk_text": "Passport Nesrine Ben Ali YA987654 Tunisina",
            "raw_ocr_text": "PASSPORT NESRINE BEN ALI YA987654",
        },
        {
            "doc_id": "contract_ahmed_altea",
            "row": {
                "tipo_documento": "contratto",
                "file_path": "src/uploads/contratto_ahmed_altea.pdf",
            },
            "fields": {
                "contraente": "Ahmed Ali",
                "societa": "Altea S.p.A.",
                "tipo_contratto": "tempo indeterminato",
            },
            "semantic_score": 0.69,
            "best_chunk_text": "Contratto Ahmed Ali Altea tempo indeterminato",
            "raw_ocr_text": "Ahmed Ali contratto Altea",
        },
        {
            "doc_id": "contract_nesrine_delta",
            "row": {
                "tipo_documento": "contratto",
                "file_path": "src/uploads/contratto_nesrine_delta.pdf",
            },
            "fields": {
                "contraente": "Nesrine Ben Ali",
                "societa": "Studio Delta",
                "tipo_contratto": "consulenza",
            },
            "semantic_score": 0.66,
            "best_chunk_text": "Contratto consulenza Nesrine Ben Ali Studio Delta",
            "raw_ocr_text": "Nesrine Ben Ali contratto consulenza",
        },
        {
            "doc_id": "invoice_ahmed",
            "row": {
                "tipo_documento": "fattura",
                "file_path": "src/uploads/fattura_ahmed.pdf",
            },
            "fields": {
                "cliente": "Ahmed Ali",
                "totale": "1250.00",
                "fornitore": "Studio Delta",
            },
            "semantic_score": 0.63,
            "best_chunk_text": "Fattura Ahmed Ali totale 1250 Studio Delta",
            "raw_ocr_text": "Ahmed Ali invoice 1250",
        },
        {
            "doc_id": "invoice_nesrine",
            "row": {
                "tipo_documento": "fattura",
                "file_path": "src/uploads/fattura_nesrine.pdf",
            },
            "fields": {
                "cliente": "Nesrine Ben Ali",
                "totale": "890.00",
                "fornitore": "Travel Group",
            },
            "semantic_score": 0.61,
            "best_chunk_text": "Fattura Nesrine Ben Ali totale 890 Travel Group",
            "raw_ocr_text": "Nesrine Ben Ali invoice 890",
        },
        {
            "doc_id": "generic_internal_doc",
            "row": {
                "tipo_documento": "documento generico",
                "file_path": "src/uploads/admin_doc.pdf",
            },
            "fields": {
                "note": "documento amministrativo interno",
                "cliente": "Mario Rossi",
            },
            "semantic_score": 0.18,
            "best_chunk_text": "Administrative internal generic document",
            "raw_ocr_text": "generic document",
        },
    ]


def get_benchmark_cases() -> list[dict[str, Any]]:
    return [
        {
            "name": "benchmark_ticket_makrem_should_be_top1",
            "query": "biglietto makrem milano tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "expected_top1": "ticket_makrem_milano_tunisi",
        },
        {
            "name": "benchmark_ticket_makrem_route_specific_should_prefer_correct_route",
            "query": "ticket makrem roma parigi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "expected_top1": "ticket_makrem_roma_parigi",
        },
        {
            "name": "benchmark_nesrine_ticket_should_not_be_beaten_by_invoice",
            "query": "ticket nesrine",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "expected_top1": "ticket_nesrine_roma_tunisi",
            "must_not_top1": "invoice_nesrine",
        },
        {
            "name": "benchmark_amna_ferry_should_be_top1",
            "query": "traghetto amna genova tunisi",
            "parsed_query": {"tipo_documento": "biglietto", "persona": "amna", "nome_file": None},
            "expected_top1": "ferry_amna_genova_tunisi",
        },
        {
            "name": "benchmark_ahmed_contract_should_be_top1_for_contract_query",
            "query": "contratto ahmed altea",
            "parsed_query": {"tipo_documento": "contratto", "persona": "ahmed", "nome_file": None},
            "expected_top1": "contract_ahmed_altea",
        },
        {
            "name": "benchmark_ahmed_ferry_should_be_in_top3_for_palermo_tunisi",
            "query": "biglietto ahmed palermo tunisi",
            "parsed_query": {"tipo_documento": "biglietto", "persona": "ahmed", "nome_file": None},
            "expected_in_top3": "ferry_ahmed_palermo_tunisi",
        },
        {
            "name": "benchmark_makrem_passport_should_be_top1",
            "query": "passaporto makrem",
            "parsed_query": {"tipo_documento": "passaporto", "persona": "makrem", "nome_file": None},
            "expected_top1": "passport_makrem",
        },
        {
            "name": "benchmark_nesrine_passport_number_should_be_in_top3",
            "query": "passport nesrine YA987654",
            "parsed_query": {"tipo_documento": "passport", "persona": "nesrine", "nome_file": None},
            "expected_in_top3": "passport_nesrine",
        },
        {
            "name": "benchmark_fatma_ticket_should_be_top1",
            "query": "e ticket fatma parigi tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "fatma", "nome_file": None},
            "expected_top1": "ticket_fatma_parigi_tunisi",
        },
        {
            "name": "benchmark_generic_doc_should_not_be_in_top3",
            "query": "ticket makrem",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "must_not_be_in_top3": "generic_internal_doc",
        },
        {
            "name": "benchmark_same_person_different_doc_types_should_choose_passport",
            "query": "passaporto nesrine",
            "parsed_query": {"tipo_documento": "passaporto", "persona": "nesrine", "nome_file": None},
            "expected_top1": "passport_nesrine",
        },
        {
            "name": "benchmark_same_person_different_doc_types_should_choose_contract",
            "query": "contratto nesrine",
            "parsed_query": {"tipo_documento": "contratto", "persona": "nesrine", "nome_file": None},
            "expected_top1": "contract_nesrine_delta",
        },
        {
            "name": "benchmark_partial_query_should_keep_right_doc_high",
            "query": "makrm tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrm", "nome_file": None},
            "expected_in_top3": "ticket_makrem_milano_tunisi",
        },
        {
            "name": "benchmark_milan_alias_should_keep_correct_ticket_high",
            "query": "ticket makrem milan tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "expected_in_top3": "ticket_makrem_milano_tunisi",
        },
        {
            "name": "benchmark_invoice_query_should_choose_invoice",
            "query": "fattura ahmed",
            "parsed_query": {"tipo_documento": "fattura", "persona": "ahmed", "nome_file": None},
            "expected_top1": "invoice_ahmed",
        },
                {
            "name": "stress_same_person_ticket_vs_passport_short_query",
            "query": "makrem",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "expected_in_top3": "ticket_makrem_milano_tunisi",
            "must_not_top1": "passport_makrem",
        },
        {
            "name": "stress_same_person_passport_vs_ticket_short_query",
            "query": "nesrine",
            "parsed_query": {"tipo_documento": "passaporto", "persona": "nesrine", "nome_file": None},
            "expected_top1": "passport_nesrine",
            "must_not_top1": "ticket_nesrine_roma_tunisi",
        },
        {
            "name": "stress_wrong_type_high_semantic_should_not_win",
            "query": "ticket ahmed",
            "parsed_query": {"tipo_documento": "ticket", "persona": "ahmed", "nome_file": None},
            "expected_in_top3": "ferry_ahmed_palermo_tunisi",
            "must_not_top1": "invoice_ahmed",
        },
        {
            "name": "stress_contract_vs_invoice_same_person",
            "query": "contratto ahmed",
            "parsed_query": {"tipo_documento": "contratto", "persona": "ahmed", "nome_file": None},
            "expected_top1": "contract_ahmed_altea",
            "must_not_top1": "invoice_ahmed",
        },
        {
            "name": "stress_generic_field_should_not_beat_strong_person_field",
            "query": "nesrine ben ali",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine ben ali", "nome_file": None},
            "expected_in_top3": "ticket_nesrine_roma_tunisi",
            "must_not_top1": "generic_internal_doc",
        },
        {
            "name": "stress_raw_ocr_should_help_but_not_override_wrong_type",
            "query": "passaporto makrem",
            "parsed_query": {"tipo_documento": "passaporto", "persona": "makrem", "nome_file": None},
            "expected_top1": "passport_makrem",
            "must_not_top1": "ticket_makrem_milano_tunisi",
        },
        {
            "name": "stress_route_signal_should_disambiguate_same_person_docs",
            "query": "makrem tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "expected_top1": "ticket_makrem_milano_tunisi",
            "must_not_top1": "ticket_makrem_roma_parigi",
        },
        {
            "name": "stress_route_signal_other_direction",
            "query": "makrem parigi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrem", "nome_file": None},
            "expected_top1": "ticket_makrem_roma_parigi",
            "must_not_top1": "ticket_makrem_milano_tunisi",
        },
        {
            "name": "stress_invoice_query_should_penalize_contract_same_person",
            "query": "fattura nesrine",
            "parsed_query": {"tipo_documento": "fattura", "persona": "nesrine", "nome_file": None},
            "expected_top1": "invoice_nesrine",
            "must_not_top1": "contract_nesrine_delta",
        },
        {
            "name": "stress_ticket_query_should_penalize_contract_same_person",
            "query": "biglietto nesrine tunisi",
            "parsed_query": {"tipo_documento": "ticket", "persona": "nesrine", "nome_file": None},
            "expected_top1": "ticket_nesrine_roma_tunisi",
            "must_not_top1": "contract_nesrine_delta",
        },
        {
            "name": "stress_short_person_query_should_not_promote_generic_doc",
            "query": "ahmed",
            "parsed_query": {"tipo_documento": "contratto", "persona": "ahmed", "nome_file": None},
            "expected_in_top3": "contract_ahmed_altea",
            "must_not_be_in_top3": "generic_internal_doc",
        },
        {
            "name": "stress_alias_and_fuzzy_should_keep_correct_doc_high",
            "query": "ticket makrm milan",
            "parsed_query": {"tipo_documento": "ticket", "persona": "makrm", "nome_file": None},
            "expected_in_top3": "ticket_makrem_milano_tunisi",
            "must_not_top1": "ticket_makrem_roma_parigi",
        },
    ]


def evaluate_weights(weights: dict, documents: list[dict], cases: list[dict]) -> tuple[int, list[dict]]:
    total_score = 0
    details = []

    for case in cases:
        ranked = rank_documents(
            query=case["query"],
            parsed_query=case["parsed_query"],
            documents=documents,
            weights=weights,
        )

        top_ids = [r["doc_id"] for r in ranked[:3]]
        top1 = ranked[0]["doc_id"] if ranked else None

        case_score = 0
        notes = []

        if "expected_top1" in case:
            if top1 == case["expected_top1"]:
                case_score += 3
                notes.append("expected_top1 OK (+3)")
            else:
                notes.append(f"expected_top1 FAIL (got={top1})")

        if "expected_in_top3" in case:
            if case["expected_in_top3"] in top_ids:
                case_score += 1
                notes.append("expected_in_top3 OK (+1)")
            else:
                notes.append(f"expected_in_top3 FAIL (top3={top_ids})")

        if "must_not_top1" in case:
            if top1 == case["must_not_top1"]:
                case_score -= 2
                notes.append("must_not_top1 FAIL (-2)")
            else:
                notes.append("must_not_top1 OK")

        if "must_not_be_in_top3" in case:
            if case["must_not_be_in_top3"] in top_ids:
                case_score -= 1
                notes.append("must_not_be_in_top3 FAIL (-1)")
            else:
                notes.append("must_not_be_in_top3 OK")

        total_score += case_score
        details.append(
            {
                "name": case["name"],
                "score": case_score,
                "top1": top1,
                "top3": top_ids,
                "notes": notes,
            }
        )

    return total_score, details


def main():
    documents = get_benchmark_documents()
    cases = get_benchmark_cases()


    baseline_score, baseline_details = evaluate_weights(
        SEARCH_SCORE_WEIGHTS,
        documents,
        cases,
    )

    print("\n=== CONFIGURAZIONE ATTUALE ===\n")
    print(f"Punteggio totale baseline: {baseline_score}\n")
    
    # Prova solo pochi pesi chiave all'inizio
    search_space = {
        "person_medium_bonus": [0.20, 0.30, 0.40],
        "person_chunk_bonus": [0.15, 0.22, 0.30],
        "full_person_phrase_strong_bonus": [0.10, 0.20, 0.30],
        "text_match_bonus": [0.08, 0.12, 0.18],
    }

    keys = list(search_space.keys())
    combinations = list(product(*(search_space[k] for k in keys)))

    print(f"Combinazioni da provare: {len(combinations)}")

    results = []

    for combo in combinations:
        weights = deepcopy(SEARCH_SCORE_WEIGHTS)

        for key, value in zip(keys, combo):
            weights[key] = value

        total_score, details = evaluate_weights(weights, documents, cases)

        results.append(
            {
                "total_score": total_score,
                "weights": deepcopy(weights),
                "details": details,
            }
        )

    results.sort(key=lambda x: x["total_score"], reverse=True)

    print("\n=== TOP 10 CONFIGURAZIONI ===\n")
    for i, item in enumerate(results[:10], start=1):
        w = item["weights"]
        print(
            f"{i}. total_score={item['total_score']} | "
            f"semantic_weight={w['semantic_weight']} | "
            f"person_strong_bonus={w['person_strong_bonus']} | "
            f"raw_ocr_bonus={w['raw_ocr_bonus']} | "
            f"tipo_wrong_penalty={w['tipo_wrong_penalty']}"
        )

    best = results[0]
    
    delta_vs_baseline = best["total_score"] - baseline_score
    print(f"Differenza rispetto alla baseline: {delta_vs_baseline:+}")
    
    print("\n=== MIGLIORE CONFIGURAZIONE ===\n")
    for k in keys:
        print(f"{k}: {best['weights'][k]}")

    print(f"\nPunteggio totale: {best['total_score']}\n")

    print("=== DETTAGLIO CASI MIGLIORE CONFIGURAZIONE ===\n")
    for d in best["details"]:
        print(f"- {d['name']}")
        print(f"  score caso: {d['score']}")
        print(f"  top1: {d['top1']}")
        print(f"  top3: {d['top3']}")
        print(f"  note: {', '.join(d['notes'])}")
        print()


if __name__ == "__main__":
    main()