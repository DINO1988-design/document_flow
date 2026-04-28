# test_search.py
from ai_search import load_index, add_document, search
# 1. Carica l'indice (o inizializza se non esiste)
load_index()

# 2. Aggiungi documenti di prova (solo se l'indice è vuoto)
if not search("test"):  # se l'indice è vuoto
    add_document(1, "Biglietti per Makrem disponibili")
    add_document(2, "Informazioni sui voli per Lisbona")
    add_document(3, "Prenotazione alberghi a Porto")

# 3. Esegui la ricerca
query = "biglietti di makrem"
results = search(query)

print(f"Query: {query}")
print(f"Risultati trovati: {len(results)}")
for r in results:
    print(f"- doc_id: {r['doc_id']}, text: {r['text']}")