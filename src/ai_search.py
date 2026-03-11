from sentence_transformers import SentenceTransformer, util
import numpy as np

model = SentenceTransformer("all-MiniLM-L6-v2")

# memoria embeddings documenti
document_embeddings = {}
document_texts = {}

def index_document(doc_id, text):
    emb = model.encode(text, convert_to_tensor=True)
    document_embeddings[doc_id] = emb
    document_texts[doc_id] = text


def search_documents(query, top_k=10):
    if not document_embeddings:
        return []

    query_emb = model.encode(query, convert_to_tensor=True)

    scores = []

    for doc_id, emb in document_embeddings.items():
        score = util.cos_sim(query_emb, emb).item()
        scores.append((doc_id, score))

    scores.sort(key=lambda x: x[1], reverse=True)

    return [doc_id for doc_id, _ in scores[:top_k]]