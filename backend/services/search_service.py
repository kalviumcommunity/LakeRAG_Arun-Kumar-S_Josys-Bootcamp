import pandas as pd
import faiss
from sentence_transformers import SentenceTransformer
import numpy as np
import os
import re

FAISS_PATH = "/app/local_data/faiss/index.faiss"
META_PATH  = "/app/local_data/faiss/metadata.parquet"

ABSOLUTE_THRESHOLD = 0.65       # if best < this → Out-Of-Context
RELATIVE_FACTOR    = 0.85       # keep results >= 85% of best score

print("🔍 Initializing semantic search service...")

if not os.path.exists(FAISS_PATH) or not os.path.exists(META_PATH):
    raise FileNotFoundError(
        "\n❌ FAISS index/metadata missing.\n"
        "Run `build_faiss_index.py` and copy outputs to local_data/faiss/"
    )

index = faiss.read_index(FAISS_PATH)
metadata = pd.read_parquet(META_PATH)
model = SentenceTransformer("BAAI/bge-large-en-v1.5")

print(f"🚀 Search service ready. FAISS size: {index.ntotal} | metadata rows: {len(metadata)}")


# ---------------------------------------------------------------
# 🔥 Rewrite long questions → compact search-friendly expressions
# ---------------------------------------------------------------
def rewrite_query(q: str) -> str:
    q = q.lower()
    # remove only stop words, keep company names and important terms
    q = re.sub(
        r"\b(tell me|about|explain|what|were|can you|give|details|information|please|describe|do you know)\b",
        "",
        q
    )
    q = re.sub(r"[^a-z0-9 ]", " ", q)
    q = re.sub(r"\s+", " ", q).strip()
    return q if q else "default"  # fallback


# ---------------------------------------------------------------
# 🔍 Semantic search
# ---------------------------------------------------------------
def semantic_search(query: str, k: int = 5, doc_id: str | None = None):
    """
    If doc_id passed → bypass OOC and return ALL chunks in that doc.
    Otherwise → keyword rewrite + score filtering.
    """

    # ⭐ If doc_id provided → return all chunks for that doc (no threshold)
    if doc_id:
        doc_rows = metadata[metadata["doc_id"] == doc_id]
        if doc_rows.empty:
            return [{"message": "Invalid doc_id — no document found"}]

        return [
            {
                "rank": i + 1,
                "score": 1.0,   # optional since doc_id match is absolute
                "doc_id": row.doc_id,
                "chunk_index": int(row.chunk_index),
                "chunk_text": row.chunk_text
            }
            for i, row in doc_rows.sort_values("chunk_index").reset_index(drop=True).itertuples()
        ]

    # ⭐ Rewrite natural-language questions → compact query
    clean_query = rewrite_query(query)
    print(f"💡 Rewritten Query for FAISS: '{clean_query}'")

    # Convert to FAISS embedding
    vec = model.encode(clean_query, normalize_embeddings=True)
    vec = np.expand_dims(vec, axis=0).astype("float32")

    scores, ids = index.search(vec, k)
    scores = scores[0]
    ids = ids[0]

    best = float(scores[0])
    print(f"🔎 BEST SCORE = {best:.4f}")

    # OOC check
    if best < ABSOLUTE_THRESHOLD:
        print("❌ OOC triggered: below absolute threshold")
        return [{"message": "Out of context — no relevant match found"}]

    cutoff = best * RELATIVE_FACTOR
    results = []
    rank = 1

    for score, idx in zip(scores, ids):
        score = float(score)
        if score < cutoff:
            continue

        row = metadata.iloc[idx]
        results.append({
            "rank": rank,
            "score": round(score, 4),
            "doc_id": row["doc_id"],
            "chunk_index": int(row["chunk_index"]),
            "chunk_text": row["chunk_text"],
        })
        rank += 1

    if not results:
        print("❌ All candidates filtered out → OOC")
        return [{"message": "Out of context — no relevant match found"}]

    return results
