import pandas as pd
import faiss
from sentence_transformers import SentenceTransformer
import numpy as np
import os

FAISS_PATH = "local_data/faiss/index.faiss"
META_PATH  = "local_data/faiss/metadata.parquet"

ABSOLUTE_THRESHOLD = 0.80       # if best < this → Out-Of-Context
RELATIVE_FACTOR    = 0.90       # keep results >= 90% of best score

print("🔍 Initializing semantic search service...")

if not os.path.exists(FAISS_PATH) or not os.path.exists(META_PATH):
    raise FileNotFoundError(
        "\n❌ FAISS index/metadata missing.\n"
        "Run `build_faiss_index.py` and copy outputs to local_data/faiss/"
    )

# Load components into RAM once on app startup
index = faiss.read_index(FAISS_PATH)
metadata = pd.read_parquet(META_PATH)
model = SentenceTransformer("BAAI/bge-large-en")

print(f"🚀 Search service ready. FAISS size: {index.ntotal} | metadata rows: {len(metadata)}")


def semantic_search(query: str, k: int = 5):
    """
    Runs semantic search with 3-layer filtering:
      1️⃣ Find FAISS top-k results
      2️⃣ If BEST score < ABSOLUTE_THRESHOLD → return OOC
      3️⃣ Filter results with score >= best * RELATIVE_FACTOR
    """

    # Encode → (1 x dim) float32
    vec = model.encode(query, normalize_embeddings=True)
    vec = np.expand_dims(vec, axis=0).astype("float32")

    scores, ids = index.search(vec, k)
    scores = scores[0]
    ids = ids[0]

    best = float(scores[0])
    print(f"\n🔎 BEST SCORE = {best:.4f}")

    # 🧱 Out-Of-Context check
    if best < ABSOLUTE_THRESHOLD:
        print("❌ OOC triggered: below absolute threshold")
        return [{"message": "Out of context — no relevant match found"}]

    # Relative cutoff (keeps only strong matches)
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

    # If nothing passes the filter → OOC
    if not results:
        print("❌ All candidates filtered out → OOC")
        return [{"message": "Out of context — no relevant match found"}]

    return results