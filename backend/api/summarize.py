from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from backend.services.search_service import semantic_search
from google import genai
import os
import pandas as pd

router = APIRouter(prefix="/summarize", tags=["Summarization"])


class SummarizeRequest(BaseModel):
    query: str | None = None
    doc_id: str | None = None
    k: int = 5


# Load metadata once for full-document mode
META_PATH = "local_data/faiss/metadata.parquet"
metadata_df = pd.read_parquet(META_PATH)


def get_gemini_client():
    key = os.getenv("GEMINI_API_KEY")
    if not key:
        raise HTTPException(status_code=500, detail="GEMINI_API_KEY is missing — configure .env")
    return genai.Client(api_key=key)


@router.post("/")
def summarize(req: SummarizeRequest):
    if not req.query and not req.doc_id:
        raise HTTPException(status_code=400, detail="Provide either 'query' or 'doc_id'")

    # 🟢 MODE 1 — FULL DOCUMENT SUMMARIZATION (no threshold, no semantic filtering)
    if req.doc_id:
        doc_chunks = metadata_df[metadata_df["doc_id"] == req.doc_id]
        if doc_chunks.empty:
            raise HTTPException(status_code=404, detail="Document not found")

        doc_chunks = doc_chunks.sort_values("chunk_index")  # preserve correct order
        hits = [
            {
                "rank": i + 1,
                "score": None,
                "doc_id": row.doc_id,
                "chunk_index": int(row.chunk_index),
                "chunk_text": row.chunk_text,
            }
            for i, row in doc_chunks.iterrows()
        ]

    # 🔵 MODE 2 — QUERY-BASED RETRIEVAL (semantic search + threshold)
    else:
        hits = semantic_search(req.query, req.k)
        if len(hits) == 1 and "message" in hits[0]:
            return {"summary": None, "message": "No relevant context found"}
        req.doc_id = hits[0]["doc_id"]  # infer doc_id from top result

    # 🔹 Build context
    context = "\n".join(h["chunk_text"] for h in hits)

    # Build prompt — different for query vs doc_id
    if req.query:
        prompt = f"""
You are an expert information extractor.

Your task is to answer ONLY based on details from the context that are relevant to the user's query.
If the context contains information NOT related to the query, ignore it completely.

Do NOT add assumptions, do NOT infer additional facts, and do NOT mention unrelated topics.
Be concise.

### User Query:
{req.query}

### Context:
{context}

### Answer (only about the query):
"""
    else:
        prompt = f"""
You are an expert summarizer.

Write a concise and well-structured summary of the entire document using the provided context.
Avoid filler text and repetition. Focus on the major points across all chunks.

### Context:
{context}

### Summary:
"""

    client = get_gemini_client()

    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
        summary = response.text
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gemini API failed: {e}")

    return {
        "query": req.query,
        "doc_id": req.doc_id,
        "chunks_used": len(hits),
        "summary": summary,
    }