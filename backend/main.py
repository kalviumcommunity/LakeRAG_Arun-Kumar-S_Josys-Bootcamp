import os
from fastapi import FastAPI
from backend.api.search import router as search_router
from backend.api.summarize import router as summarize_router

from dotenv import load_dotenv

# Load .env globally for backend
load_dotenv()

app = FastAPI(title="LakeRAG API")

app.include_router(search_router)
app.include_router(summarize_router)

@app.get("/health")
def health():
    return {"status": "ok"}