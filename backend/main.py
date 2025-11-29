from fastapi import FastAPI
from backend.api.search import router as search_router

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

app.include_router(search_router)
