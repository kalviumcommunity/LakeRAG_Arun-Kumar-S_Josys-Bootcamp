import os
import torch
import pandas as pd
from sentence_transformers import SentenceTransformer
from deltalake import DeltaTable
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

GOLD_PATH = "s3://lakerag-arun-bootcamp/gold"
BUCKET = "lakerag-arun-bootcamp"
EMB_PREFIX = "gold-embeddings"

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
AWS_KEY    = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")

storage_options = {
    "AWS_REGION": AWS_REGION,
    "AWS_ACCESS_KEY_ID": AWS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET,
}

model_name = "BAAI/bge-large-en"
device = "cuda" if torch.cuda.is_available() else "cpu"
model = SentenceTransformer(model_name).to(device)

print(f"📥 Reading GOLD Delta from S3 → {GOLD_PATH}")
gold_df = DeltaTable(GOLD_PATH, storage_options=storage_options).to_pyarrow_table().to_pandas()

if gold_df.empty:
    print("❌ No gold rows found — abort")
    exit()

texts = gold_df["chunk_text"].tolist()
print(f"🧠 Generating embeddings for {len(texts)} chunks using {model_name}")

embeddings = model.encode(
    texts,
    batch_size=32,
    convert_to_numpy=True,
    normalize_embeddings=True
)

gold_df["embedding"] = embeddings.tolist()
gold_df["embedding_timestamp"] = datetime.now(timezone.utc)

table = pa.Table.from_pandas(gold_df)

fs = pa.fs.S3FileSystem(region=AWS_REGION, access_key=AWS_KEY, secret_key=AWS_SECRET)
timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
key = f"{EMB_PREFIX}/embeddings_{timestamp}.parquet"
output_path = f"{BUCKET}/{key}"

print(f"💾 Writing embeddings to S3 → s3://{output_path}")
pq.write_table(table, output_path, filesystem=fs)

print("🚀 DONE — Embeddings generated & saved successfully")
print("📌", f"s3://{output_path}")
