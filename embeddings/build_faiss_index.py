import os
import faiss
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

BUCKET = "lakerag-arun-bootcamp"
EMB_PREFIX = "gold-embeddings/"
INDEX_PREFIX = "vector-index/"

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
AWS_KEY    = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")

fs = pa.fs.S3FileSystem(
    region=AWS_REGION,
    access_key=AWS_KEY,
    secret_key=AWS_SECRET,
)

print(f"📥 Scanning S3 for parquet files → s3://{BUCKET}/{EMB_PREFIX}")

selector = pa.fs.FileSelector(f"{BUCKET}/{EMB_PREFIX}", recursive=False)
files = [f for f in fs.get_file_info(selector) if f.is_file]

if not files:
    print("❌ No parquet embedding files found — abort")
    exit()

dfs = []
for f in files:
    key = f.path
    print(f"🔍 Reading: s3://{key}")
    table = pq.read_table(key, filesystem=fs)
    dfs.append(table.to_pandas())

df = pd.concat(dfs, ignore_index=True)
print(f"✔ Loaded {len(df)} embedding rows from parquet")

# Build FAISS index
emb = np.stack(df["embedding"].values).astype("float32")
dimension = emb.shape[1]
index = faiss.IndexFlatIP(dimension)
index.add(emb)
print(f"🎯 FAISS index size: {index.ntotal}")

# Save locally
os.makedirs("faiss_build", exist_ok=True)
faiss.write_index(index, "faiss_build/index.faiss")
df.drop(columns=["embedding"]).to_parquet("faiss_build/metadata.parquet", index=False)

# Upload function
def upload_file(local_path: str, s3_key: str):
    with open(local_path, "rb") as local_f:
        data = local_f.read()
    with fs.open_output_stream(s3_key) as s3_f:
        s3_f.write(data)

timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
index_key = f"{BUCKET}/{INDEX_PREFIX}index_{timestamp}.faiss"
meta_key  = f"{BUCKET}/{INDEX_PREFIX}metadata_{timestamp}.parquet"

print("🚀 Uploading FAISS artifacts to S3...")
upload_file("faiss_build/index.faiss", index_key)
upload_file("faiss_build/metadata.parquet", meta_key)

print("🎉 DONE — FAISS index generated & uploaded to S3")
print(f"📌 index:    s3://{index_key}")
print(f"📌 metadata: s3://{meta_key}")
