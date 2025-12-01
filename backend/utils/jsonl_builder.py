import os
import json
import pandas as pd

GOLD_PATH = "local_data/gold/part-00000-820b63be-557c-460e-b138-06992f62652a-c000.snappy.parquet"
OUTPUT_DIR = "local_data/jsonl"
OUTPUT_JSONL = f"{OUTPUT_DIR}/fine_tune.jsonl"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# --------- constants ---------
MAX_OUTPUT_LENGTH = 600  # previously magic number

# --------- helper: classify chunk type ---------
def get_instruction(chunk_text):
    lower = chunk_text.lower()

    if "experience" in lower or "intern" in lower or "worked" in lower:
        return "Summarize the professional work experience in this text:"
    elif "skills" in lower or "technical" in lower or "programming" in lower:
        return "Extract all technical skills mentioned in this text:"
    elif "education" in lower or "university" in lower or "b.tech" in lower:
        return "Summarize the academic background contained in the text:"
    else:
        return "Provide a clear and concise summary of the following text:"

# --------- helper: naive response generator (baseline label) ---------
def generate_output(chunk_text):
    return chunk_text.strip()[:MAX_OUTPUT_LENGTH]

# --------- main dataset builder ---------
def build_dataset():
    df = pd.read_parquet(GOLD_PATH)
    total = 0

    with open(OUTPUT_JSONL, "w") as f:
        for row in df.itertuples():  # ⚡ faster iteration
            text = row.chunk_text
            record = {
                "instruction": get_instruction(text),
                "input": "",
                "output": generate_output(text),
                "metadata": {
                    "doc_id": row.doc_id,
                    "chunk_id": row.chunk_id,
                    "chunk_index": int(row.chunk_index)
                }
            }
            f.write(json.dumps(record) + "\n")
            total += 1

    print(f"✅ JSONL dataset built: {OUTPUT_JSONL}")
    print(f"📌 Total Records Written: {total}")


if __name__ == "__main__":
    build_dataset()
