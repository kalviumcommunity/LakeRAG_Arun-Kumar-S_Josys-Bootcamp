import os
import json
import pandas as pd

GOLD_PATH = "local_data/gold/part-00000-820b63be-557c-460e-b138-06992f62652a-c000.snappy.parquet"
OUTPUT_JSONL = "local_data/jsonl/fine_tune.jsonl"

os.makedirs("local_data/jsonl", exist_ok=True)

# --------- helper: classify chunk type ---------
def get_instruction(chunk_text):
    lower = chunk_text.lower()

    if "experience" in lower or "intern" in lower or "worked" in lower:
        return "Summarize the professional work experience in this text:"
    if "skills" in lower or "technical" in lower or "programming" in lower:
        return "Extract all technical skills mentioned in this text:"
    if "education" in lower or "university" in lower or "b.tech" in lower:
        return "Summarize the academic background contained in the text:"
    
    # fallback — universal
    return "Provide a clear and concise summary of the following text:"

# --------- helper: naive response generator (deterministic) ---------
def generate_output(chunk_text):
    # Simple baseline = return short cleaned summary
    # (LLM can replace this later)
    return chunk_text.strip()[:600]   # trimmed so training is clean

# --------- main builder ---------
def build_dataset():
    df = pd.read_parquet(GOLD_PATH)
    records = []

    for _, row in df.iterrows():
        text = row["chunk_text"]
        instruction = get_instruction(text)
        output = generate_output(text)

        record = {
            "instruction": instruction,
            "input": "",
            "output": output,
            "metadata": {
                "doc_id": row["doc_id"],
                "chunk_id": row["chunk_id"],
                "chunk_index": int(row["chunk_index"])
            }
        }
        records.append(record)

    with open(OUTPUT_JSONL, "w") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")

    print(f"✅ JSONL dataset built: {OUTPUT_JSONL}")
    print(f"📌 Total Records: {len(records)}")


if __name__ == "__main__":
    build_dataset()
