"""
Fetch bill text from the LegiScan API using document IDs from a CSV.
"""

import base64
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

API_KEY = os.getenv("LEGISCAN_API_KEY")
API_URL = "https://api.legiscan.com/"
SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "bill-text"

MIME_EXT = {
    "application/pdf": ".pdf",
    "text/html": ".html",
    "application/msword": ".doc",
}


def fetch_bill_text(doc_id: int) -> dict | None:
    resp = requests.get(API_URL, params={"key": API_KEY, "op": "getBillText", "id": doc_id})
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "OK":
        print(f"  API error for doc {doc_id}: {data}")
        return None

    return data["text"]


def main():
    csv_path = SCRIPT_DIR / (sys.argv[1] if len(sys.argv) > 1 else "example.csv")
    df = pd.read_csv(csv_path)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not API_KEY:
        print("LEGISCAN_API_KEY not set in .env")
        sys.exit(1)

    print(f"Loaded {len(df)} rows from {csv_path.name}\n")

    success = 0
    failed = 0

    for _, row in df.iterrows():
        doc_id = int(row["document_id"])
        name = f"{row['state']}_{row['bill_id']}_{row['session_id']}_{row['bill_number']}_{doc_id}"

        print(f"  [{name}] fetching doc {doc_id}...")

        try:
            text = fetch_bill_text(doc_id)
            if not text:
                failed += 1
                continue

            ext = MIME_EXT.get(text.get("mime", ""), ".bin")
            dest = OUTPUT_DIR / f"{name}{ext}"
            dest.write_bytes(base64.b64decode(text["doc"]))

            print(f"  [{name}] saved {dest.name} ({text['text_size']} bytes)")
            success += 1

        except Exception as e:
            print(f"  [{name}] ERROR: {e}")
            failed += 1

        time.sleep(1)

    print(f"\nDone — {success} saved, {failed} failed")


if __name__ == "__main__":
    main()
