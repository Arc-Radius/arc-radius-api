"""
Fetch bill text from the LegiScan API using document IDs from a CSV.
"""

import base64
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

API_KEY = os.getenv("LEGISCAN_API_KEY")
API_URL = "https://api.legiscan.com/"
SCRIPT_DIR = Path(__file__).parent
OUTPUT_DIR = SCRIPT_DIR / "bill-text"
LOG_FILE = SCRIPT_DIR / "bill_text_retrieval.txt"
DEFAULT_CSV = SCRIPT_DIR.parent / "eda" / "matched_lgbtq_bills.csv"

MIME_EXT = {
    "application/pdf": ".pdf",
    "text/html": ".html",
    "application/msword": ".doc",
}


def log(msg: str, fh):
    print(msg)
    fh.write(msg + "\n")
    fh.flush()


def fetch_bill_text(doc_id: int) -> dict | None:
    resp = requests.get(API_URL, params={"key": API_KEY, "op": "getBillText", "id": doc_id})
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "OK":
        return None

    return data["text"]


def main():
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CSV
    df = pd.read_csv(csv_path)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not API_KEY:
        print("LEGISCAN_API_KEY not set in .env")
        sys.exit(1)

    with open(LOG_FILE, "a") as fh:
        log(f"\n{'='*60}", fh)
        log(f"Run started: {datetime.now().isoformat()}", fh)
        log(f"Loaded {len(df)} rows from {csv_path.name}\n", fh)

        success = 0
        failed = 0

        for _, row in df.iterrows():
            doc_id = int(row["document_id"])
            name = f"{row['state']}_{row['bill_id']}_{row['session_id']}_{row['bill_number']}_{doc_id}"

            try:
                text = fetch_bill_text(doc_id)
                if not text:
                    log(f"FAIL  {name}  (API returned non-OK)", fh)
                    failed += 1
                    continue

                ext = MIME_EXT.get(text.get("mime", ""), ".bin")
                dest = OUTPUT_DIR / f"{name}{ext}"
                dest.write_bytes(base64.b64decode(text["doc"]))

                log(f"OK    {name}{ext}  ({text['text_size']} bytes)", fh)
                success += 1

            except Exception as e:
                log(f"ERROR {name}  ({e})", fh)
                failed += 1

            time.sleep(1)

        log(f"\nDone — {success} saved, {failed} failed", fh)


if __name__ == "__main__":
    main()
