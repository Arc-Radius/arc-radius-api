import csv
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from src.neo4j_client import Neo4j

CSV_PATH = Path(__file__).resolve().parents[1] / ".." / "datasources" / "eda" / "matched_lgbtq_bills.csv"
BATCH_SIZE = 500

BILL_FIELDS = [
    "state", "session_id", "bill_id", "bill_number",
    "status", "status_desc", "status_date",
    "title", "description",
    "last_action_date", "last_action",
    "url", "state_link",
    "label", "label_source",
    "year", "state_lean", "bill_dominant_party",
    "passed", "failed", "vetoed",
]

INGEST_CYPHER = """
UNWIND $rows AS row
MERGE (b:Bill {bill_pk: row.bill_pk})
SET b.state = row.state,
    b.session_id = row.session_id,
    b.bill_id = row.bill_id,
    b.bill_number = row.bill_number,
    b.status = row.status,
    b.status_desc = row.status_desc,
    b.status_date = row.status_date,
    b.title = row.title,
    b.description = row.description,
    b.last_action_date = row.last_action_date,
    b.last_action = row.last_action,
    b.url = row.url,
    b.state_link = row.state_link,
    b.label = row.label,
    b.label_source = row.label_source,
    b.year = row.year,
    b.state_lean = row.state_lean,
    b.bill_dominant_party = row.bill_dominant_party,
    b.passed = row.passed,
    b.failed = row.failed,
    b.vetoed = row.vetoed
WITH b, row
WHERE row.document_id IS NOT NULL AND row.document_id <> ''
MERGE (d:Document {document_id: row.document_id})
SET d.url = row.document_url,
    d.document_type = row.document_type
MERGE (b)-[:HAS_DOCUMENT]->(d)
"""


def build_row(raw: dict) -> dict:
    row = {k: raw.get(k, "") for k in BILL_FIELDS}
    row["bill_pk"] = f"{raw['state']}:{raw['session_id']}:{raw['bill_id']}"
    row["document_id"] = raw.get("document_id", "")
    row["document_url"] = raw.get("document_url", "")
    row["document_type"] = raw.get("document_type", "")
    return row


def main(path: str | None = None):
    csv_path = Path(path) if path else CSV_PATH
    db = Neo4j()

    batch = []
    total = 0

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            batch.append(build_row(raw))
            if len(batch) >= BATCH_SIZE:
                db.run_batch(INGEST_CYPHER, batch)
                total += len(batch)
                print(f"  ingested {total} rows...")
                batch = []

    if batch:
        db.run_batch(INGEST_CYPHER, batch)
        total += len(batch)

    db.close()
    print(f"[OK] metadata ingested — {total} rows")


if __name__ == "__main__":
    main()
