#!/usr/bin/env python3
"""
Apply wide CSV from 8_bulk_generate_bills.py to Neo4j :Bill nodes.

Sets:
  llm_bill_summary, llm_bill_why_matters, llm_related_bills_json,
  llm_generation_errors, llm_generated_at

Example:
  cd backend && poetry run python ../graph/scripts/9_apply_llm_outputs_to_neo4j.py \\
    --input-csv ../graph/output/bulk_llm_generation_abc123.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BACKEND_ROOT = _REPO_ROOT / "backend"
sys.path.insert(0, str(_BACKEND_ROOT))

csv.field_size_limit(sys.maxsize)

from dotenv import load_dotenv  # noqa: E402

load_dotenv(_BACKEND_ROOT / ".env")

from src.neo4j_graph.neo4j_client import Neo4j  # noqa: E402

APPLY_CYPHER = """
UNWIND $rows AS row
MATCH (b:Bill {bill_pk: row.bill_pk})
SET b.llm_bill_summary = row.llm_bill_summary,
    b.llm_bill_why_matters = row.llm_bill_why_matters,
    b.llm_related_bills_json = row.llm_related_bills_json,
    b.llm_generation_errors = row.errors,
    b.llm_generated_at = datetime()
RETURN count(b) AS updated
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply bulk LLM CSV to Neo4j bills.")
    parser.add_argument(
        "--input-csv",
        type=Path,
        required=True,
        help="Wide CSV from 8_bulk_generate_bills.py",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Rows per UNWIND batch.",
    )
    args = parser.parse_args()
    path = args.input_csv.resolve()
    if not path.is_file():
        raise SystemExit(f"Not found: {path}")

    rows: list[dict[str, str]] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {
            "bill_pk",
            "llm_bill_summary",
            "llm_bill_why_matters",
            "llm_related_bills_json",
            "errors",
        }
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise SystemExit(
                f"CSV must have columns {sorted(required)}; got {reader.fieldnames}"
            )
        for raw in reader:
            rows.append(
                {
                    "bill_pk": (raw.get("bill_pk") or "").strip(),
                    "llm_bill_summary": raw.get("llm_bill_summary") or "",
                    "llm_bill_why_matters": raw.get("llm_bill_why_matters") or "",
                    "llm_related_bills_json": raw.get("llm_related_bills_json")
                    or "[]",
                    "errors": raw.get("errors") or "",
                }
            )

    rows = [r for r in rows if r["bill_pk"]]
    if not rows:
        raise SystemExit("No data rows with bill_pk")

    db = Neo4j()
    total = 0
    try:
        batch_size = max(1, args.batch_size)
        for i in range(0, len(rows), batch_size):
            chunk = rows[i : i + batch_size]
            out = db.run_batch(APPLY_CYPHER, chunk, key="rows")
            if out:
                total += int(out[0].get("updated", 0))
            print(f"[BATCH] {i + len(chunk)}/{len(rows)} rows sent")
    finally:
        db.close()

    print(f"[DONE] Batches applied; last reported updated count sum: {total}")


if __name__ == "__main__":
    main()
