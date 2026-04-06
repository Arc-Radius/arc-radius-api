"""
Apply wide CSV from 8_bulk_generate_bills.py to Neo4j :Bill nodes.

Sets:
  llm_bill_summary, llm_bill_why_matters, llm_related_bills_json,
  llm_generation_errors, llm_generated_at
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BACKEND_ROOT = _REPO_ROOT / "backend"
sys.path.insert(0, str(_BACKEND_ROOT))

csv.field_size_limit(sys.maxsize)

from dotenv import load_dotenv  # noqa: E402

load_dotenv(_BACKEND_ROOT / ".env")

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

_BULK_CSV_NUMERIC = re.compile(r"^bulk_llm_generation_(\d+)\.csv$")


def _sort_bulk_csv_paths(paths: list[Path]) -> list[Path]:
    """Numeric order for bulk_llm_generation_<n>.csv; other names sort after by name."""

    def key(p: Path) -> tuple[int, int | str]:
        m = _BULK_CSV_NUMERIC.match(p.name)
        return (0, int(m.group(1))) if m else (1, p.name)

    return sorted(paths, key=key)


def _resolve_input_paths(args: argparse.Namespace) -> list[Path]:
    if args.input_dir is not None:
        d = args.input_dir.resolve()
        if not d.is_dir():
            raise SystemExit(f"Not a directory: {d}")
        paths = [p for p in d.glob("bulk_llm_generation_*.csv") if p.is_file()]
        paths = _sort_bulk_csv_paths(paths)
        if not paths:
            raise SystemExit(f"No bulk_llm_generation_*.csv in {d}")
        return paths
    # Mutually exclusive with --input-dir; nargs="+" guarantees a non-empty list.
    return [p.resolve() for p in args.input_csv]


def _load_rows(paths: list[Path]) -> list[dict[str, str]]:
    required = {
        "bill_pk",
        "llm_bill_summary",
        "llm_bill_why_matters",
        "llm_related_bills_json",
        "errors",
    }
    # Last wins if the same bill_pk appears in multiple files (overlapping pages).
    by_pk: dict[str, dict[str, str]] = {}
    for path in paths:
        if not path.is_file():
            raise SystemExit(f"Not found: {path}")
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
                raise SystemExit(
                    f"{path.name}: CSV must have columns {sorted(required)}; "
                    f"got {reader.fieldnames}"
                )
            for raw in reader:
                pk = (raw.get("bill_pk") or "").strip()
                if not pk:
                    continue
                by_pk[pk] = {
                    "bill_pk": pk,
                    "llm_bill_summary": raw.get("llm_bill_summary") or "",
                    "llm_bill_why_matters": raw.get("llm_bill_why_matters") or "",
                    "llm_related_bills_json": raw.get("llm_related_bills_json")
                    or "[]",
                    "errors": raw.get("errors") or "",
                }
    return sorted(by_pk.values(), key=lambda r: r["bill_pk"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply bulk LLM CSV to Neo4j bills.")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument(
        "--input-csv",
        type=Path,
        nargs="+",
        metavar="PATH",
        help="One or more wide CSVs from 8_bulk_generate_bills.py (same columns as single run).",
    )
    src.add_argument(
        "--input-dir",
        type=Path,
        metavar="DIR",
        help="Directory containing bulk_llm_generation_<n>.csv files (applied in numeric order).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Rows per UNWIND batch.",
    )
    parser.add_argument(
        "--uri",
        type=str,
        help="Override NEO4J_URI for this run (e.g., neo4j+s://... for Aura).",
    )
    parser.add_argument(
        "--user",
        type=str,
        help="Override NEO4J_USER for this run.",
    )
    parser.add_argument(
        "--password",
        type=str,
        help="Override NEO4J_PASSWORD for this run.",
    )
    args = parser.parse_args()
    paths = _resolve_input_paths(args)
    print(f"[INFO] Input files ({len(paths)}): " + ", ".join(p.name for p in paths))
    rows = _load_rows(paths)
    if not rows:
        raise SystemExit("No data rows with bill_pk")

    if args.uri:
        os.environ["NEO4J_URI"] = args.uri
    if args.user:
        os.environ["NEO4J_USER"] = args.user
    if args.password:
        os.environ["NEO4J_PASSWORD"] = args.password

    # Import after env overrides so module-scope driver picks up effective credentials.
    from src.neo4j_graph.neo4j_client import Neo4j  # noqa: E402

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
