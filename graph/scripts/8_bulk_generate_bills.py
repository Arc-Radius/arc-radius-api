#!/usr/bin/env python3
"""
List every :Bill bill_pk from Neo4j, run bill_summary / bill_why_matters / bill_related
via the same pipeline as POST /generate/bill, write one wide CSV row per bill.

Requires backend env (NEO4J_URI, AWS/Bedrock for generate). Run from repo root, e.g.:
  cd backend && poetry run python ../graph/scripts/8_bulk_generate_bills.py --max-bills 2

Do not run in CI without credentials; this script is intentionally offline-capable only
when Neo4j + Bedrock are configured.
"""

from __future__ import annotations

import argparse
import csv
import secrets
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BACKEND_ROOT = _REPO_ROOT / "backend"
sys.path.insert(0, str(_BACKEND_ROOT))

csv.field_size_limit(sys.maxsize)

from dotenv import load_dotenv  # noqa: E402

load_dotenv(_BACKEND_ROOT / ".env")

from src.neo4j_graph.neo4j_client import Neo4j  # noqa: E402
from src.services.rag_service import query_and_generate_task  # noqa: E402

BULK_TASKS = ("bill_summary", "bill_why_matters", "bill_related")

LIST_BILL_PKS_CYPHER = """
MATCH (b:Bill)
WHERE coalesce(b.bill_pk, '') <> ''
RETURN DISTINCT b.bill_pk AS bill_pk
ORDER BY b.bill_pk
"""

OUTPUT_FIELDS = (
    "bill_pk",
    "llm_bill_summary",
    "llm_bill_why_matters",
    "llm_related_bills_json",
    "errors",
)


def _graph_output_dir() -> Path:
    return _REPO_ROOT / "graph" / "output"


def load_bill_pks(
    *,
    max_bills: int | None,
    start_after: str | None,
) -> list[str]:
    db = Neo4j()
    try:
        rows = db.run(LIST_BILL_PKS_CYPHER)
    finally:
        db.close()
    pks = [str(r["bill_pk"]).strip() for r in rows if r.get("bill_pk")]
    if start_after:
        pks = [p for p in pks if p > start_after]
    if max_bills is not None:
        pks = pks[: max_bills]
    return pks


def run_one_bill(bill_pk: str, *, sleep_s: float) -> dict[str, str]:
    row = {
        "bill_pk": bill_pk,
        "llm_bill_summary": "",
        "llm_bill_why_matters": "",
        "llm_related_bills_json": "[]",
        "errors": "",
    }
    errs: list[str] = []
    for i, task in enumerate(BULK_TASKS):
        if i and sleep_s > 0:
            time.sleep(sleep_s)
        try:
            out = query_and_generate_task(task, bill_pk)
            if task == "bill_summary":
                row["llm_bill_summary"] = (out.get("answer") or "").strip()
            elif task == "bill_why_matters":
                row["llm_bill_why_matters"] = (out.get("answer") or "").strip()
            else:
                row["llm_related_bills_json"] = (
                    out.get("related_bills_json") or "[]"
                ).strip()
                pe = out.get("related_bills_parse_error")
                if pe:
                    errs.append(f"bill_related_parse:{pe}")
        except Exception as exc:
            errs.append(f"{task}:{exc!s}")
    row["errors"] = "; ".join(errs)
    return row


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk LLM generation for all bills in Neo4j (wide CSV).",
    )
    parser.add_argument(
        "--out-csv",
        type=Path,
        default=None,
        help="Default: graph/output/bulk_llm_generation_<random>.csv",
    )
    parser.add_argument(
        "--max-bills",
        type=int,
        default=None,
        help="Limit bills (smoke test).",
    )
    parser.add_argument(
        "--start-after",
        default=None,
        help="Only include bill_pk > this string (lexicographic resume).",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=2.0,
        help="Pause between generation tasks (not between bills).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print bill count and first few PKs; exit without calling Bedrock.",
    )
    args = parser.parse_args()

    pks = load_bill_pks(max_bills=args.max_bills, start_after=args.start_after)
    print(f"[INFO] Bills to process: {len(pks)}")
    if args.dry_run:
        print("[DRY-RUN] First PKs:", pks[:5])
        return

    out_path = args.out_csv
    if out_path is None:
        rid = secrets.token_hex(4)
        out_path = _graph_output_dir() / f"bulk_llm_generation_{rid}.csv"
    out_path = out_path.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[WRITE] {out_path}")
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(OUTPUT_FIELDS))
        w.writeheader()
        for idx, pk in enumerate(pks, start=1):
            print(f"[BILL {idx}/{len(pks)}] {pk}")
            row = run_one_bill(pk, sleep_s=args.sleep_seconds)
            w.writerow(row)
            f.flush()

    print("[DONE]")


if __name__ == "__main__":
    main()
