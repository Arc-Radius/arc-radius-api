"""
List every :Bill bill_pk from Neo4j, run bill_summary / bill_why_matters / bill_related
via the same pipeline as POST /generate/bill, write one wide CSV row per bill.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
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
    "output_sources",
    "errors",
)


def build_output_sources(
    sources: list[dict],
    anchor_context: str | None = None,
) -> dict:
    return {
        "anchor_context": anchor_context,
        "sources": sources,
    }


def _graph_output_dir() -> Path:
    return _REPO_ROOT / "graph" / "output"


_BULK_CSV_NUMERIC = re.compile(r"^bulk_llm_generation_(\d+)\.csv$")


def _next_bulk_csv_path(output_dir: Path) -> Path:
    """bulk_llm_generation_1.csv, _2.csv, ... skipping unrelated filenames."""
    max_n = 0
    if output_dir.is_dir():
        for p in output_dir.glob("bulk_llm_generation_*.csv"):
            m = _BULK_CSV_NUMERIC.match(p.name)
            if m:
                max_n = max(max_n, int(m.group(1)))
    return output_dir / f"bulk_llm_generation_{max_n + 1}.csv"


def load_all_bill_pks(*, start_after: str | None) -> list[str]:
    db = Neo4j()
    try:
        rows = db.run(LIST_BILL_PKS_CYPHER)
    finally:
        db.close()
    pks = [str(r["bill_pk"]).strip() for r in rows if r.get("bill_pk")]
    if start_after:
        pks = [p for p in pks if p > start_after]
    return pks


def slice_bill_pks(
    pks: list[str],
    *,
    offset: int,
    max_bills: int | None,
) -> list[str]:
    if offset < 0:
        raise ValueError("offset must be >= 0")
    pks = pks[offset:]
    if max_bills is not None:
        pks = pks[: max_bills]
    return pks


def run_one_bill(bill_pk: str, *, sleep_s: float) -> dict[str, str]:
    row = {
        "bill_pk": bill_pk,
        "llm_bill_summary": "",
        "llm_bill_why_matters": "",
        "llm_related_bills_json": "[]",
        "output_sources": "",
        "errors": "",
    }
    sources_by_task: dict[str, dict] = {
        t: build_output_sources([], None) for t in BULK_TASKS
    }
    errs: list[str] = []
    for i, task in enumerate(BULK_TASKS):
        if i and sleep_s > 0:
            time.sleep(sleep_s)
        try:
            out = query_and_generate_task(task, bill_pk)
            sources_by_task[task] = build_output_sources(
                out.get("sources") or [],
                out.get("anchor_context"),
            )
            if task == "bill_summary":
                row["llm_bill_summary"] = (out.get("answer") or "").strip()
            elif task == "bill_why_matters":
                row["llm_bill_why_matters"] = (out.get("answer") or "").strip()
            else:
                row["llm_related_bills_json"] = (out.get("answer") or "").strip()
        except Exception as exc:
            errs.append(f"{task}:{exc!s}")
    row["output_sources"] = json.dumps(sources_by_task)
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
        help="Default: graph/output/bulk_llm_generation_<n>.csv (next integer).",
    )
    parser.add_argument(
        "--max-bills",
        type=int,
        default=None,
        help="Max bills in this run after --offset (batch size / smoke test).",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Skip this many bills (ordered bill_pk) after --start-after. For 100-bill "
        "batches: 0, 100, 200, …",
    )
    parser.add_argument(
        "--page",
        type=int,
        default=None,
        metavar="N",
        help="1-based page index; sets --offset to (N-1)*--page-size. Mutually exclusive "
        "with --offset.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        metavar="K",
        help="Bills per page when using --page (default: 100).",
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

    if args.page is not None and args.offset != 0:
        parser.error("Use either --page or --offset, not both")
    if args.page is not None and args.page < 1:
        parser.error("--page must be >= 1")
    offset = args.offset
    if args.page is not None:
        offset = (args.page - 1) * args.page_size
    max_bills = args.max_bills
    if args.page is not None and max_bills is None:
        max_bills = args.page_size

    all_pks = load_all_bill_pks(start_after=args.start_after)
    total_after_filter = len(all_pks)
    pks = slice_bill_pks(all_pks, offset=offset, max_bills=max_bills)

    print(
        f"[INFO] Bills in scope (after --start-after): {total_after_filter}; "
        f"this run: offset={offset}"
        + (f", limit={max_bills}" if max_bills is not None else "")
        + f" → {len(pks)} bills",
    )
    if args.dry_run:
        print("[DRY-RUN] First PKs:", pks[:5])
        if pks:
            print("[DRY-RUN] Last PK:", pks[-1])
        return

    out_path = args.out_csv
    if out_path is None:
        out_path = _next_bulk_csv_path(_graph_output_dir())
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
