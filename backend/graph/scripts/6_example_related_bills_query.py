import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv()

from graph.api.query import graph_related_bills_query_for_bill
from graph.api.query.formatting import format_context_block


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bill-pk", type=str, required=True)
    parser.add_argument("--seed-query-max-chars", type=int, default=2500)
    parser.add_argument("--top", "-n", type=int, default=30)
    parser.add_argument("--max-related-bills", type=int, default=5)
    parser.add_argument("--max-chunks-per-related-bill", type=int, default=2)
    parser.add_argument("--max-total-chunks", type=int, default=12)
    args = parser.parse_args()

    print(f"Bill PK: {args.bill_pk}\n")

    ranked, meta, context = graph_related_bills_query_for_bill(
        args.bill_pk,
        seed_query_max_chars=args.seed_query_max_chars,
        top=args.top,
        max_related_bills=args.max_related_bills,
        max_chunks_per_related_bill=args.max_chunks_per_related_bill,
        max_total_chunks=args.max_total_chunks,
    )

    print(f"Related chunks returned: {len(ranked)}\n")
    for r in ranked:
        m = meta.get(r["chunk_id"], {})
        print(format_context_block(r["text"], m, score=r["score"]))
        print()

    print("=" * 72)
    print("LLM CONTEXT PAYLOAD")
    print("=" * 72)
    print(context)


if __name__ == "__main__":
    main()
