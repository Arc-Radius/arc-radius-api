import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv()

from graph.api.query import graph_rag_query_for_bill
from graph.api.query.formatting import format_context_block


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bill-pk", type=str, required=True)
    args = parser.parse_args()

    print(f"Bill PK: {args.bill_pk}\n")

    ranked, meta, context = graph_rag_query_for_bill(args.bill_pk)

    print(f"Chunks returned: {len(ranked)}\n")
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
