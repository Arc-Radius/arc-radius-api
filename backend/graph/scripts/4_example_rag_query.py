import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv()

from graph.api.query.formatting import format_context_block
from graph.api.query import graph_rag_query

DEFAULT_QUERY = "How does Texas compare to other states in terms of handling transgender athletes in school sports?"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", "-q", default=DEFAULT_QUERY)
    parser.add_argument("--top", "-n", type=int, default=10)
    args = parser.parse_args()

    print(f"Query: {args.query}\n")

    ranked, meta, context = graph_rag_query(args.query, top=args.top)

    print(f"Top {len(ranked)} results:\n")
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
