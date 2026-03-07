"""
Single file chunking test.
"""

import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from graph.api.chunking import make_chunks
from graph.api.extract_text import extract_elements

BASE_DIR = (
    Path(__file__).resolve().parents[3]
    / "datasources"
    / "legiscan-bill-text"
    / "bill-text"
)


def pick_random_bill_text() -> Path:
    candidates = [
        p for p in BASE_DIR.iterdir()
        if p.is_file() and p.suffix.lower() in {".pdf", ".html", ".htm", ".doc"}
    ]
    if not candidates:
        raise FileNotFoundError(f"No bill text files found in: {BASE_DIR}")
    return random.choice(candidates)


def main():
    # get bill path from command line argument or pick a random one
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else pick_random_bill_text()
    if not path.exists():
        print(f"File not found: {path}")
        sys.exit(1)

    # print bill path
    print(f"File: {path}")
    # extract elements from bill
    elements = extract_elements(path)
    text_len = sum(len(e.text) for e in elements if getattr(e, "text", None))
    print(f"Extracted {len(elements)} elements ({text_len} chars)")

    # make chunks from elements
    chunks = make_chunks(elements)
    print(f"Chunks: {len(chunks)}")
    print("=" * 60)

    # loop through chunks and print them to view in the terminal
    for i, ch in enumerate(chunks):
        section = ch["heading"] or "(no section path)"
        print(f"\n--- chunk {i} | {section} ---")
        print(ch["text"])
        # if len(ch["text"]) > 300:
        #     print(f"... ({len(ch['text']) - 300} more chars)")


if __name__ == "__main__":
    main()
