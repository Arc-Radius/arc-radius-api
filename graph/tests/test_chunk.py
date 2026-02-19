"""
Single file chunking test.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.chunking import make_chunks
from src.extract_text import extract_elements

# default to one example bill
DEFAULT = (
    Path(__file__).resolve().parents[2]
    / "datasources"
    / "legiscan-bill-text"
    / "bill-text"
    / "AK_1398089_1796_HB17_2232246.pdf"
)


def main():
    # get bill path from command line argument or use the default
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT
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
        print(ch["text"][:300])
        if len(ch["text"]) > 300:
            print(f"... ({len(ch['text']) - 300} more chars)")


if __name__ == "__main__":
    main()
