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

    # make chunks from elements using context-aware embedding text
    chunks = make_chunks(
        elements,
        doc_context={
            "state": "TEST",
            "bill_id": "0",
            "bill_number": "TEST-BILL",
            "session_id": "0",
            "document_type": path.suffix.lower().lstrip("."),
            "bill_title": "Chunking smoke test",
            "bill_description": "Validates section/subsection and contextual embed text output.",
        },
    )
    print(f"Chunks: {len(chunks)}")
    print("=" * 60)

    # loop through chunks and print them to view in the terminal
    for i, ch in enumerate(chunks):
        section = ch.get("section_heading") or "(no section heading)"
        subsection = ch.get("subsection_heading") or "-"
        print(f"\n--- chunk {i} | section={section} | subsection={subsection} ---")
        print(ch["text"])
        if ch.get("embed_text"):
            preview = ch["embed_text"][:250].replace("\n", " ")
            print(f"[embed_text preview] {preview}")


if __name__ == "__main__":
    main()
