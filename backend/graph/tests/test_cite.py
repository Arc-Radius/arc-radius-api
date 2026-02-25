"""
Single-file citation extraction test. Extracts text then finds citations.

Usage (run from graph/):
    python tests/test_cite.py
    python tests/test_cite.py ../datasources/.../some_bill.pdf
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from graph.api.citations import extract_citations
from graph.api.extract_text import extract_text_from_file

DEFAULT = (
    Path(__file__).resolve().parents[2]
    / "datasources"
    / "legiscan-bill-text"
    / "bill-text"
    / "AK_1398089_1796_HB17_2232246.pdf"
)

CONTEXT_CHARS = 50


def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT
    if not path.exists():
        print(f"File not found: {path}")
        sys.exit(1)

    print(f"File: {path}")
    text = extract_text_from_file(path)
    print(f"Extracted {len(text)} chars")

    cites = extract_citations(text)
    print(f"Citations found: {len(cites)}")
    print("=" * 60)

    for c in cites:
        before = text[max(0, c["span_start"] - CONTEXT_CHARS) : c["span_start"]]
        after = text[c["span_end"] : c["span_end"] + CONTEXT_CHARS]
        print(f"\n  jurisdiction: {c['jurisdiction']}")
        print(f"  canonical:    {c['canonical']}")
        print(f"  raw:          {c['raw']}")
        print(f"  confidence:   {c['confidence']}")
        print(f"  context:      ...{before}>>>{c['raw']}<<<{after}...")


if __name__ == "__main__":
    main()
