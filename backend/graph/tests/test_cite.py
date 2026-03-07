"""
Single-file citation extraction test. Extracts text then finds citations.

Usage (run from graph/):
    python tests/test_cite.py
    python tests/test_cite.py ../datasources/.../some_bill.pdf
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from graph.api.extract_text import extract_text_from_file

DEFAULT = (
    Path(__file__).resolve().parents[3]
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


if __name__ == "__main__":
    main()
