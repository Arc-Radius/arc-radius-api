"""
Single-file extraction test.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from graph.api.extract_text import extract_text_from_file

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
    print(f"File:  {path}")
    # print bill type
    print(f"Type:  {path.suffix}")
    text = extract_text_from_file(path)
    # print number of characters in text
    print(f"Chars: {len(text)}")
    print("=" * 60)
    # print first 2000 characters of text
    print(text[:2000])
    if len(text) > 2000:
        print(f"\n... ({len(text) - 2000} more chars)")


if __name__ == "__main__":
    main()
