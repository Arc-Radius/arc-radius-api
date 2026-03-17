from pathlib import Path

from bs4 import BeautifulSoup
from unstructured.partition.html import partition_html
from unstructured.partition.pdf import partition_pdf
from unstructured.partition.text import partition_text


def _safe_read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


# use unstructured to extract elements from a file
def extract_elements(path: Path) -> list:
    suffix = path.suffix.lower()

    if suffix in (".html", ".htm"):
        elements = partition_html(filename=str(path))
        if not elements:
            soup = BeautifulSoup(_safe_read_text(path), "lxml")
            elements = partition_text(text=soup.get_text("\n"))
        return elements

    elif suffix == ".pdf":
        return partition_pdf(
            filename=str(path),
        )

    else:
        return partition_text(filename=str(path))


# extract text from elements
def extract_text_from_file(path: Path) -> str:
    elements = extract_elements(path)
    return "\n".join(
        e.text.strip() for e in elements if getattr(e, "text", None)
    ).strip()


# get local bill path via file name
def local_bill_path(
    state: str,
    bill_id: str,
    session_id: str,
    bill_number: str,
    document_id: str,
    base_dir: Path,
) -> Path | None:
    name = f"{state}_{bill_id}_{session_id}_{bill_number}_{document_id}"

    # prefer html over pdf because structure is often cleaner
    for ext in (".html", ".htm", ".pdf", ".doc", ".txt"):
        p = base_dir / f"{name}{ext}"
        if p.exists():
            return p
    return None