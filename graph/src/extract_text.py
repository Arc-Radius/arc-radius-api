from pathlib import Path

from bs4 import BeautifulSoup
from unstructured.partition.html import partition_html
from unstructured.partition.pdf import partition_pdf

# use unstructured to extract elements from a file
def extract_elements(path: Path) -> list:
    suffix = path.suffix.lower()
    # partition PDF file
    if suffix == ".pdf":
        # partition PDF file
        return partition_pdf(filename=str(path))
    elif suffix in (".html", ".htm"):
        # partition HTML file
        elements = partition_html(filename=str(path))
        # if no elements, partition text from HTML
        if not elements:
            from unstructured.partition.text import partition_text

            soup = BeautifulSoup(path.read_text(), "lxml")
            elements = partition_text(text=soup.get_text("\n"))
        return elements
    # if not pdf of html
    else:
        from unstructured.partition.text import partition_text

        return partition_text(filename=str(path))

# extract text from elements
def extract_text_from_file(path: Path) -> str:
    elements = extract_elements(path)
    return "\n".join(e.text for e in elements if getattr(e, "text", None)).strip()


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
    for ext in (".pdf", ".html", ".doc"):
        p = base_dir / f"{name}{ext}"
        if p.exists():
            return p
    return None
