import re

from unstructured.chunking.basic import chunk_elements
from unstructured.chunking.title import chunk_by_title

# section detection regex
LEGAL_HEADING_RE = re.compile(
    r"""
    ^
    (
        \s*(?:Sec\.|Section|SECTION|§+)\s+[A-Za-z0-9.\-]+.* |
        \s*[A-Z]\.\s+.* |
        \s*\([a-z]{1,3}\)\s+.* |
        \s*\(\d{1,3}\)\s+.* |
        \s*\([A-Z]{1,5}\)\s+.*
    )
    $
    """,
    re.MULTILINE | re.IGNORECASE | re.VERBOSE,
)

# regex for line numbers
LINE_NUMBER_RE = re.compile(r"^\d{1,4}$")

# regex for section headings
SECTION_RE = re.compile(
    r'^\s*["“]?\s*(?:Sec\.|Section|SECTION|§+)\s+[A-Za-z0-9.\-]+',
    re.IGNORECASE,
)

# regex for major paragraph markers like "A. ..."
PARAGRAPH_RE = re.compile(r"^\s*[A-Z]\.\s+")

# regex for minor enumerators like "(1)", "(a)", "(A)" -- do not split on these by default
MINOR_ENUM_RE = re.compile(r"^\s*\(([a-z]{1,3}|\d{1,3}|[A-Z]{1,5})\)\s+")

# minimum chunk characters
MIN_CHUNK_CHARS = 400

# normalize space in text
def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


# check if text is spaced letter garbage
def _is_spaced_letter_garbage(text: str) -> bool:
    # catches lines like: "w e n = l a i r e t a m ..."
    compact = text.strip()
    if not compact:
        return False
    return bool(re.match(r"^(?:[A-Za-z]\s+){5,}[A-Za-z]?$", compact))


def _is_noise_text(text: str) -> bool:
    compact = _normalize_space(text)
    if not compact:
        return True

    # bare line numbers
    if LINE_NUMBER_RE.match(compact):
        return True

    # page markers like "- 2 -"
    if re.match(r"^-\s*\d{1,4}\s*-$", compact):
        return True

    # document/page ids like ".227174.2"
    if re.match(r"^\.*\d{3,}\.\d+$", compact):
        return True

    # OCR/PDF artifact lines with spaced letters
    if _is_spaced_letter_garbage(text):
        return True

    # check for bracketed or underscored material
    lower = compact.lower()
    if "bracketed material" in lower or "underscored material" in lower:
        return True

    return False


# clean chunk text
def _clean_chunk_text(text: str) -> str:
    if not text:
        return ""

    lines = [ln.rstrip() for ln in text.splitlines()]
    kept = []

    for line in lines:
        if _is_noise_text(line):
            continue
        kept.append(line)

    cleaned = "\n".join(kept)

    # collapse excessive blank lines
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned.strip()


# check if element is noise
def _is_noise(element) -> bool:
    text = getattr(element, "text", None) or ""
    return _is_noise_text(text)


# trim heading to max length
def _trim_heading(raw: str, max_len: int = 120) -> str:
    raw = _normalize_space(raw)
    if len(raw) > max_len:
        raw = raw[:max_len].rstrip() + "..."
    return raw


# trim value to max length
def _trim_value(raw: str | None, max_len: int = 300) -> str | None:
    if not raw:
        return None
    raw = _normalize_space(raw)
    if not raw:
        return None
    if len(raw) > max_len:
        raw = raw[:max_len].rstrip() + "..."
    return raw


# extract headings from text
def _extract_headings(text: str) -> list[str]:
    return [_trim_heading(m.group(0).strip()) for m in LEGAL_HEADING_RE.finditer(text)]


# detect section heading
def _detect_section_heading(text: str) -> str | None:
    clean = _normalize_space(text)
    if not clean:
        return None

    m = SECTION_RE.match(clean)
    if not m:
        return None

    return _trim_heading(m.group(0).strip(), max_len=80)


# detect paragraph heading
def _detect_paragraph_heading(text: str) -> str | None:
    clean = _normalize_space(text)
    if not clean:
        return None
    if PARAGRAPH_RE.match(clean):
        return _trim_heading(clean, max_len=180)
    return None


# build context prefix
def _build_context_prefix(
    doc_context: dict | None,
    section_heading: str | None,
    paragraph_heading: str | None,
    heading: str | None,
) -> str:
    """
    Build a context prefix to prepend to chunk text for embedding.
    """
    doc_context = doc_context or {}
    lines = []

    state = _trim_value(doc_context.get("state"), max_len=50)
    bill_number = _trim_value(doc_context.get("bill_number"), max_len=80)
    bill_id = _trim_value(
        str(doc_context.get("bill_id")) if doc_context.get("bill_id") is not None else None,
        max_len=80,
    )
    session_id = _trim_value(
        str(doc_context.get("session_id")) if doc_context.get("session_id") is not None else None,
        max_len=80,
    )
    document_type = _trim_value(doc_context.get("document_type"), max_len=80)
    bill_title = _trim_value(doc_context.get("bill_title"), max_len=240)
    bill_description = _trim_value(doc_context.get("bill_description"), max_len=260)

    if state:
        lines.append(f"State: {state}")
    if bill_number:
        lines.append(f"Bill: {bill_number}")
    elif bill_id:
        lines.append(f"Bill ID: {bill_id}")
    if session_id:
        lines.append(f"Session: {session_id}")
    if document_type:
        lines.append(f"Document type: {document_type}")
    if bill_title:
        lines.append(f"Bill title: {bill_title}")
    if bill_description:
        lines.append(f"Bill description: {bill_description}")

    if section_heading:
        lines.append(f"Section: {section_heading}")
    if paragraph_heading:
        lines.append(f"Paragraph: {paragraph_heading}")
    elif heading and heading != section_heading:
        lines.append(f"Section path: {heading}")

    return "\n".join(lines).strip()


# fallback chunks by title
def _fallback_chunks_by_title(
    cleaned_elements: list,
    doc_context: dict | None,
    max_characters: int,
    new_after_n_chars: int,
    overlap: int,
    combine_under: int,
) -> list[dict]:
    chunks = chunk_by_title(
        cleaned_elements,
        max_characters=max_characters,
        new_after_n_chars=new_after_n_chars,
        overlap=overlap,
        combine_text_under_n_chars=combine_under,
    )

    result = []
    last_heading = None

    for chunk in chunks:
        raw_text = _clean_chunk_text(chunk.text or "")
        if not raw_text:
            continue

        headings = _extract_headings(raw_text)
        if headings:
            last_heading = " > ".join(headings)

        prefix = _build_context_prefix(
            doc_context=doc_context,
            section_heading=last_heading,
            paragraph_heading=None,
            heading=last_heading,
        )
        embed_text = f"{prefix}\n\n{raw_text}" if prefix else raw_text

        result.append(
            {
                "heading": last_heading,
                "section_heading": last_heading,
                "paragraph_heading": None,
                "text": raw_text,
                "embed_text": embed_text,
                "start_char": None,
                "end_char": None,
            }
        )

    return _merge_small_chunks(result, min_chars=MIN_CHUNK_CHARS)


# split section into paragraphs
def _split_section_into_paragraphs(section_elements: list) -> list[tuple[str | None, list]]:
    """
    Within a section, split only on major paragraph markers ("A. ...", "B. ...", etc.)
    Do NOT split on minor enumerators like:
    (a), (1), (A), etc.
    """
    parts: list[tuple[str | None, list]] = []
    current_heading: str | None = None
    current_elements: list = []

    for element in section_elements:
        text = (getattr(element, "text", None) or "").strip()
        paragraph_heading = _detect_paragraph_heading(text)

        if paragraph_heading:
            if current_elements:
                parts.append((current_heading, current_elements))
            current_heading = paragraph_heading
            current_elements = [element]
        else:
            current_elements.append(element)

    if current_elements:
        parts.append((current_heading, current_elements))

    return parts


# merge small chunks
def _merge_small_chunks(chunks: list[dict], min_chars: int = MIN_CHUNK_CHARS) -> list[dict]:
    """
    Merge undersized sibling chunks so retrieval does not surface lots of tiny fragments.
    """
    if not chunks:
        return chunks

    merged: list[dict] = []
    buffer = chunks[0].copy()

    for ch in chunks[1:]:
        buffer_text = (buffer.get("text") or "").strip()
        same_section = buffer.get("section_heading") == ch.get("section_heading")

        if len(buffer_text) < min_chars and same_section:
            next_text = (ch.get("text") or "").strip()
            if next_text:
                buffer["text"] = f"{buffer_text}\n\n{next_text}".strip()

            # rebuild embed_text from merged raw text so context prefix is not duplicated
            prefix = _build_context_prefix(
                doc_context={
                    "state": None,
                    "bill_number": None,
                    "bill_id": None,
                    "session_id": None,
                    "document_type": None,
                    "bill_title": None,
                    "bill_description": None,
                },
                section_heading=None,
                paragraph_heading=None,
                heading=None,
            )

            # keep the existing contextual prefix by extracting everything before first blank line
            old_embed = buffer.get("embed_text") or ""
            if "\n\n" in old_embed:
                existing_prefix = old_embed.split("\n\n", 1)[0].strip()
                if existing_prefix:
                    buffer["embed_text"] = f"{existing_prefix}\n\n{buffer['text']}"
                else:
                    buffer["embed_text"] = buffer["text"]
            else:
                buffer["embed_text"] = buffer["text"]

            # preserve first heading/paragraph heading already in buffer
        else:
            merged.append(buffer)
            buffer = ch.copy()

    merged.append(buffer)
    return merged


# make chunks
def make_chunks(
    elements: list,
    doc_context: dict | None = None,
    max_characters: int = 3500,
    new_after_n_chars: int = 2500,
    overlap: int = 200,
    combine_under: int = 800,
) -> list[dict]:
    """
    Steps:
    1. remove noise elements
    2. split by section headings
    3. split each section by major paragraph headings only
    4. chunk oversized paragraph blocks
    5. prepend contextual metadata into embed_text
    6. merge undersized sibling chunks
    """
    cleaned = [e for e in elements if not _is_noise(e)]

    sections: list[tuple[str | None, list]] = []
    current_heading: str | None = None
    current_elements: list = []
    detected_sections = 0

    for element in cleaned:
        text = (getattr(element, "text", None) or "").strip()
        heading = _detect_section_heading(text)

        if heading:
            if current_elements:
                sections.append((current_heading, current_elements))
            current_heading = heading
            current_elements = [element]
            detected_sections += 1
            continue

        current_elements.append(element)

    if current_elements:
        sections.append((current_heading, current_elements))

    # fallback if section detection fails
    if detected_sections == 0:
        return _fallback_chunks_by_title(
            cleaned_elements=cleaned,
            doc_context=doc_context,
            max_characters=max_characters,
            new_after_n_chars=new_after_n_chars,
            overlap=overlap,
            combine_under=combine_under,
        )

    result = []

    for section_heading, section_elements in sections:
        paragraph_groups = _split_section_into_paragraphs(section_elements)

        for paragraph_heading, paragraph_elements in paragraph_groups:
            heading = paragraph_heading or section_heading

            section_chunks = chunk_elements(
                paragraph_elements,
                max_characters=max_characters,
                new_after_n_chars=new_after_n_chars,
                overlap=overlap,
            )

            for chunk in section_chunks:
                raw_text = _clean_chunk_text(chunk.text or "")
                if not raw_text:
                    continue

                # skip tiny enumerator-only chunks if they somehow slip through
                if len(raw_text) < 80 and MINOR_ENUM_RE.match(raw_text):
                    continue

                prefix = _build_context_prefix(
                    doc_context=doc_context,
                    section_heading=section_heading,
                    paragraph_heading=paragraph_heading,
                    heading=heading,
                )
                embed_text = f"{prefix}\n\n{raw_text}" if prefix else raw_text

                result.append(
                    {
                        "heading": heading,
                        "section_heading": section_heading,
                        "paragraph_heading": paragraph_heading,
                        "text": raw_text,
                        "embed_text": embed_text,
                        "start_char": None,
                        "end_char": None,
                    }
                )

    return _merge_small_chunks(result, min_chars=MIN_CHUNK_CHARS)