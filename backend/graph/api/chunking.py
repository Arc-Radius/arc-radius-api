import re

from unstructured.chunking.basic import chunk_elements
from unstructured.chunking.title import chunk_by_title

LEGAL_HEADING_RE = re.compile(r"^(Sec\.\s.+|SECTION\s.+|§+\s*.+)$", re.MULTILINE | re.IGNORECASE)
LINE_NUMBER_RE = re.compile(r"^\d{1,3}$")
SECTION_RE = re.compile(r'^\s*["“]?\s*(?:Sec\.|Section|SECTION|§)\s+', re.IGNORECASE)


def _is_noise_text(text: str) -> bool:
    compact = re.sub(r"\s+", " ", text).strip()
    if not compact:
        return True
    if LINE_NUMBER_RE.match(compact):
        return True
    return False


def _is_noise(element) -> bool:
    text = getattr(element, "text", None) or ""
    return _is_noise_text(text)


def _trim_heading(raw: str, max_len: int = 80) -> str:
    raw = " ".join(raw.split())
    if len(raw) > max_len:
        raw = raw[:max_len].rstrip() + "..."
    return raw.rstrip()


def _extract_headings(text: str) -> list[str]:
    return [_trim_heading(m.group(0).strip()) for m in LEGAL_HEADING_RE.finditer(text)]


def _detect_section_heading(text: str) -> str | None:
    clean = re.sub(r"\s+", " ", text).strip()
    if not clean:
        return None
    if SECTION_RE.match(clean):
        return _trim_heading(clean, max_len=160)
    return None


def _fallback_chunks_by_title(
    cleaned_elements: list,
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
    last_section = None
    for chunk in chunks:
        headings = _extract_headings(chunk.text)
        if headings:
            last_section = " > ".join(headings)
        result.append({
            "heading": last_section,
            "text": chunk.text,
            "start_char": None,
            "end_char": None,
        })
    return result


def make_chunks(
    elements: list,
    max_characters: int = 4000,
    new_after_n_chars: int = 3500,
    overlap: int = 200,
    combine_under: int = 500,
) -> list[dict]:
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

    # Safety fallback for non-legislative docs with no detectable sections.
    if detected_sections == 0:
        return _fallback_chunks_by_title(
            cleaned,
            max_characters=max_characters,
            new_after_n_chars=new_after_n_chars,
            overlap=overlap,
            combine_under=combine_under,
        )

    result = []
    for heading, section_elements in sections:
        section_chunks = chunk_elements(
            section_elements,
            max_characters=max_characters,
            new_after_n_chars=new_after_n_chars,
            overlap=overlap,
        )
        for chunk in section_chunks:
            result.append({
                "heading": heading,
                "text": chunk.text,
                "start_char": None,
                "end_char": None,
            })
    return result
