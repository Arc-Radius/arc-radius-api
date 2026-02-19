import re

from unstructured.chunking.title import chunk_by_title

# regex for legal headings
LEGAL_HEADING_RE = re.compile(
    r"^(Sec\.\s.+|SECTION\s.+|§+\s*.+|ARTICLE\s.+|PART\s.+|TITLE\s.+|CHAPTER\s.+|SUBDIVISION\s.+)$",
    re.MULTILINE | re.IGNORECASE,
)

# regex for line numbers (in many bills at the start of lines / in margins)
LINE_NUMBER_RE = re.compile(r"^\d{1,3}$")

# subdivision markers like (a), (1), (A) that signal body text
_SUBDIV_RE = re.compile(r"\s+\([a-z0-9]\)\s", re.IGNORECASE)

# filter out margin line numbers and other PDF extraction artifacts
def _is_noise(element) -> bool:
    text = getattr(element, "text", None)
    if not text:
        return True
    text = text.strip()
    if not text:
        return True
    if LINE_NUMBER_RE.match(text):
        return True
    return False

# trim heading line to just the section designator
def _trim_heading(raw: str, max_len: int = 80) -> str:
    m = _SUBDIV_RE.search(raw)
    if m:
        raw = raw[: m.start()]
    if len(raw) > max_len:
        dot = raw.rfind(". ", 0, max_len)
        if dot > 20:
            raw = raw[: dot + 1]
        else:
            raw = raw[:max_len].rstrip() + "…"
    return raw.rstrip()


# find all legal headings in chunk text
def _extract_headings(text: str) -> list[str]:
    return [_trim_heading(m.group(0).strip()) for m in LEGAL_HEADING_RE.finditer(text)]


# make chunks from elements
def make_chunks(
    elements: list,
    max_characters: int = 4000,
    new_after_n_chars: int = 3500,
    overlap: int = 200,
    combine_under: int = 500,
) -> list[dict]:
    cleaned = [e for e in elements if not _is_noise(e)]

    # use unstructured's chunk_by_title to make chunks
    chunks = chunk_by_title(
        cleaned,
        max_characters=max_characters,
        new_after_n_chars=new_after_n_chars,
        overlap=overlap,
        combine_text_under_n_chars=combine_under,
    )

    # create chunk rows
    result = []
    # keep track of last section
    last_section = None
    # loop through chunks
    for chunk in chunks:
        # extract headings from chunk text
        headings = _extract_headings(chunk.text)
        if headings:
            # join headings with " > "
            last_section = " > ".join(headings)
        # add chunk to result
        result.append({
            "heading": last_section,
            "text": chunk.text,
            "start_char": None,
            "end_char": None,
        })
    return result
