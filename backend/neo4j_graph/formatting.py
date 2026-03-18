"""Format rich graph metadata chunks into a context block an LLM can use."""

from __future__ import annotations

# format status line


def _status_line(m: dict) -> str:
    parts = []
    if m.get("passed"):
        parts.append("Passed")
    if m.get("failed"):
        parts.append("Failed")
    if m.get("vetoed"):
        parts.append("Vetoed")
    return ", ".join(parts) if parts else (m.get("status") or "Unknown")

# format context block for a single chunk
# IMPORTANT: This should be changed based on how we want to structure the context block for the LLM


def format_context_block(chunk_text: str, meta: dict, score: float | None = None) -> str:
    state = meta.get("state", "?")
    bill_num = meta.get("bill_number", "?")
    title = meta.get("title") or meta.get("description") or "?"

    header = f"[SOURCE: {state} {bill_num} — {title!r}]"

    lines = [header]

    info_parts = [f"State: {state}"]
    if meta.get("year"):
        info_parts.append(f"Year: {meta['year']}")
    info_parts.append(f"Status: {_status_line(meta)}")
    if meta.get("label"):
        info_parts.append(f"Label: {meta['label']}")
    if meta.get("state_lean"):
        info_parts.append(f"Lean: {meta['state_lean']}")
    lines.append(" | ".join(info_parts))

    if meta.get("r_sponsorship_ratio"):
        lines.append(f"R-sponsorship ratio: {meta['r_sponsorship_ratio']}")
    if meta.get("bipartisan_ratio"):
        lines.append(f"Bipartisan ratio: {meta['bipartisan_ratio']}")

    if meta.get("section_path"):
        lines.append(f"Section: {meta['section_path']}")

    if score is not None:
        lines.append(f"Relevance: {score:.4f}")

    lines.append("---")
    lines.append(" ".join(chunk_text.split()))
    lines.append("---")

    urls = []
    if meta.get("bill_url"):
        urls.append(meta["bill_url"])
    if meta.get("state_link"):
        urls.append(meta["state_link"])
    if urls:
        lines.append(f"URLs: {' | '.join(urls)}")

    return "\n".join(lines)

# build context string from ranked chunks and metadata


def build_context(ranked: list[dict], meta_map: dict[str, dict]) -> str:
    blocks = []
    # loop through ranked chunks and format context block for each
    for r in ranked:
        cid = r["chunk_id"]
        m = meta_map.get(cid, {})
        blocks.append(format_context_block(r["text"], m, score=r.get("score")))
    return "\n\n".join(blocks)
