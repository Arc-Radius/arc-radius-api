import json
import os

from src.neo4j_graph.graph_rag_query import (
    graph_rag_query,
    graph_rag_query_for_bill,
    graph_related_bills_query_for_bill,
    graph_semantic_anchor_chunks_for_bill,
)
from src.bedrock.bedrock_client import generate

DEFAULT_MAX_CONTEXT_CHARS = 160_000
MAX_CONTEXT_CHARS = int(
    os.getenv("RAG_MAX_CONTEXT_CHARS", str(DEFAULT_MAX_CONTEXT_CHARS)))

SYSTEM_PROMPT = """
Audience:
The message is for LGBTQ+ young adults (ages 18-20).

Style guidelines:
- Use clear, accessible language.
- Avoid jargon.
- Avoid assumptions about identity or experiences.
- Do not assume the user is publicly out.
- Be supportive, but not overly emotional.
- Respect autonomy and user choice.

User details:
- Do not invent personal details about the user.
- Only use personal information if explicitly provided.

Language level:
- Target 8th-9th grade reading level.
- Use a conversational but respectful tone.
- No slang.
- No academic phrasing.

Tone requirements:
- Calm.
- Respectful.
- Non-judgmental.
- Empowering, not directive.
- Informational, not persuasive pressure.

Clarity rules:
- Keep sentences under 20 words.
- Prefer common words over complex terms.
- Avoid legal terminology unless defined.

Avoid:
- Speaking on behalf of all LGBTQ people.
- Assuming political views.
- Assuming identity labels.
- Moralizing language.
- Urgent, alarmist phrasing.
- Giving legal advice.
- Commenting on the status of the bill.

Grounding:
- Explain using only information supported by the bill text.
- If something is unclear, say so instead of guessing.
""".strip()

RELATED_BILLS_JSON_SYSTEM = """
Output format (this task only):
Return a single JSON array only. Do not use markdown code fences or any prose outside the array.
Each object must use exactly: {"bill_pk": "<STATE:SESSION_ID:BILL_ID>", "summary": "<2-3 sentences>", "confidence": "low"|"medium"|"high"}.
Every bill_pk MUST be copied exactly from a candidate related bill in the provided records (see SOURCE lines / bill metadata). Do not invent bill_pk values.
If nothing in the records is clearly related, return [].
""".strip()

_ALLOWED_CONFIDENCE = frozenset({"low", "medium", "high"})


def _strip_json_fence(text: str) -> str:
    t = text.strip()
    if not t.startswith("```"):
        return t
    lines = t.split("\n")
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip().startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _candidate_bill_pks_from_chunk_meta(meta: dict[str, dict]) -> set[str]:
    out: set[str] = set()
    for m in meta.values():
        pk = str(m.get("bill_pk") or "").strip()
        if pk:
            out.add(pk)
    return out


def parse_related_bills_response(
    raw: str,
    allowed_bill_pks: set[str],
) -> tuple[list[dict[str, str]], str | None]:
    """
    Parse model JSON; keep only bill_pk present in retrieval candidates.
    Returns (items, error_message).
    """
    text = _strip_json_fence(raw)
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        return [], f"json_decode:{exc}"
    if not isinstance(data, list):
        return [], "expected_top_level_array"
    items: list[dict[str, str]] = []
    seen: set[str] = set()
    for el in data:
        if not isinstance(el, dict):
            continue
        pk = str(el.get("bill_pk") or "").strip()
        summary = str(el.get("summary") or "").strip()
        conf = str(el.get("confidence") or "").lower().strip()
        if pk not in allowed_bill_pks or not summary:
            continue
        if conf not in _ALLOWED_CONFIDENCE:
            conf = "medium"
        if pk in seen:
            continue
        seen.add(pk)
        items.append({"bill_pk": pk, "summary": summary, "confidence": conf})
    return items, None


TASK_PROMPTS = {
    "bill_summary": """
Role: You are a helpful, affirming legislative explainer for LGBTQ+ youth navigating legal landscapes. Use the provided bill data to give accurate, accessible answers in plain language appropriate for teenagers.

Task: Explain what this bill does.

Rules:
- Use only the information provided below.
- Do not add outside knowledge.
- Do not speculate.
- Do not interpret intent.
- Length: 3-5 sentences.
- Be neutral and factual.
""".strip(),
    "bill_why_matters": """
Role: You are a helpful, affirming policy translator helping LGBTQ+ youth understand how a bill might affect daily life. Use the provided bill data to give accurate, accessible answers in plain language appropriate for teenagers.

Task: Explain why this bill could matter for LGBTQ+ young adults.

Requirements:
- Do not use second person; do not address the reader (no "you" or "your").
- Focus on real-world impact.
- Avoid exaggeration.
- Avoid fear language.
- Explain practical effects.
- If a bill is harmful, explain what it does without being alarmist.
- If a bill is supportive, highlight how it is supportive to the LGBTQ+ audience.
- If impact is uncertain, clearly state that.
- Length: 3-5 sentences.
""".strip(),
    "bill_related": """
Role: You are a helpful, affirming legislative analyst for LGBTQ+ youth navigating legal landscapes. Use the provided bill data to give accurate, accessible answers in plain language appropriate for teenagers.

Task: Identify bills from the candidate related bill records that relate to the current bill.

Requirements:
- Use only: (1) current bill semantic anchor chunks and (2) candidate related bill records below.
- For each related bill, write a 2-3 sentence neutral summary of the concrete similarity (topic, approach, or legal mechanism).
- Assign confidence low, medium, or high per bill.
- Output must be ONLY the JSON array described in the system instructions — no bullet lists or extra text.
- If relationships are weak or unclear for all candidates, return an empty array [].
""".strip(),
}


def _build_sources(ranked, meta):
    return [
        {
            "chunk_id": r["chunk_id"],
            "score": r["score"],
            "bill_pk": meta.get(r["chunk_id"], {}).get("bill_pk"),
            "state": meta.get(r["chunk_id"], {}).get("state"),
            "bill_number": meta.get(r["chunk_id"], {}).get("bill_number"),
            "title": meta.get(r["chunk_id"], {}).get("title"),
            "chunk_text": _limit_context_length(r.get("text", "")),
            "chunk_index": meta.get(r["chunk_id"], {}).get("chunk_index"),
        }
        for r in ranked
    ]


def _build_shared_bill_meta(ranked, meta):
    if not ranked:
        return {}
    first_chunk_id = ranked[0]["chunk_id"]
    first_meta = dict(meta.get(first_chunk_id, {}))
    first_meta.pop("chunk_id", None)
    first_meta.pop("section_path", None)
    return first_meta


def _build_default_prompt(query: str, context: str) -> str:
    return (
        "Use only the bill records below as evidence.\n\n"
        f"Bill records:\n{context}\n\n"
        f"User question:\n{query}\n\n"
        "If evidence is missing, do not make up information."
    )


def _build_task_prompt(task: str, query: str, context: str) -> str:
    if task not in TASK_PROMPTS:
        raise ValueError(f"Unsupported generation task: {task}")

    return (
        f"{TASK_PROMPTS[task]}\n\n"
        f"Bill records:\n{context}\n\n"
        f"User request:\n{query}\n\n"
        "If evidence is missing, do not make up information."
    )


def _build_task_prompt_template(task: str, query: str) -> str:
    if task not in TASK_PROMPTS:
        raise ValueError(f"Unsupported generation task: {task}")

    return (
        f"{TASK_PROMPTS[task]}\n\n"
        "Bill records:\n{context}\n\n"
        f"User request:\n{query}\n\n"
        "If evidence is missing, do not make up information."
    )


def _limit_context_length(context: str, max_chars: int = MAX_CONTEXT_CHARS) -> str:
    if max_chars <= 0 or len(context) <= max_chars:
        return context
    return context[:max_chars]


def query_and_generate(query: str, top: int = 10):
    """Full RAG pipeline: query graph → build context → generate answer."""
    ranked, meta, context = graph_rag_query(query, top=top)
    prompt = _build_default_prompt(query=query, context=context)

    answer = generate(prompt=prompt, system=SYSTEM_PROMPT)
    sources = _build_sources(ranked, meta)

    return {"query": query, "answer": answer, "sources": sources}


def query_and_generate_task(
    task: str,
    bill_pk: str,
):
    """Task-based bill generation handler using all chunks from one bill."""
    query = task
    shared_bill_task = task in {"bill_summary", "bill_why_matters"}
    anchor_context: str | None = None
    if task == "bill_related":
        ranked, meta, related_context = graph_related_bills_query_for_bill(
            bill_pk)
        _, _, anchor_context = graph_semantic_anchor_chunks_for_bill(
            bill_pk, top=3)
        context = (
            f"Current bill semantic anchor chunks (top 3):\n{anchor_context}\n\n"
            f"Candidate related bill records:\n{related_context}"
        )
        context = _limit_context_length(context)
    else:
        ranked, meta, context = graph_rag_query_for_bill(bill_pk)
        context = _limit_context_length(context)
    prompt = _build_task_prompt(task=task, query=query, context=context)
    prompt_template = _build_task_prompt_template(task=task, query=query)

    system = (
        f"{SYSTEM_PROMPT}\n\n{RELATED_BILLS_JSON_SYSTEM}"
        if task == "bill_related"
        else SYSTEM_PROMPT
    )
    max_tokens = 2048 if task == "bill_related" else 1024
    answer = generate(prompt=prompt, system=system, max_tokens=max_tokens)
    sources = _build_sources(ranked, meta)
    result = {
        "task": task,
        "query": query,
        "prompt_template": prompt_template,
        "answer": answer,
        "anchor_context": anchor_context,
        "sources": sources,
    }
    result["bill_pk"] = bill_pk
    if shared_bill_task:
        result["bill_meta"] = _build_shared_bill_meta(ranked, meta)
    if task == "bill_related":
        allowed = _candidate_bill_pks_from_chunk_meta(meta)
        items, parse_err = parse_related_bills_response(answer, allowed)
        result["related_bills"] = items
        result["related_bills_json"] = json.dumps(items, separators=(",", ":"))
        result["related_bills_parse_error"] = parse_err
        if parse_err is None:
            result["answer"] = result["related_bills_json"]

    return result
