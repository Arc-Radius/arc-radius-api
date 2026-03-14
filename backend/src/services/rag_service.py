import os

from graph.api.query import (
    graph_rag_query,
    graph_rag_query_for_bill,
    graph_related_bills_query_for_bill,
    graph_semantic_anchor_chunks_for_bill,
)
from bedrock.bedrock_client import generate

SONNET4_ROUGH_CONTEXT_TOKENS = 180_000
CHARS_PER_TOKEN_ESTIMATE = 4
DEFAULT_MAX_CONTEXT_CHARS = SONNET4_ROUGH_CONTEXT_TOKENS * CHARS_PER_TOKEN_ESTIMATE
MAX_CONTEXT_CHARS = int(os.getenv("RAG_MAX_CONTEXT_CHARS", str(DEFAULT_MAX_CONTEXT_CHARS)))

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

Task: Explain why this bill could matter to an LGBTQ+ Young Adult.

Requirements:
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

Task: Highlight bills that are related to the current bill.

Requirements:
- Use only the two sections provided below: current bill semantic anchor chunks and candidate related bill records.
- Explain concrete similarities (topic, approach, or legal mechanism).
- Keep the explanation practical and neutral.
- Do not speculate beyond the provided records.
- If relationships are weak or unclear, clearly state that.
- Each bill should be its own bullet point.
- Length: 2-3 sentences per bill.
- Display a confidence rating below each bullet point of low, medium, or high based on how confident you are in the relationship.
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
        ranked, meta, related_context = graph_related_bills_query_for_bill(bill_pk)
        _, _, anchor_context = graph_semantic_anchor_chunks_for_bill(bill_pk, top=2)
        context = (
            f"Current bill semantic anchor chunks (top 2):\n{anchor_context}\n\n"
            f"Candidate related bill records:\n{related_context}"
        )
    else:
        ranked, meta, context = graph_rag_query_for_bill(bill_pk)
        context = _limit_context_length(context)
    prompt = _build_task_prompt(task=task, query=query, context=context)
    prompt_template = _build_task_prompt_template(task=task, query=query)

    answer = generate(prompt=prompt, system=SYSTEM_PROMPT)
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

    return result
