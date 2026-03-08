from graph.api.query import (
    graph_rag_query,
    graph_rag_query_for_bill,
    graph_related_bills_query_for_bill,
)
from bedrock.bedrock_client import generate

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

Grounding:
- Explain using only information supported by the bill text.
- If something is unclear, say so instead of guessing.
""".strip()

TASK_PROMPTS = {
    "bill_summary": """
Role: You are a legislative explainer.

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
Role: You are a policy translator helping someone understand how a bill might affect daily life.

Task: Explain why this bill could matter to an LGBTQ+ Young Adult.

Requirements:
- Focus on real-world impact.
- Avoid exaggeration.
- Avoid fear language.
- Explain practical effects.
- If impact is uncertain, clearly state that.
- Length: 3-5 sentences.
""".strip(),
    "bill_related": """
Role: You are a legislative analyst.

Task: Highlight bills that are related to the current bill.

Requirements:
- Use only the related bill information provided below.
- Explain concrete similarities (topic, approach, or legal mechanism).
- Keep the explanation practical and neutral.
- Do not speculate beyond the provided records.
- If relationships are weak or unclear, clearly state that.
- Length: 3-5 sentences.
""".strip(),
}


def _build_sources(ranked, meta):
    return [
        {
            "chunk_id": r["chunk_id"],
            "text": r["text"],
            "score": r["score"],
            "meta": meta.get(r["chunk_id"], {}),
        }
        for r in ranked
    ]


def _build_sources_without_meta(ranked):
    return [
        {
            "chunk_id": r["chunk_id"],
            "text": r["text"],
            "score": r["score"],
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
        "If evidence is missing, say what is missing."
    )


def _build_task_prompt(task: str, query: str, context: str) -> str:
    if task not in TASK_PROMPTS:
        raise ValueError(f"Unsupported generation task: {task}")

    return (
        f"{TASK_PROMPTS[task]}\n\n"
        f"Bill records:\n{context}\n\n"
        f"User request:\n{query}\n\n"
        "If evidence is missing, say what is missing."
    )


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
    if task == "bill_related":
        ranked, meta, context = graph_related_bills_query_for_bill(bill_pk)
    else:
        ranked, meta, context = graph_rag_query_for_bill(bill_pk)
    prompt = _build_task_prompt(task=task, query=query, context=context)

    answer = generate(prompt=prompt, system=SYSTEM_PROMPT)
    sources = _build_sources_without_meta(ranked) if shared_bill_task else _build_sources(ranked, meta)
    result = {"task": task, "query": query, "answer": answer, "sources": sources}
    result["bill_pk"] = bill_pk
    if shared_bill_task:
        result["bill_meta"] = _build_shared_bill_meta(ranked, meta)

    return result
