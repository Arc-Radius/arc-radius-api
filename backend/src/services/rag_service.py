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
- Saying when provisions would take effect.

Grounding:
- Explain using only information supported by the bill text.
- If something is unclear, say so instead of guessing.
""".strip()


TASK_PROMPTS = {
    "bill_summary": """
Role: You are a helpful, affirming legislative explainer for LGBTQ+ youth navigating legal landscapes. Use the provided bill data to give accurate, accessible answers in plain language appropriate for teenagers.

Task: Explain what this bill does.

Instructions:
- Write exactly 4 sentences.
- Use only the the provided bill text, metadata, and excerpts.
- Do not mention topics that are not clearly supported by the provided materials.
- Do not speculate.
- Do not interpret intent.
- Be neutral and factual.

Output rules:
 - One paragraph only.
 - No bullets.
 - No headings.
""".strip(),
    "bill_why_matters": """
Role: You are a helpful, affirming policy translator helping LGBTQ+ youth understand how a bill might affect daily life. Use the provided bill data to give accurate, accessible answers in plain language appropriate for teenagers.

Task: Explain why this bill could matter for LGBTQ+ young adults.

Instructions:
- Write exactly 4 sentences.
- Use only the the provided bill text, metadata, and excerpts.
- Do not mention topics that are not clearly supported by the provided materials.
- Do not use second person; do not address the reader (no "you" or "your").
- Focus on real-world impact.
- Avoid exaggeration.
- Avoid fear language.
- Explain practical effects.
- If a bill is harmful, explain what it does without being alarmist.
- If a bill is supportive, highlight how it is supportive to the LGBTQ+ audience.
- If impact is uncertain, clearly state that.

Output rules:
 - One paragraph only.
 - No bullets.
 - No headings.
""".strip(),
    "bill_related": """
Role: You are a helpful, affirming legislative analyst for LGBTQ+ youth navigating legal landscapes. Use the provided bill data to give accurate, accessible answers in plain language appropriate for teenagers.

Task: Highlight bills that are related to the current bill.

Instructions:
- Use only the current bill semantic anchor chunks and candidate related bill records.
- Explain concrete similarities (topic, approach, or legal mechanism).
- Keep the explanation practical and neutral.
- Do not speculate beyond the provided records.
- If relationships are weak or unclear, clearly state that.
- Each bill should be its own bullet point.

Output rules:
- Use bullet points.
- For each bullet, write exactly 2 sentences:
  - Sentence 1: name the related bill with its bill_pk and state code (for example, "TX:2160:1890827 (TX SB 2160)") and explain the concrete similarity.
  - Sentence 2: note an important difference or limitation in the match.
- The state code for the related bill is required in every bullet.
- The related bill's bill_pk is required in every bullet.
- After each bullet, add a new line with: Confidence: low, medium, or high
- Do not add any text before or after the bullet list.
""".strip(),
    "generate_letter": """
Role: You help users draft respectful messages to elected officials.

Safety rules:
- Do not provide legal advice
- Do not threaten or shame
- Do not exaggerate claims
- Do not invent personal details

Task: Write a message about this bill.

User selections:
Format: {email_or_phone}
Position: {support_or_oppose}
Tone: {tone}

Formatting rules:

If Email:
- include subject line
- include greeting with "Dear {representative_greeting}"
- 2-3 paragraphs: introduce yourself and position, explain why using bill details, make a clear request
- include a respectful closing with signature placeholder [Your Name]

If Phone:
- spoken conversational style, not a script to read verbatim
- 45-90 seconds when spoken aloud
- structure: greeting, state your name and that you are a constituent, state your position on the bill, give 2-3 specific reasons referencing the bill content, make a clear ask, thank them
- do not use bullet points, write as flowing speech

The message must:
- clearly state the user's position
- reference specific provisions or effects from the bill text
- include one clear request (vote yes/no, amend, etc.)
- stay respectful
- stay grounded in provided information
- be substantive enough to show the caller/writer has read the bill""".strip(),
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


def _safe(value):
    if value is None:
        return ""
    return str(value).strip()


def _format_current_bill_block(bill_meta: dict) -> str:
    return (
        "Current bill:\n"
        f"- Bill ID: {_safe(bill_meta.get('bill_pk'))}\n"
        f"- State: {_safe(bill_meta.get('state'))}\n"
        f"- Bill number: {_safe(bill_meta.get('bill_number'))}\n"
        f"- Title: {_safe(bill_meta.get('title'))}\n"
    )


def _build_labeled_context_block(
    *,
    bill_meta: dict,
    bill_text_or_chunks: str,
    candidate_records: str | None = None,
) -> str:
    parts = [
        _format_current_bill_block(bill_meta),
        "Trusted source materials:",
        "<bill_text>",
        _safe(bill_text_or_chunks),
        "</bill_text>",
    ]

    if candidate_records is not None:
        parts.extend(
            [
                "",
                "For related-bill tasks only:",
                "<candidate_related_bills>",
                _safe(candidate_records),
                "</candidate_related_bills>",
            ]
        )

    return "\n".join(parts)


def _build_default_prompt(query: str, context: str) -> str:
    return (
        "Role: You are a helpful, affirming legislative explainer for LGBTQ+ youth "
        "navigating legal landscapes. Use the provided bill data to give accurate, "
        "accessible answers in plain language appropriate for teenagers.\n\n"
        "Requirements:\n"
        "- Use only the information provided below.\n"
        "- Do not add outside knowledge.\n"
        "- Do not speculate.\n"
        "- Be neutral and factual.\n"
        "- If information is missing for a state or topic, clearly state that.\n"
        "- When comparing states, organize by state.\n"
        "- Reference specific bill numbers when available.\n\n"
        f"Bill records:\n{context}\n\n"
        f"User question:\n{query}\n\n"
        "If evidence is missing, do not make up information."
    )


def _build_task_prompt(task: str, query: str, context_block: str) -> str:
    if task not in TASK_PROMPTS:
        raise ValueError(f"Unsupported generation task: {task}")

    return (
        f"{TASK_PROMPTS[task]}\n\n"
        f"{context_block}\n\n"
        f"User request:\n{query}\n\n"
        "If evidence is missing, do not make up information."
    )


def _build_task_prompt_template(task: str, query: str) -> str:
    if task not in TASK_PROMPTS:
        raise ValueError(f"Unsupported generation task: {task}")

    context_template = _build_labeled_context_block(
        bill_meta={
            "bill_pk": "{bill_id}",
            "state": "{state}",
            "bill_number": "{bill_number}",
            "title": "{title}",
        },
        bill_text_or_chunks="{bill_text_or_chunks}",
        candidate_records="{candidate_records}" if task == "bill_related" else None,
    )

    return (
        f"{TASK_PROMPTS[task]}\n\n"
        f"{context_template}\n\n"
        f"User request:\n{query}\n\n"
        "If evidence is missing, do not make up information."
    )


def _resolve_letter_prompt(params: dict) -> str:
    rep_name = params.get("representative_name")
    greeting = rep_name if rep_name else "[Representative/Senator Name]"
    return (
        TASK_PROMPTS["generate_letter"]
        .replace("{email_or_phone}", params.get("medium", "email"))
        .replace("{support_or_oppose}", params.get("stance", "support"))
        .replace("{tone}", params.get("tone", "conversational"))
        .replace("{representative_greeting}", greeting)
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
    letter_params: dict | None = None,
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
        bill_meta = _build_shared_bill_meta(ranked, meta) or {
            "bill_pk": bill_pk}
        context_block = _build_labeled_context_block(
            bill_meta=bill_meta,
            bill_text_or_chunks=_limit_context_length(anchor_context or ""),
            candidate_records=_limit_context_length(related_context or ""),
        )
    else:
        ranked, meta, context = graph_rag_query_for_bill(bill_pk)
        bill_meta = _build_shared_bill_meta(ranked, meta) or {
            "bill_pk": bill_pk}
        context_block = _build_labeled_context_block(
            bill_meta=bill_meta,
            bill_text_or_chunks=_limit_context_length(context or ""),
        )

    if task == "generate_letter":
        resolved_prompt_text = _resolve_letter_prompt(letter_params or {})
        prompt = (
            f"{resolved_prompt_text}\n\n"
            f"Bill records:\n{context_block}\n\n"
            "If evidence is missing, do not make up information."
        )
        prompt_template = (
            f"{TASK_PROMPTS['generate_letter']}\n\n"
            "Bill records:\n{{context}}\n\n"
            "If evidence is missing, do not make up information."
        )
    else:
        prompt = _build_task_prompt(
            task=task, query=query, context_block=context_block)
        prompt_template = _build_task_prompt_template(task=task, query=query)

    system = SYSTEM_PROMPT
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
        "bill_pk": bill_pk,
    }
    if shared_bill_task:
        result["bill_meta"] = _build_shared_bill_meta(ranked, meta)
    if task == "bill_related":
        result["related_bills"] = []
        result["related_bills_json"] = "[]"

    return result
