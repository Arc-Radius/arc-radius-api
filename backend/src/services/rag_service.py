from graph.api.query import graph_rag_query
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
def query_and_generate(query: str, top: int = 10):
    """Full RAG pipeline: query graph → build context → generate answer."""
    ranked, meta, context = graph_rag_query(query, top=top)
    prompt = (
    "Use only the bill records below as evidence.\n\n"
    f"Bill records:\n{context}\n\n"
    f"User question:\n{query}\n\n"
    "If evidence is missing, say what is missing."
    )

    answer = generate(prompt=prompt, system=SYSTEM_PROMPT)

    sources = [
        {
            "chunk_id": r["chunk_id"],
            "text": r["text"],
            "score": r["score"],
            "meta": meta.get(r["chunk_id"], {}),
        }
        for r in ranked
    ]

    return {"query": query, "answer": answer, "sources": sources}
