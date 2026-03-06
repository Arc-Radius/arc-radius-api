from graph.src.query import graph_rag_query 
from bedrock.bedrock_client import generate

SYSTEM_PROMPT = """You are a helpful, affirming assistant for LGBTQ+ youth 
navigating legal landscapes. Use the provided bill data to give accurate, 
accessible answers in plain language appropriate for teenagers. Always be 
supportive and factual. If a bill is harmful, explain what it does clearly 
without being alarmist. If a bill is supportive, highlight the protections 
it offers."""


def build_context(ranked, meta):
    """Format graph results into LLM-ready context."""
    return "\n\n".join([
        f"Bill: {meta.get(r['chunk_id'], {}).get('bill_number', 'N/A')} "
        f"({meta.get(r['chunk_id'], {}).get('state', 'N/A')})\n"
        f"{r['text']}"
        for r in ranked
    ])


def query_and_generate(query: str, top: int = 10):
    """Full RAG pipeline: query graph → build context → generate answer."""
    ranked, meta, context = graph_rag_query(query, top=top)

    graph_context = build_context(ranked, meta)

    prompt = f"Based on these legislative records:\n\n{graph_context}\n\nQuestion: {query}"

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
