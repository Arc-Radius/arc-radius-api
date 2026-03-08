"""GraphRAG retrieval query pipeline built on neo4j-graphrag retrievers."""

from __future__ import annotations

import logging
import os
from typing import Any

from neo4j_graphrag.retrievers import HybridCypherRetriever, VectorCypherRetriever
from neo4j_graphrag.types import RetrieverResultItem

from graph.api.query.bedrock_embedder import BedrockEmbedder
from graph.api.query.formatting import build_context
from graph.api.neo4j_client import Neo4j

logger = logging.getLogger(__name__)

DEFAULT_RETRIEVER_MODE = "vector"
VECTOR_INDEX_NAME = "chunkEmbeddingIndex"
FULLTEXT_INDEX_NAME = "chunkTextIndex"
EFFECTIVE_SEARCH_RATIO = 4
HYBRID_RANKER = "naive"

RETRIEVAL_CYPHER = """
WITH node AS seed, score AS seed_score
MATCH (d:Document)-[:HAS_CHUNK]->(seed)
MATCH (b:Bill)-[:HAS_DOCUMENT]->(d)
OPTIONAL MATCH (d)-[:HAS_CHUNK]->(sib:Chunk)
WHERE abs(sib.chunk_index - seed.chunk_index) <= 2
WITH seed, seed_score, b, collect(DISTINCT sib) AS sibling_nodes
OPTIONAL MATCH (b)-[:HAS_TOPIC]->(:Topic)<-[:HAS_TOPIC]-(ob:Bill)
WHERE ob <> b
OPTIONAL MATCH (ob)-[:HAS_DOCUMENT]->(:Document)-[:HAS_CHUNK]->(tc:Chunk)
WITH seed, seed_score, b, sibling_nodes, collect(DISTINCT tc)[..50] AS topic_nodes
OPTIONAL MATCH (b)-[:IN_STATE]->(s:State)<-[:IN_STATE]-(sb:Bill)
WHERE sb <> b
OPTIONAL MATCH (sb)-[:HAS_DOCUMENT]->(:Document)-[:HAS_CHUNK]->(sc:Chunk)
WITH seed, seed_score, sibling_nodes, topic_nodes, collect(DISTINCT sc)[..50] AS state_nodes
WITH [seed]
     + sibling_nodes
     + [n IN topic_nodes WHERE n IS NOT NULL]
     + [n IN state_nodes WHERE n IS NOT NULL] AS expanded_nodes,
     seed_score
UNWIND expanded_nodes AS node
WITH node, max(seed_score) AS score
RETURN node, score
ORDER BY score DESC
LIMIT $top_k
"""

# metadata query to get metadata for the chunks
METADATA_CYPHER = """
UNWIND $chunk_ids AS cid
MATCH (d:Document)-[:HAS_CHUNK]->(c:Chunk {chunk_id: cid})
MATCH (b:Bill)-[:HAS_DOCUMENT]->(d)
OPTIONAL MATCH (b)-[:IN_STATE]->(s:State)

RETURN c.chunk_id AS chunk_id,
       c.section_path AS section_path,
       d.document_id AS document_id,
       d.url AS doc_url,
       d.document_desc AS doc_desc,
       b.bill_pk AS bill_pk,
       coalesce(b.state, s.code) AS state,
       b.bill_number AS bill_number,
       b.title AS title,
       b.description AS description,
       b.status_desc AS status,
       b.label AS label,
       b.year AS year,
       b.state_lean AS state_lean,
       b.r_sponsorship_ratio AS r_sponsorship_ratio,
       b.bipartisan_ratio AS bipartisan_ratio,
       b.passed AS passed,
       b.failed AS failed,
       b.vetoed AS vetoed,
       b.url AS bill_url,
       b.state_link AS state_link
"""

BILL_CHUNKS_CYPHER = """
MATCH (b:Bill {bill_pk: $bill_pk})-[:HAS_DOCUMENT]->(:Document)-[:HAS_CHUNK]->(c:Chunk)
RETURN c.chunk_id AS chunk_id,
       coalesce(c.text, "") AS text
ORDER BY c.chunk_index ASC
"""


def _record_formatter(record: Any) -> RetrieverResultItem:
    node = record.get("node")
    score = record.get("score")
    if node is None:
        return RetrieverResultItem(content={"chunk_id": None, "text": ""}, metadata={"score": score})
    return RetrieverResultItem(
        content={
            "chunk_id": node.get("chunk_id"),
            "text": node.get("text", ""),
        },
        metadata={"score": score},
    )


def _extract_ranked(result: Any) -> list[dict]:
    ranked: list[dict] = []
    for item in result.items:
        content = item.content or {}
        chunk_id = content.get("chunk_id")
        if not chunk_id:
            continue
        ranked.append(
            {
                "chunk_id": chunk_id,
                "text": content.get("text", ""),
                "score": float((item.metadata or {}).get("score", 0.0)),
            }
        )
    return ranked


def _run_vector_retrieval(db: Neo4j, query: str, top: int) -> list[dict]:
    retriever = VectorCypherRetriever(
        db.driver,
        VECTOR_INDEX_NAME,
        retrieval_query=RETRIEVAL_CYPHER,
        embedder=BedrockEmbedder(),
        result_formatter=_record_formatter,
    )
    result = retriever.search(
        query_text=query,
        top_k=max(top, 1),
        effective_search_ratio=EFFECTIVE_SEARCH_RATIO,
    )
    return _extract_ranked(result)


def _run_hybrid_retrieval(db: Neo4j, query: str, top: int) -> list[dict]:
    retriever = HybridCypherRetriever(
        db.driver,
        VECTOR_INDEX_NAME,
        FULLTEXT_INDEX_NAME,
        retrieval_query=RETRIEVAL_CYPHER,
        embedder=BedrockEmbedder(),
        result_formatter=_record_formatter,
    )
    result = retriever.search(
        query_text=query,
        top_k=max(top, 1),
        effective_search_ratio=EFFECTIVE_SEARCH_RATIO,
        ranker=HYBRID_RANKER,
    )
    return _extract_ranked(result)


def _search_ranked(db: Neo4j, query: str, top: int) -> list[dict]:
    mode = os.getenv("RAG_RETRIEVER_MODE", DEFAULT_RETRIEVER_MODE).strip().lower()
    if mode == "hybrid":
        try:
            return _run_hybrid_retrieval(db, query, top)
        except Exception:
            logger.exception("Hybrid retrieval failed, falling back to vector retriever")
            return _run_vector_retrieval(db, query, top)
    if mode != "vector":
        logger.warning("Unknown RAG_RETRIEVER_MODE=%r, using vector retriever", mode)
    return _run_vector_retrieval(db, query, top)


def graph_rag_query(
    query: str, *, top: int = 10, db: Neo4j | None = None
) -> tuple[list[dict], dict[str, dict], str]:
    own_db = db is None
    if own_db:
        db = Neo4j()

    try:
        ranked = _search_ranked(db, query, top)
        top_ranked = ranked[:top]
        chunk_ids = [r["chunk_id"] for r in top_ranked]
        if not chunk_ids:
            return top_ranked, {}, ""
        meta_rows = db.run(METADATA_CYPHER, chunk_ids=chunk_ids)
        meta = {m["chunk_id"]: m for m in meta_rows}
        context = build_context(top_ranked, meta)
        return top_ranked, meta, context
    finally:
        if own_db:
            db.close()


def graph_rag_query_for_bill(
    bill_pk: str, *, db: Neo4j | None = None
) -> tuple[list[dict], dict[str, dict], str]:
    own_db = db is None
    if own_db:
        db = Neo4j()

    try:
        ranked = db.run(BILL_CHUNKS_CYPHER, bill_pk=bill_pk)
        if not ranked:
            return [], {}, ""
        for row in ranked:
            row["score"] = 1.0

        chunk_ids = [r["chunk_id"] for r in ranked]
        meta_rows = db.run(METADATA_CYPHER, chunk_ids=chunk_ids)
        meta = {m["chunk_id"]: m for m in meta_rows}
        context = build_context(ranked, meta)
        return ranked, meta, context
    finally:
        if own_db:
            db.close()


def _build_related_seed_query(current_ranked: list[dict], max_chunks: int = 3) -> str:
    seed_chunks = current_ranked[:max_chunks]
    seed_text = "\n\n".join((chunk.get("text") or "").strip() for chunk in seed_chunks)
    return seed_text[:2500]


def graph_related_bills_query_for_bill(
    bill_pk: str,
    *,
    seed_chunk_count: int = 3,
    top: int = 30,
    max_related_bills: int = 8,
    db: Neo4j | None = None,
) -> tuple[list[dict], dict[str, dict], str]:
    own_db = db is None
    if own_db:
        db = Neo4j()

    try:
        current_ranked, _, _ = graph_rag_query_for_bill(bill_pk, db=db)
        if not current_ranked:
            return [], {}, ""

        seed_query = _build_related_seed_query(current_ranked, max_chunks=seed_chunk_count)
        if not seed_query:
            return [], {}, ""

        ranked, meta, _ = graph_rag_query(seed_query, top=top, db=db)
        target_bill_pk = str(bill_pk)

        # Keep one top chunk per related bill, excluding the current bill.
        seen_related_bills: set[str] = set()
        related_ranked: list[dict] = []
        for row in ranked:
            row_meta = meta.get(row["chunk_id"], {})
            row_bill_pk = str(row_meta.get("bill_pk", ""))
            if (
                not row_bill_pk
                or row_bill_pk == target_bill_pk
                or row_bill_pk in seen_related_bills
            ):
                continue
            seen_related_bills.add(row_bill_pk)
            related_ranked.append(row)
            if len(related_ranked) >= max_related_bills:
                break

        if not related_ranked:
            return [], meta, ""

        context = build_context(related_ranked, meta)
        return related_ranked, meta, context
    finally:
        if own_db:
            db.close()
