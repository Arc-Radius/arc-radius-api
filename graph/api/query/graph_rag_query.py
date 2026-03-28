"""GraphRAG retrieval query pipeline built on neo4j-graphrag retrievers."""

'''
    This file might be outdated, check the backend/src/neo4j_graph/graph_rag_query.py file for the latest version.
'''


from __future__ import annotations
import logging


logger = logging.getLogger(__name__)

DEFAULT_RETRIEVER_MODE = "vector"
VECTOR_INDEX_NAME = "chunkEmbeddingIndex"
FULLTEXT_INDEX_NAME = "chunkTextIndex"
EFFECTIVE_SEARCH_RATIO = 4
HYBRID_RANKER = "naive"

# Large expansion retrieval
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

# Related-bills retrieval should stay topical: do not fan out to other states or topics or sessions
RELATED_RETRIEVAL_CYPHER = """
WITH node, score
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
       c.chunk_index AS chunk_index,
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
       coalesce(c.text, "") AS text,
       c.chunk_index AS chunk_index
ORDER BY c.chunk_index ASC
"""

BILL_SEED_META_CYPHER = """
MATCH (b:Bill {bill_pk: $bill_pk})
RETURN coalesce(b.title, "") AS title,
       coalesce(b.description, "") AS description
LIMIT 1
"""

BILL_SEMANTIC_ANCHOR_CHUNKS_CYPHER = """
MATCH (b:Bill {bill_pk: $bill_pk})-[:HAS_DOCUMENT]->(:Document)-[:HAS_CHUNK]->(c:Chunk)
WHERE c.embedding IS NOT NULL
WITH c, vector.similarity.cosine(c.embedding, $query_embedding) AS score
RETURN c.chunk_id AS chunk_id,
       coalesce(c.text, "") AS text,
       c.chunk_index AS chunk_index,
       score
ORDER BY score DESC
LIMIT $top_k
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

# general vector retrieval with expansion


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

# related bills retrieval with no expansion


def _run_related_vector_retrieval(db: Neo4j, query: str, top: int) -> list[dict]:
    retriever = VectorCypherRetriever(
        db.driver,
        VECTOR_INDEX_NAME,
        retrieval_query=RELATED_RETRIEVAL_CYPHER,
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
    mode = os.getenv("RAG_RETRIEVER_MODE",
                     DEFAULT_RETRIEVER_MODE).strip().lower()
    if mode == "hybrid":
        try:
            return _run_hybrid_retrieval(db, query, top)
        except Exception:
            logger.exception(
                "Hybrid retrieval failed, falling back to vector retriever")
            return _run_vector_retrieval(db, query, top)
    if mode != "vector":
        logger.warning(
            "Unknown RAG_RETRIEVER_MODE=%r, using vector retriever", mode)
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


def _build_seed_query_from_bill_meta(meta: dict[str, str]) -> str:
    title = (meta.get("title") or "").strip()
    description = (meta.get("description") or "").strip()
    if not title and not description:
        return ""
    parts: list[str] = []
    if title:
        parts.append(f"Bill title: {title}")
    if description:
        parts.append(f"Bill description: {description}")
    return "\n".join(parts)


def _select_related_chunks_by_bill(
    ranked: list[dict],
    meta: dict[str, dict],
    *,
    target_bill_pk: str,
    max_chunks_per_related_bill: int,
    max_total_chunks: int,
) -> list[dict]:
    if not ranked or max_total_chunks <= 0 or max_chunks_per_related_bill <= 0:
        return []

    rows_by_bill: dict[str, list[dict]] = {}
    for row in ranked:
        row_meta = meta.get(row["chunk_id"], {})
        row_bill_pk = str(row_meta.get("bill_pk", ""))
        if not row_bill_pk or row_bill_pk == target_bill_pk:
            continue
        rows_by_bill.setdefault(row_bill_pk, []).append(row)

    if not rows_by_bill:
        return []

    # Sort chunks in each bill by relevance so we can take top M.
    for bill_rows in rows_by_bill.values():
        bill_rows.sort(
            key=lambda r: (-float(r.get("score", 0.0)), str(r.get("chunk_id", ""))))

    # How many distinct bills we can include while honoring chunk caps.
    max_related_bills = max(
        1,
        (max_total_chunks + max_chunks_per_related_bill -
         1) // max_chunks_per_related_bill,
    )

    # Rank bills by their best chunk score.
    bill_rank = sorted(
        rows_by_bill.items(),
        key=lambda item: (
            -float(item[1][0].get("score", 0.0)),
            item[0],
        ),
    )
    selected_bill_pks = [bill_pk for bill_pk,
                         _ in bill_rank[:max_related_bills]]

    related_ranked: list[dict] = []
    for bill_pk in selected_bill_pks:
        for row in rows_by_bill[bill_pk][:max_chunks_per_related_bill]:
            related_ranked.append(row)
            if len(related_ranked) >= max_total_chunks:
                return related_ranked
    return related_ranked


def graph_related_bills_query_for_bill(
    bill_pk: str,
    *,
    seed_query_max_chars: int = 10000,
    anchor_top: int = 3,
    top: int = 30,
    max_chunks_per_related_bill: int = 2,
    max_total_chunks: int = 12,
    db: Neo4j | None = None,
) -> tuple[list[dict], dict[str, dict], str]:
    own_db = db is None
    if own_db:
        db = Neo4j()

    try:
        # Prefer semantic anchor chunks from the current bill as retrieval seed context.
        _, _, anchor_context = graph_semantic_anchor_chunks_for_bill(
            bill_pk,
            top=anchor_top,
            seed_query_max_chars=seed_query_max_chars,
            db=db,
        )
        seed_query = (anchor_context or "")[: max(seed_query_max_chars, 1)]

        # Fallback to title/description seed if no anchor chunks are available.
        if not seed_query:
            bill_meta_rows = db.run(BILL_SEED_META_CYPHER, bill_pk=bill_pk)
            bill_meta = bill_meta_rows[0] if bill_meta_rows else {}
            seed_query = _build_seed_query_from_bill_meta(
                bill_meta)[: max(seed_query_max_chars, 1)]
        if not seed_query:
            return [], {}, ""

        ranked = _run_related_vector_retrieval(db, seed_query, top)
        if not ranked:
            return [], {}, ""

        candidate_chunk_ids = [r["chunk_id"] for r in ranked]
        meta_rows = db.run(METADATA_CYPHER, chunk_ids=candidate_chunk_ids)
        meta = {m["chunk_id"]: m for m in meta_rows}
        related_ranked = _select_related_chunks_by_bill(
            ranked,
            meta,
            target_bill_pk=str(bill_pk),
            max_chunks_per_related_bill=max_chunks_per_related_bill,
            max_total_chunks=max_total_chunks,
        )

        if not related_ranked:
            return [], {}, ""

        # Rebuild metadata for the final filtered chunk set.
        chunk_ids = [r["chunk_id"] for r in related_ranked]
        meta_rows = db.run(METADATA_CYPHER, chunk_ids=chunk_ids)
        meta = {m["chunk_id"]: m for m in meta_rows}
        context = build_context(related_ranked, meta)
        return related_ranked, meta, context
    finally:
        if own_db:
            db.close()


def graph_semantic_anchor_chunks_for_bill(
    bill_pk: str,
    *,
    top: int = 2,
    seed_query_max_chars: int = 2500,
    db: Neo4j | None = None,
) -> tuple[list[dict], dict[str, dict], str]:
    own_db = db is None
    if own_db:
        db = Neo4j()

    try:
        bill_meta_rows = db.run(BILL_SEED_META_CYPHER, bill_pk=bill_pk)
        bill_meta = bill_meta_rows[0] if bill_meta_rows else {}
        seed_query = _build_seed_query_from_bill_meta(
            bill_meta)[: max(seed_query_max_chars, 1)]
        if not seed_query:
            return [], {}, ""

        query_embedding = BedrockEmbedder().embed_query(seed_query)
        ranked = db.run(
            BILL_SEMANTIC_ANCHOR_CHUNKS_CYPHER,
            bill_pk=bill_pk,
            query_embedding=query_embedding,
            top_k=max(top, 1),
        )
        if not ranked:
            return [], {}, ""

        for row in ranked:
            row["score"] = float(row.get("score", 0.0))

        chunk_ids = [r["chunk_id"] for r in ranked]
        meta_rows = db.run(METADATA_CYPHER, chunk_ids=chunk_ids)
        meta = {m["chunk_id"]: m for m in meta_rows}
        context = build_context(ranked, meta)
        return ranked, meta, context
    finally:
        if own_db:
            db.close()
