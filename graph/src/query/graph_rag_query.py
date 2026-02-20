"""GraphRAG retrieval query pipeline: seed -> expand -> rerank."""

from __future__ import annotations

from src.query.formatting import build_context
from src.embed import embed_query
from src.neo4j_client import Neo4j

# seed query to find 20 most relevant chunks
SEED_CYPHER = """
CALL db.index.vector.queryNodes('chunkEmbeddingIndex', 20, $qvec)
YIELD node, score
RETURN node.chunk_id AS chunk_id, node.text AS text, score
ORDER BY score DESC
"""

# expand query: sibling chunks (same doc, +/-2 index) + topic-neighbor chunks
EXPAND_CYPHER = """
MATCH (seed:Chunk)
WHERE seed.chunk_id IN $seed_ids
MATCH (d:Document)-[:HAS_CHUNK]->(seed)
MATCH (b:Bill)-[:HAS_DOCUMENT]->(d)

OPTIONAL MATCH (d)-[:HAS_CHUNK]->(sib:Chunk)
WHERE abs(sib.chunk_index - seed.chunk_index) <= 2

WITH seed, b, collect(DISTINCT sib.chunk_id) AS siblings

OPTIONAL MATCH (b)-[:HAS_TOPIC]->(t:Topic)<-[:HAS_TOPIC]-(ob:Bill)
WHERE ob <> b
OPTIONAL MATCH (ob)-[:HAS_DOCUMENT]->(od:Document)-[:HAS_CHUNK]->(tc:Chunk)

WITH siblings, collect(DISTINCT tc.chunk_id)[..50] AS topic_chunks

RETURN siblings, topic_chunks
"""

# rerank query to find 20 most relevant chunks
RERANK_CYPHER = """
CALL db.index.vector.queryNodes('chunkEmbeddingIndex', 400, $qvec)
YIELD node, score
WHERE node.chunk_id IN $candidate_ids
RETURN node.chunk_id AS chunk_id, node.text AS text, score
ORDER BY score DESC
LIMIT 20
"""

# metadata query to get metadata for the chunks
METADATA_CYPHER = """
UNWIND $chunk_ids AS cid
MATCH (d:Document)-[:HAS_CHUNK]->(c:Chunk {chunk_id: cid})
MATCH (b:Bill)-[:HAS_DOCUMENT]->(d)

RETURN c.chunk_id AS chunk_id,
       c.section_path AS section_path,
       d.document_id AS document_id,
       d.url AS doc_url,
       d.document_desc AS doc_desc,
       b.bill_pk AS bill_pk,
       b.state AS state,
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

# graph RAG query pipeline
# run seed -> expand -> rerank pipeline
# return ranked chunks, metadata map, and context string
def graph_rag_query(
    query: str, *, top: int = 10, db: Neo4j | None = None
) -> tuple[list[dict], dict[str, dict], str]:
    own_db = db is None
    if own_db:
        db = Neo4j()

    try:
        qvec = embed_query(query)
        # run seed query to find 20 most relevant chunks
        seeds = db.run(SEED_CYPHER, qvec=qvec)
        seed_ids = [s["chunk_id"] for s in seeds]
        # run expand query to find sibling + topic-neighbor chunks
        exp = db.run(EXPAND_CYPHER, seed_ids=seed_ids)
        candidate_ids = set(seed_ids)
        for r in exp:
            candidate_ids.update(r.get("siblings") or [])
            candidate_ids.update(r.get("topic_chunks") or [])
        # run rerank query to find 20 most relevant chunks
        ranked = db.run(RERANK_CYPHER, qvec=qvec, candidate_ids=list(candidate_ids))
        # get top ranked chunks
        top_ranked = ranked[:top]
        chunk_ids = [r["chunk_id"] for r in top_ranked]
        meta_rows = db.run(METADATA_CYPHER, chunk_ids=chunk_ids)
        meta = {m["chunk_id"]: m for m in meta_rows}
        # use formatting module to build context string
        context = build_context(top_ranked, meta)
        return top_ranked, meta, context
    finally:
        if own_db:
            db.close()
