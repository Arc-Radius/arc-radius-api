from dotenv import load_dotenv

load_dotenv()

from src.citations import extract_citations
from src.embed import embed_texts
from src.neo4j_client import Neo4j

BATCH_SIZE = 100

# fetch chunks that have no embedding
FETCH_QUERY = """
MATCH (c:Chunk)
WHERE c.embedding IS NULL
RETURN c.chunk_id AS chunk_id, c.text AS text
LIMIT $batch
"""

# set embedding for chunks
SET_EMBEDDING_CYPHER = """
UNWIND $rows AS row
MATCH (c:Chunk {chunk_id: row.chunk_id})
SET c.embedding = row.embedding
"""

# create citation links
CITE_CYPHER = """
UNWIND $rows AS row
MATCH (ch:Chunk {chunk_id: row.chunk_id})
MERGE (x:Citation {citation_id: row.citation_id})
SET x.jurisdiction = row.jurisdiction,
    x.canonical = row.canonical
MERGE (ch)-[r:CITES]->(x)
SET r.span_start = row.span_start,
    r.span_end = row.span_end,
    r.confidence = row.confidence
"""


def main(batch_size: int = BATCH_SIZE):
    db = Neo4j()
    # keep track of total embedded chunks and citation links
    total_embedded = 0
    total_cites = 0

    # loop until we've processed the max number of chunks
    while True:
        # fetch chunks that have no embedding
        rows = db.run(FETCH_QUERY, batch=batch_size)
        if not rows:
            break

        # extract text from chunks
        texts = [r["text"] for r in rows]
        # embed text
        vecs = embed_texts(texts)

        embed_rows = []
        cite_rows = []

        # loop through chunks and embeddings
        for r, v in zip(rows, vecs):
            chunk_id = r["chunk_id"]
            embed_rows.append({"chunk_id": chunk_id, "embedding": v})
            # extract citations from text
            for cite in extract_citations(r["text"]):
                citation_id = f"{cite['jurisdiction']}:{cite['canonical']}"
                cite_rows.append({
                    "chunk_id": chunk_id,
                    "citation_id": citation_id,
                    "jurisdiction": cite["jurisdiction"],
                    "canonical": cite["canonical"],
                    "span_start": cite["span_start"],
                    "span_end": cite["span_end"],
                    "confidence": cite["confidence"],
                })

        # set embedding for chunks
        db.run_batch(SET_EMBEDDING_CYPHER, embed_rows)
        # create citation links
        if cite_rows:
            # create citation links in batches
            for i in range(0, len(cite_rows), 200):
                db.run_batch(CITE_CYPHER, cite_rows[i : i + 200])

        # update total embedded chunks and citation links
        total_embedded += len(rows)
        total_cites += len(cite_rows)
        print(f"[OK] embedded {len(rows)} chunks ({total_embedded} total), {len(cite_rows)} citations")

    # close database connection
    db.close()
    # log progress
    print(f"[DONE] {total_embedded} chunks embedded, {total_cites} citation links created")


if __name__ == "__main__":
    main()
