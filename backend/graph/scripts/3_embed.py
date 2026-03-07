from dotenv import load_dotenv

load_dotenv()

from graph.api.embed import embed_texts
from graph.api.neo4j_client import Neo4j

BATCH_SIZE = 100

FETCH_QUERY = """
MATCH (c:Chunk)
WHERE c.embedding IS NULL
RETURN c.chunk_id AS chunk_id, c.text AS text
LIMIT $batch
"""

SET_EMBEDDING_CYPHER = """
UNWIND $rows AS row
MATCH (c:Chunk {chunk_id: row.chunk_id})
SET c.embedding = row.embedding
"""


def main(batch_size: int = BATCH_SIZE):
    db = Neo4j()
    total_embedded = 0

    while True:
        rows = db.run(FETCH_QUERY, batch=batch_size)
        if not rows:
            break

        texts = [r["text"] for r in rows]
        vecs = embed_texts(texts)

        embed_rows = [
            {"chunk_id": r["chunk_id"], "embedding": v}
            for r, v in zip(rows, vecs)
        ]

        db.run_batch(SET_EMBEDDING_CYPHER, embed_rows)

        total_embedded += len(rows)
        print(f"[OK] embedded {len(rows)} chunks ({total_embedded} total)")

    db.close()
    print(f"[DONE] {total_embedded} chunks embedded")


if __name__ == "__main__":
    main()
