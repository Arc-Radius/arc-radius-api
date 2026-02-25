from graph.api.neo4j_client import Neo4j
from graph.api.extract_text import extract_elements, local_bill_path
from graph.api.chunking import make_chunks
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


BILL_TEXT_DIR = (
    Path(__file__).resolve().parents[1]
    / ".."
    / "datasources"
    / "legiscan-bill-text"
    / "bill-text"
)

# fetch documents that have no chunks
FETCH_QUERY = """
MATCH (b:Bill)-[:HAS_DOCUMENT]->(d:Document)
WHERE NOT (d)-[:HAS_CHUNK]->(:Chunk)
  AND d.skip IS NULL
RETURN d.document_id AS document_id,
       d.url AS url,
       d.document_type AS document_type,
       b.state AS state,
       b.bill_id AS bill_id,
       b.session_id AS session_id,
       b.bill_number AS bill_number
LIMIT $limit
"""

# create chunks for documents
CHUNK_CYPHER = """
UNWIND $rows AS row
MATCH (d:Document {document_id: row.document_id})
MERGE (c:Chunk {chunk_id: row.chunk_id})
SET c.chunk_index = row.chunk_index,
    c.section_path = row.heading,
    c.text = row.text
MERGE (d)-[:HAS_CHUNK]->(c)
"""


def main(limit: int = 100, max_docs: int | None = None):
    db = Neo4j()
    processed = 0

    # loop until we've processed the max number of documents
    while True:
        fetch_limit = min(limit, max_docs - processed) if max_docs else limit
        # fetch documents that have no chunks
        docs = db.run(FETCH_QUERY, limit=fetch_limit)
        if not docs:
            break

        # loop through documents
        for d in docs:
            path = local_bill_path(
                d["state"], str(d["bill_id"]), str(d["session_id"]),
                d["bill_number"], str(d["document_id"]),
                BILL_TEXT_DIR,
            )
            if not path:
                db.run(
                    "MATCH (d:Document {document_id: $did}) SET d.skip = true",
                    did=str(d["document_id"]),
                )
                print(f"[SKIP] no local file for doc {d['document_id']}")
                continue

            # extract text from documents
            try:
                elements = extract_elements(path)
            except Exception as e:
                print(f"[WARN] extract failed {d['document_id']}: {e}")
                continue

            # skip documents that have no text
            text = "\n".join(
                e.text for e in elements if getattr(e, "text", None))
            if not text or len(text) < 200:
                print(
                    f"[WARN] too little text for {d['document_id']} ({len(text)} chars)")
                db.run(
                    "MATCH (d:Document {document_id: $did}) SET d.extracted_at = datetime(), d.extracted_len = $n",
                    did=d["document_id"], n=len(text),
                )
                continue

            # create chunks for documents
            chunks = make_chunks(elements)
            doc_id = str(d["document_id"])

            # update document metadata
            db.run(
                "MATCH (d:Document {document_id: $did}) SET d.extracted_at = datetime(), d.extracted_len = $n",
                did=doc_id, n=len(text),
            )

            # create chunk rows
            chunk_rows = []
            for idx, ch in enumerate(chunks):
                chunk_rows.append({
                    "document_id": doc_id,
                    "chunk_id": f"{doc_id}:{idx}",
                    "chunk_index": idx,
                    "heading": ch["heading"],
                    "text": ch["text"],
                })

            # create chunks for documents
            for i in range(0, len(chunk_rows), 200):
                # add chunks to database
                db.run_batch(CHUNK_CYPHER, chunk_rows[i: i + 200])

            processed += 1
            # log progress
            print(f"[OK] {doc_id} ({path.suffix}): {len(chunks)} chunks")

            # break if we've processed the max number of documents
            if max_docs and processed >= max_docs:
                break

    # close database connection
    db.close()
    # log progress
    print(f"[DONE] processed {processed} documents")


if __name__ == "__main__":
    import argparse

    # parse command line argument for max number of documents
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-docs", type=int, default=None,
                        help="Cap total documents processed")
    args = parser.parse_args()
    # run main function with max number of documents
    main(max_docs=args.max_docs)
