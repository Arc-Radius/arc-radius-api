CREATE CONSTRAINT bill_pk IF NOT EXISTS
FOR (b:Bill) REQUIRE b.bill_pk IS UNIQUE;

CREATE CONSTRAINT doc_id IF NOT EXISTS
FOR (d:Document) REQUIRE d.document_id IS UNIQUE;

CREATE CONSTRAINT chunk_id IF NOT EXISTS
FOR (c:Chunk) REQUIRE c.chunk_id IS UNIQUE;

CREATE CONSTRAINT citation_id IF NOT EXISTS
FOR (x:Citation) REQUIRE x.citation_id IS UNIQUE;

CREATE VECTOR INDEX chunkEmbeddingIndex IF NOT EXISTS
FOR (c:Chunk)
ON (c.embedding)
OPTIONS {
  indexConfig: {
    `vector.dimensions`: 768,
    `vector.similarity_function`: 'cosine'
  }
};
