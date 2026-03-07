from __future__ import annotations

from graph.src.embed import embed_query

# Adapter that satisfies neo4j-graphrag embedder protocol.
class BedrockEmbedder:
    def embed_query(self, text: str) -> list[float]:
        return embed_query(text)
