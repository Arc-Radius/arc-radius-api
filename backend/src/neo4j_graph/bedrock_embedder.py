"""Adapter that satisfies the neo4j-graphrag embedder protocol."""

from __future__ import annotations

from src.bedrock.bedrock_client import embed_query


class BedrockEmbedder:
    def embed_query(self, text: str) -> list[float]:
        return embed_query(text)
