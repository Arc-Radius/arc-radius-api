"""
Bedrock client — single source for embeddings AND text generation.

Uses lru_cache for the boto3 client (equivalent to module scope,
but lazy — only created on first call).
"""

import json
import os
from functools import lru_cache

import boto3

DEFAULT_REGION = "us-east-1"
MAX_EMBED_INPUT_CHARS = 8000
DEFAULT_TEXT_MODEL_ID = (
    "arn:aws:bedrock:us-east-1:233894721797:inference-profile/"
    "global.anthropic.claude-sonnet-4-20250514-v1:0"
)
DEFAULT_EMBED_MODEL_ID = os.getenv(
    "BEDROCK_EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0"
)


@lru_cache()
def get_bedrock_client(region: str = DEFAULT_REGION):
    """Singleton boto3 Bedrock client — created once, reused forever."""
    return boto3.client("bedrock-runtime", region_name=region)


# ─── Embeddings ───────────────────────────────────────────


def embed_text(
    text: str,
    *,
    model_id: str | None = None,
    dimensions: int | None = None,
    normalize: bool | None = None,
) -> list[float]:
    """Embed a single text string. Returns vector (e.g. 1024-dim)."""
    if not text:
        return []

    payload: dict[str, object] = {"inputText": text[:MAX_EMBED_INPUT_CHARS]}
    if dimensions is not None:
        payload["dimensions"] = dimensions
    if normalize is not None:
        payload["normalize"] = normalize

    client = get_bedrock_client()
    selected_model = model_id or DEFAULT_EMBED_MODEL_ID

    response = client.invoke_model(
        modelId=selected_model,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(payload),
    )
    result = json.loads(response["body"].read())
    embedding = result.get("embedding")
    if not isinstance(embedding, list):
        raise RuntimeError(
            f"Bedrock embed response missing vector for {selected_model}"
        )
    return embedding


def embed_texts(
    texts: list[str],
    *,
    model_id: str | None = None,
    dimensions: int | None = None,
    normalize: bool | None = None,
) -> list[list[float]]:
    """Embed a batch of texts. Returns list of vectors."""
    return [
        embed_text(
            text,
            model_id=model_id,
            dimensions=dimensions,
            normalize=normalize,
        )
        for text in texts
    ]


def embed_query(q: str) -> list[float]:
    """Convenience: embed a single query string."""
    return embed_text(q)


# ─── Text Generation ─────────────────────────────────────


def generate(
    prompt: str,
    system: str = "",
    model_id: str = DEFAULT_TEXT_MODEL_ID,
    max_tokens: int = 1024,
    temperature: float = 0.3,
) -> str:
    """
    Call Claude on Bedrock via Messages API.
    Used by RAG answer generation and letter/flyer generation.
    """
    client = get_bedrock_client()

    body: dict = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [{"role": "user", "content": prompt}],
    }

    if system:
        body["system"] = system

    response = client.invoke_model(
        modelId=model_id,
        body=json.dumps(body),
    )
    result = json.loads(response["body"].read())
    return result["content"][0]["text"]
