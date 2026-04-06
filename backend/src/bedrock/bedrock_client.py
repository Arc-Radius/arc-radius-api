"""
Bedrock client — single source for embeddings AND text generation.

Uses lru_cache for the boto3 client (equivalent to module scope,
but lazy — only created on first call).
"""

import json
import os
from functools import lru_cache

import boto3
from botocore.config import Config

DEFAULT_REGION = "us-east-1"
MAX_EMBED_INPUT_CHARS = 8000

DEFAULT_TEXT_MODEL_ID = os.getenv(
    "BEDROCK_TEXT_MODEL_ID",
    "global.amazon.nova-2-lite-v1:0",
)

# set BEDROCK_TEXT_REQUEST_FORMAT to "nova" or "anthropic".
BEDROCK_TEXT_REQUEST_FORMAT = os.getenv("BEDROCK_TEXT_REQUEST_FORMAT", "").strip().lower()
DEFAULT_EMBED_MODEL_ID = os.getenv(
    "BEDROCK_EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0"
)


@lru_cache()
def get_bedrock_client(region: str = DEFAULT_REGION):
    """Singleton boto3 Bedrock client — created once, reused forever."""
    # Nova extended thinking (high effort) can run long; default SDK read timeout is too low.
    return boto3.client(
        "bedrock-runtime",
        region_name=region,
        config=Config(read_timeout=3600, connect_timeout=60),
    )


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


def _use_anthropic_invoke_body(model_id: str) -> bool:
    """Route InvokeModel JSON body: Anthropic Messages vs Bedrock messages-v1 (Nova, etc.)."""
    if BEDROCK_TEXT_REQUEST_FORMAT == "anthropic":
        return True
    if BEDROCK_TEXT_REQUEST_FORMAT in ("nova", "messages-v1", "amazon"):
        return False
    mid = model_id.lower()
    if "anthropic" in mid or "claude" in mid:
        return True
    return False


# ─── Text Generation ─────────────────────────────────────


def generate(
    prompt: str,
    system: str = "",
    model_id: str = DEFAULT_TEXT_MODEL_ID,
    max_tokens: int = 1024,
    temperature: float = 0.3,
) -> str:
    """
    Text generation on Bedrock: Nova 2 Lite (messages-v1) or Anthropic Messages API.
    Used by RAG answer generation and letter/flyer generation.
    `max_tokens` applies only to the Anthropic path.
    """
    client = get_bedrock_client()

    if not _use_anthropic_invoke_body(model_id):
        # Nova 2 Lite extended thinking: maxTokens, temperature, topP, topK must be unset.
        body: dict = {
            "messages": [
                {"role": "user", "content": [{"text": prompt}]},
            ],
            "reasoningConfig": {
                "type": "enabled",
                "maxReasoningEffort": "low",
            },
        }
        if system:
            body["system"] = [{"text": system}]
        response = client.invoke_model(
            modelId=model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(body),
        )
        result = json.loads(response["body"].read())
        content_list = result.get("output", {}).get("message", {}).get("content", [])
        # Nova may emit multiple text blocks (preamble + JSON, or split output). Using only
        # the first block breaks JSON-only tasks like bill_related.
        parts: list[str] = []
        for item in content_list:
            if not isinstance(item, dict) or "text" not in item:
                continue
            chunk = item["text"]
            if isinstance(chunk, str):
                parts.append(chunk)
        if not parts:
            raise RuntimeError(
                f"Bedrock messages-v1 response missing text for {model_id}; "
                f"keys={list(result.keys())}"
            )
        return "".join(parts).strip()

    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [{"role": "user", "content": prompt}],
    }

    if system:
        body["system"] = system

    response = client.invoke_model(
        modelId=model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(body),
    )
    result = json.loads(response["body"].read())
    return result["content"][0]["text"]
