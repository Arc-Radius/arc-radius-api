import json
import os
from functools import lru_cache

import boto3
from botocore.config import Config

DEFAULT_REGION = "us-east-1"
MAX_EMBED_INPUT_CHARS = 8000
DEFAULT_TEXT_MODEL_ID = (
    "arn:aws:bedrock:us-east-1:233894721797:inference-profile/"
    "global.anthropic.claude-sonnet-4-20250514-v1:0"
)
DEFAULT_EMBED_MODEL_ID = os.getenv("BEDROCK_EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0")


@lru_cache()  # singleton — reuse the client
def get_bedrock_client(region: str = DEFAULT_REGION):
    return boto3.client(
        "bedrock-runtime",
        region_name=region
    )


def embed_text(
    text: str,
    *,
    model_id: str | None = None,
    dimensions: int | None = None,
    normalize: bool | None = None,
) -> list[float]:
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
        raise RuntimeError(f"Bedrock embed response missing vector for {selected_model}")
    return embedding


def embed_texts(
    texts: list[str],
    *,
    model_id: str | None = None,
    dimensions: int | None = None,
    normalize: bool | None = None,
) -> list[list[float]]:
    return [
        embed_text(
            text,
            model_id=model_id,
            dimensions=dimensions,
            normalize=normalize,
        )
        for text in texts
    ]


def generate(
    prompt: str,
    system: str = "",
    model_id: str = DEFAULT_TEXT_MODEL_ID,
    max_tokens: int = 1024,
    temperature: float = 0.3,
):
    """
    Uses the Messages API format (current standard for Claude on Bedrock).
    NOT the old completion API with 'Human:/Assistant:' prompts.
    """
    client = get_bedrock_client()

    body = {
        "anthropic_version": "bedrock-2023-05-31",  # required for Messages API
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [
            {"role": "user", "content": prompt}
        ],
    }

    # System prompt goes at top level, not inside messages
    if system:
        body["system"] = system

    response = client.invoke_model(
        modelId=model_id,
        body=json.dumps(body),
    )
    result = json.loads(response["body"].read())
    return result["content"][0]["text"]
