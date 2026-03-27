import json
import os
from functools import lru_cache

import boto3

MODEL_ID = os.getenv("BEDROCK_EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0")
RAW_DIMS = os.getenv("BEDROCK_EMBED_DIMS")
RAW_NORMALIZE = os.getenv("BEDROCK_EMBED_NORMALIZE")

DIMS = int(RAW_DIMS) if RAW_DIMS else None
NORMALIZE = None if RAW_NORMALIZE is None else RAW_NORMALIZE.lower() == "true"

MAX_EMBED_INPUT_CHARS = 8000
DEFAULT_REGION = "us-east-1"


@lru_cache()
def _get_client(region: str = DEFAULT_REGION):
    return boto3.client("bedrock-runtime", region_name=region)


def _embed_one(
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

    client = _get_client()
    response = client.invoke_model(
        modelId=model_id or MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(payload),
    )
    result = json.loads(response["body"].read())
    embedding = result.get("embedding")
    if not isinstance(embedding, list):
        raise RuntimeError(f"Bedrock embed response missing vector for {model_id}")
    return embedding


def embed_texts(texts: list[str]) -> list[list[float]]:
    return [
        _embed_one(text, model_id=MODEL_ID, dimensions=DIMS, normalize=NORMALIZE)
        for text in texts
    ]


def embed_query(q: str) -> list[float]:
    return embed_texts([q])[0]
