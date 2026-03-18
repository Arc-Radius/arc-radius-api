import os

from bedrock.bedrock_client import embed_texts as bedrock_embed_texts

MODEL_ID = os.getenv("BEDROCK_EMBED_MODEL_ID", "amazon.titan-embed-text-v2:0")
RAW_DIMS = os.getenv("BEDROCK_EMBED_DIMS")
RAW_NORMALIZE = os.getenv("BEDROCK_EMBED_NORMALIZE")

DIMS = int(RAW_DIMS) if RAW_DIMS else None
NORMALIZE = None if RAW_NORMALIZE is None else RAW_NORMALIZE.lower() == "true"


def embed_texts(texts: list[str]) -> list[list[float]]:
    return bedrock_embed_texts(
        texts,
        model_id=MODEL_ID,
        dimensions=DIMS,
        normalize=NORMALIZE,
    )


def embed_query(q: str) -> list[float]:
    return embed_texts([q])[0]
