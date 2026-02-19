from sentence_transformers import SentenceTransformer

_model = SentenceTransformer("sentence-transformers/all-mpnet-base-v2")


def embed_texts(texts: list[str]) -> list[list[float]]:
    return _model.encode(texts, normalize_embeddings=True, batch_size=32).tolist()


def embed_query(q: str) -> list[float]:
    return embed_texts([q])[0]
