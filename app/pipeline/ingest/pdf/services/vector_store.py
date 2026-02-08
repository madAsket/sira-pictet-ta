from __future__ import annotations

from typing import Sequence

from openai import OpenAI
from qdrant_client import QdrantClient, models

from app.pipeline.ingest.pdf.models import ChunkRecord, DocumentMetadata


def embed_texts(
    openai_client: OpenAI,
    embedding_model: str,
    texts: Sequence[str],
) -> list[list[float]]:
    if not texts:
        return []
    try:
        response = openai_client.embeddings.create(
            model=embedding_model,
            input=list(texts),
        )
    except Exception as exc:
        raise RuntimeError(f"Failed to create embeddings: {exc}") from exc
    return [item.embedding for item in response.data]


def get_collection_vector_size(collection_info: models.CollectionInfo) -> int | None:
    vectors = getattr(collection_info.config.params, "vectors", None)
    if vectors is None:
        return None
    if hasattr(vectors, "size") and getattr(vectors, "size") is not None:
        return int(vectors.size)
    if isinstance(vectors, dict):
        first_value = next(iter(vectors.values()), None)
        if first_value is not None and hasattr(first_value, "size"):
            size = getattr(first_value, "size")
            return int(size) if size is not None else None
    return None


def ensure_qdrant_collection(
    qdrant_client: QdrantClient,
    collection_name: str,
    vector_size: int,
) -> None:
    if qdrant_client.collection_exists(collection_name):
        info = qdrant_client.get_collection(collection_name)
        current_size = get_collection_vector_size(info)
        if current_size is not None and current_size != vector_size:
            raise ValueError(
                f"Collection '{collection_name}' vector size mismatch: "
                f"existing={current_size}, required={vector_size}. "
                "Run `make clear-vector-db-recreate VECTOR_SIZE=<required_size>`."
            )
        return
    qdrant_client.create_collection(
        collection_name=collection_name,
        vectors_config=models.VectorParams(size=vector_size, distance=models.Distance.COSINE),
    )


def chunk_records_to_points(
    chunk_records: Sequence[ChunkRecord],
    vectors: Sequence[Sequence[float]],
) -> list[models.PointStruct]:
    points: list[models.PointStruct] = []
    for record, vector in zip(chunk_records, vectors, strict=True):
        payload = {
            "doc_id": record.doc_id,
            "page": record.page,
            "chunk_index": record.chunk_index,
            "text": record.text,
            "quote_snippet": record.quote_snippet,
            "token_count": record.token_count,
            "mentions_company_names": record.mentions_company_names,
            "mentions_company_names_norm": record.mentions_company_names_norm,
            "mentions_tickers": record.mentions_tickers,
        }
        points.append(models.PointStruct(id=record.point_id, vector=list(vector), payload=payload))
    return points


def enrich_chunk_payload(
    points: list[models.PointStruct],
    metadata: DocumentMetadata,
) -> None:
    for point in points:
        payload = point.payload if isinstance(point.payload, dict) else {}
        payload["title"] = metadata.title
        payload["publisher"] = metadata.publisher
        payload["year"] = metadata.year
        payload["meta_source"] = metadata.meta_source
        point.payload = payload


def upload_points_in_batches(
    qdrant_client: QdrantClient,
    collection_name: str,
    points: Sequence[models.PointStruct],
    batch_size: int,
) -> None:
    for start in range(0, len(points), batch_size):
        batch = points[start : start + batch_size]
        qdrant_client.upsert(
            collection_name=collection_name,
            points=batch,
            wait=True,
        )


def normalize_embedding_model_name(model_name: str) -> str:
    normalized = model_name.strip()
    if normalized.casefold().startswith("text-embedding-"):
        return normalized.casefold()
    return normalized
