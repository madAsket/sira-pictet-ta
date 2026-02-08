from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Sequence

from openai import OpenAI
from qdrant_client import QdrantClient, models
from rapidfuzz import fuzz

from app.core.normalization import normalize_match_text
from app.core.settings import get_settings

def normalize_text(text: str) -> str:
    return normalize_match_text(text, remove_non_alnum=True)


@dataclass(frozen=True)
class RetrievedChunk:
    point_id: str
    score: float
    payload: dict[str, Any]


@dataclass(frozen=True)
class RAGRetrievalResult:
    query_text: str
    retrieved_chunks: list[RetrievedChunk]
    deduplicated_chunks: list[RetrievedChunk]
    context_snippets: list[dict[str, Any]]
    sources: list[dict[str, Any]]


def _embed_query(openai_client: OpenAI, embedding_model: str, query_text: str) -> list[float]:
    model_name = embedding_model.casefold() if embedding_model.casefold().startswith("text-embedding-") else embedding_model
    response = openai_client.embeddings.create(
        model=model_name,
        input=query_text,
    )
    return response.data[0].embedding


def _query_qdrant(
    qdrant_client: QdrantClient,
    collection_name: str,
    query_vector: Sequence[float],
    limit: int,
    query_filter: models.Filter | None = None,
) -> list[RetrievedChunk]:
    response = qdrant_client.query_points(
        collection_name=collection_name,
        query=list(query_vector),
        query_filter=query_filter,
        limit=limit,
        with_payload=True,
        with_vectors=False,
    )
    points = getattr(response, "points", []) or []
    output: list[RetrievedChunk] = []
    for point in points:
        payload = point.payload if isinstance(point.payload, dict) else {}
        output.append(
            RetrievedChunk(
                point_id=str(point.id),
                score=float(point.score),
                payload=payload,
            )
        )
    return output


def _coerce_optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed


def _coerce_optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def _build_quote_snippet(text: str, *, max_chars: int = 320) -> str:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", cleaned)
    snippet = " ".join(sentence for sentence in sentences[:2] if sentence.strip())
    if not snippet:
        snippet = cleaned
    return snippet[:max_chars].strip()


def build_rag_query_text(
    question: str,
    *,
    entities: Sequence[dict[str, Any]] | None = None,
) -> str:
    question_clean = re.sub(r"\s+", " ", question).strip()
    entities = list(entities or [])
    if not entities:
        return question_clean

    context_lines: list[str] = []
    for entity in entities:
        company_name = _coerce_optional_str(entity.get("company_name")) or "Unknown Company"
        ticker = _coerce_optional_str(entity.get("ticker")) or "null"
        isin = _coerce_optional_str(entity.get("isin")) or "null"
        context_lines.append(f"- {company_name} | {ticker} | {isin}")

    if not context_lines:
        return question_clean
    return (
        f"{question_clean}\n\n"
        "Entity context (canonical):\n"
        + "\n".join(context_lines)
    ).strip()


def deduplicate_retrieved_chunks(
    chunks: Sequence[RetrievedChunk],
    *,
    similarity_threshold: float = 0.95,
) -> list[RetrievedChunk]:
    ranked = sorted(chunks, key=lambda item: item.score, reverse=True)
    unique: list[RetrievedChunk] = []
    seen_doc_pages: set[tuple[str, int]] = set()
    normalized_seen_texts: list[str] = []

    for chunk in ranked:
        payload = chunk.payload if isinstance(chunk.payload, dict) else {}
        doc_id = _coerce_optional_str(payload.get("doc_id"))
        page = _coerce_optional_int(payload.get("page"))
        if doc_id is not None and page is not None:
            key = (doc_id, page)
            if key in seen_doc_pages:
                continue
        else:
            key = None

        text_for_similarity = _coerce_optional_str(payload.get("text")) or ""
        normalized_text = normalize_text(text_for_similarity)
        if normalized_text:
            duplicate_by_text = False
            for previous in normalized_seen_texts:
                similarity = fuzz.ratio(normalized_text, previous) / 100.0
                if similarity >= similarity_threshold:
                    duplicate_by_text = True
                    break
            if duplicate_by_text:
                continue
            normalized_seen_texts.append(normalized_text)

        unique.append(chunk)
        if key is not None:
            seen_doc_pages.add(key)

    return unique


def build_sources_from_chunks(
    chunks: Sequence[RetrievedChunk],
    *,
    max_sources: int = 3,
    min_score: float = 0.25,
) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    for chunk in chunks:
        if chunk.score < min_score:
            continue
        payload = chunk.payload if isinstance(chunk.payload, dict) else {}
        text = _coerce_optional_str(payload.get("text")) or ""
        quote_snippet = _coerce_optional_str(payload.get("quote_snippet")) or _build_quote_snippet(text)
        if quote_snippet:
            quote_snippet = _build_quote_snippet(quote_snippet, max_chars=320)
        source = {
            "title": _coerce_optional_str(payload.get("title")),
            "publisher": _coerce_optional_str(payload.get("publisher")),
            "year": _coerce_optional_int(payload.get("year")),
            "page": _coerce_optional_int(payload.get("page")),
            "quote_snippet": quote_snippet,
        }
        sources.append(source)
        if len(sources) >= max_sources:
            break
    return sources


def build_context_snippets_from_chunks(
    chunks: Sequence[RetrievedChunk],
    *,
    max_snippets: int = 5,
    min_score: float = 0.0,
    max_text_chars: int = 4000,
) -> list[dict[str, Any]]:
    snippets: list[dict[str, Any]] = []
    for chunk in chunks:
        if chunk.score < min_score:
            continue
        payload = chunk.payload if isinstance(chunk.payload, dict) else {}
        text = _coerce_optional_str(payload.get("text")) or ""
        if not text:
            continue
        compact_text = re.sub(r"\s+", " ", text).strip()
        snippets.append(
            {
                "doc_id": _coerce_optional_str(payload.get("doc_id")),
                "page": _coerce_optional_int(payload.get("page")),
                "title": _coerce_optional_str(payload.get("title")),
                "publisher": _coerce_optional_str(payload.get("publisher")),
                "year": _coerce_optional_int(payload.get("year")),
                "score": round(float(chunk.score), 6),
                "text": compact_text[:max_text_chars],
            }
        )
        if len(snippets) >= max_snippets:
            break
    return snippets


def retrieve_chunks_with_mentions_fallback(
    query_text: str,
    *,
    company_mentions: Sequence[str] | None = None,
    ticker_mentions: Sequence[str] | None = None,
    limit: int = 8,
    qdrant_url: str | None = None,
    collection_name: str | None = None,
    embedding_model: str | None = None,
    openai_client: OpenAI | None = None,
    qdrant_client: QdrantClient | None = None,
) -> list[RetrievedChunk]:
    settings = get_settings()
    qdrant_url = qdrant_url or settings.qdrant_url
    collection_name = collection_name or settings.qdrant_collection
    embedding_model = embedding_model or settings.openai_embedding_model

    if openai_client is None:
        api_key = settings.openai_api_key
        if not api_key:
            raise ValueError("OPENAI_API_KEY is not configured.")
        openai_client = OpenAI(api_key=api_key)

    if qdrant_client is None:
        qdrant_client = QdrantClient(url=qdrant_url)

    query_vector = _embed_query(
        openai_client=openai_client,
        embedding_model=embedding_model,
        query_text=query_text,
    )

    filtered_results: list[RetrievedChunk] = []
    normalized_mentions = sorted(
        {
            normalize_text(name)
            for name in (company_mentions or [])
            if normalize_text(name)
        }
    )
    if normalized_mentions:
        mention_filter = models.Filter(
            must=[
                models.FieldCondition(
                    key="mentions_company_names_norm",
                    match=models.MatchAny(any=normalized_mentions),
                )
            ]
        )
        filtered_results = _query_qdrant(
            qdrant_client=qdrant_client,
            collection_name=collection_name,
            query_vector=query_vector,
            limit=limit,
            query_filter=mention_filter,
        )

    if not filtered_results:
        normalized_tickers = sorted(
            {
                ticker.strip().upper()
                for ticker in (ticker_mentions or [])
                if ticker and ticker.strip()
            }
        )
        if normalized_tickers:
            ticker_filter = models.Filter(
                must=[
                    models.FieldCondition(
                        key="mentions_tickers",
                        match=models.MatchAny(any=normalized_tickers),
                    )
                ]
            )
            filtered_results = _query_qdrant(
                qdrant_client=qdrant_client,
                collection_name=collection_name,
                query_vector=query_vector,
                limit=limit,
                query_filter=ticker_filter,
            )

    if len(filtered_results) >= limit:
        return filtered_results[:limit]

    fallback_results = _query_qdrant(
        qdrant_client=qdrant_client,
        collection_name=collection_name,
        query_vector=query_vector,
        limit=limit,
        query_filter=None,
    )

    merged: list[RetrievedChunk] = []
    seen_ids: set[str] = set()
    for result in [*filtered_results, *fallback_results]:
        if result.point_id in seen_ids:
            continue
        seen_ids.add(result.point_id)
        merged.append(result)
        if len(merged) >= limit:
            break
    return merged


def retrieve_rag_context(
    question: str,
    *,
    entities: Sequence[dict[str, Any]] | None = None,
    qdrant_url: str | None = None,
    collection_name: str | None = None,
    embedding_model: str | None = None,
    openai_client: OpenAI | None = None,
    qdrant_client: QdrantClient | None = None,
) -> RAGRetrievalResult:
    settings = get_settings()
    top_k = settings.rag_top_k
    max_sources = settings.rag_max_sources
    dedup_similarity_threshold = settings.rag_dedup_similarity_threshold
    min_score = settings.rag_min_score
    context_max_snippets = settings.rag_context_max_snippets
    context_max_chars = settings.rag_context_max_chars

    entity_list = list(entities or [])
    query_text = build_rag_query_text(question, entities=entity_list)
    company_mentions = [
        value
        for value in (_coerce_optional_str(item.get("company_name")) for item in entity_list)
        if value
    ]
    ticker_mentions = [
        value
        for value in (_coerce_optional_str(item.get("ticker")) for item in entity_list)
        if value
    ]

    retrieved = retrieve_chunks_with_mentions_fallback(
        query_text,
        company_mentions=company_mentions,
        ticker_mentions=ticker_mentions,
        limit=top_k,
        qdrant_url=qdrant_url,
        collection_name=collection_name,
        embedding_model=embedding_model,
        openai_client=openai_client,
        qdrant_client=qdrant_client,
    )
    deduplicated = deduplicate_retrieved_chunks(
        retrieved,
        similarity_threshold=dedup_similarity_threshold,
    )
    sources = build_sources_from_chunks(
        deduplicated,
        max_sources=max_sources,
        min_score=min_score,
    )
    context_snippets = build_context_snippets_from_chunks(
        deduplicated,
        max_snippets=context_max_snippets,
        min_score=0.0,
        max_text_chars=context_max_chars,
    )
    return RAGRetrievalResult(
        query_text=query_text,
        retrieved_chunks=retrieved,
        deduplicated_chunks=deduplicated,
        context_snippets=context_snippets,
        sources=sources,
    )
