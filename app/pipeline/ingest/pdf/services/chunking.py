from __future__ import annotations

import re
import uuid
from typing import Sequence

import tiktoken
from rapidfuzz import fuzz

from app.core.utils import collapse_spaces


def get_tokenizer(model_name: str):
    normalized = model_name.strip().lower()
    try:
        return tiktoken.encoding_for_model(normalized)
    except Exception:
        return tiktoken.get_encoding("cl100k_base")


def split_into_token_chunks(
    text: str,
    encoding,
    chunk_size: int,
    overlap_ratio: float,
) -> list[tuple[str, int]]:
    token_ids = encoding.encode(text)
    if not token_ids:
        return []

    overlap_tokens = int(round(chunk_size * overlap_ratio))
    overlap_tokens = max(0, min(overlap_tokens, chunk_size - 1))
    step = max(1, chunk_size - overlap_tokens)

    chunks: list[tuple[str, int]] = []
    start = 0
    while start < len(token_ids):
        end = min(start + chunk_size, len(token_ids))
        token_slice = token_ids[start:end]
        chunk_text = collapse_spaces(encoding.decode(token_slice))
        if chunk_text:
            chunks.append((chunk_text, len(token_slice)))
        if end >= len(token_ids):
            break
        start += step
    return chunks


def deduplicate_chunks(
    chunks: Sequence[tuple[str, int]],
    similarity_threshold: float,
) -> list[tuple[str, int]]:
    unique: list[tuple[str, int]] = []
    normalized_existing: list[str] = []

    for text, token_count in chunks:
        normalized = collapse_spaces(text.casefold())
        if not normalized:
            continue

        is_duplicate = False
        for previous in normalized_existing:
            similarity = fuzz.ratio(normalized, previous) / 100.0
            if similarity >= similarity_threshold:
                is_duplicate = True
                break

        if is_duplicate:
            continue

        unique.append((text, token_count))
        normalized_existing.append(normalized)

    return unique


def build_quote_snippet(chunk_text: str, max_chars: int = 350) -> str:
    text = collapse_spaces(chunk_text)
    sentences = re.split(r"(?<=[.!?])\s+", text)
    snippet = " ".join(sentence for sentence in sentences[:2] if sentence)
    if not snippet:
        snippet = text
    return snippet[:max_chars].strip()


def point_id_from_chunk(doc_id: str, page: int, chunk_index: int, text: str) -> str:
    seed = f"{doc_id}|{page}|{chunk_index}|{text}"
    return str(uuid.uuid5(uuid.NAMESPACE_URL, seed))
