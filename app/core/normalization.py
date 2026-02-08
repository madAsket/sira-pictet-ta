from __future__ import annotations

import re

from app.core.utils import collapse_spaces

LEGAL_SUFFIXES = {
    "inc",
    "corporation",
    "corp",
    "ltd",
    "plc",
    "sa",
    "ag",
    "nv",
    "se",
    "spa",
}

LEGAL_SUFFIX_PATTERNS: tuple[tuple[str, ...], ...] = (
    ("inc",),
    ("corporation",),
    ("corp",),
    ("ltd",),
    ("plc",),
    ("sa",),
    ("s", "a"),
    ("ag",),
    ("nv",),
    ("n", "v"),
    ("se",),
    ("spa",),
    ("s", "p", "a"),
)


def normalize_match_text(value: str, *, remove_non_alnum: bool = True) -> str:
    normalized = value.casefold()
    normalized = normalized.replace("&", " and ")
    normalized = re.sub(r"[.,'\"()&/\\\-]", " ", normalized)
    if remove_non_alnum:
        normalized = re.sub(r"[^a-z0-9\s]", " ", normalized)
    return collapse_spaces(normalized)


def strip_legal_suffix_tokens(value: str) -> str:
    tokens = value.split(" ")
    while tokens:
        stripped = False
        if tokens[-1] in LEGAL_SUFFIXES:
            tokens.pop()
            stripped = True
        else:
            for pattern in LEGAL_SUFFIX_PATTERNS:
                if len(tokens) >= len(pattern) and tuple(tokens[-len(pattern) :]) == pattern:
                    del tokens[-len(pattern) :]
                    stripped = True
                    break
        if not stripped:
            break
    return " ".join(tokens).strip()


def normalize_company_name(
    value: str,
    *,
    remove_the: bool = True,
    remove_non_alnum: bool = True,
    strip_legal_suffixes: bool = True,
) -> str:
    normalized = normalize_match_text(value, remove_non_alnum=remove_non_alnum)
    if remove_the and normalized.startswith("the "):
        normalized = normalized[4:].strip()
    if strip_legal_suffixes and normalized:
        normalized = strip_legal_suffix_tokens(normalized)
    return normalized
