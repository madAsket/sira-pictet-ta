from __future__ import annotations

import re
import sqlite3

from app.core.normalization import normalize_match_text
from app.core.utils import collapse_spaces
from app.pipeline.ingest.pdf.models import MentionCatalog


def normalize_text(text: str) -> str:
    return normalize_match_text(text, remove_non_alnum=True)


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?;",
        (table_name,),
    ).fetchone()
    return row is not None


def load_mention_catalog(connection: sqlite3.Connection) -> MentionCatalog:
    aliases: dict[tuple[str, str], None] = {}
    if table_exists(connection, "company_aliases"):
        rows = connection.execute(
            """
            SELECT alias_normalized, company_name
            FROM company_aliases
            WHERE alias_normalized IS NOT NULL
              AND company_name IS NOT NULL;
            """
        ).fetchall()
        for alias_normalized, company_name in rows:
            alias = collapse_spaces(str(alias_normalized))
            company = collapse_spaces(str(company_name))
            if not alias or not company:
                continue
            token_count = len(alias.split(" "))
            if token_count == 1 and len(alias) < 4:
                continue
            aliases[(alias, company)] = None

    if not aliases and table_exists(connection, "equities"):
        rows = connection.execute(
            """
            SELECT company_name
            FROM equities
            WHERE company_name IS NOT NULL
              AND company_name <> '';
            """
        ).fetchall()
        for (company_name,) in rows:
            company = collapse_spaces(str(company_name))
            alias = normalize_text(company)
            if not alias:
                continue
            token_count = len(alias.split(" "))
            if token_count == 1 and len(alias) < 4:
                continue
            aliases[(alias, company)] = None

    sorted_aliases = sorted(aliases.keys(), key=lambda item: len(item[0]), reverse=True)

    ticker_patterns: dict[str, re.Pattern[str]] = {}
    if table_exists(connection, "equities"):
        rows = connection.execute(
            """
            SELECT ticker
            FROM equities
            WHERE ticker IS NOT NULL
              AND ticker <> '';
            """
        ).fetchall()
        for (ticker_raw,) in rows:
            ticker = collapse_spaces(str(ticker_raw)).upper()
            if len(ticker) < 2:
                continue
            pattern = re.compile(rf"(?<![A-Z0-9]){re.escape(ticker)}(?![A-Z0-9])")
            ticker_patterns.setdefault(ticker, pattern)

    return MentionCatalog(
        aliases=tuple(sorted_aliases),
        ticker_patterns=tuple(ticker_patterns.items()),
    )


def detect_mentions(chunk_text: str, catalog: MentionCatalog) -> tuple[list[str], list[str], list[str]]:
    if not chunk_text:
        return [], [], []

    normalized_chunk = f" {normalize_text(chunk_text)} "
    mentions_company_names: set[str] = set()

    for alias, company_name in catalog.aliases:
        if f" {alias} " in normalized_chunk:
            mentions_company_names.add(company_name)

    mentions_company_names_sorted = sorted(mentions_company_names)
    mentions_company_names_norm = sorted(
        {
            normalize_text(name)
            for name in mentions_company_names_sorted
            if normalize_text(name)
        }
    )

    uppercase_chunk = chunk_text.upper()
    mentions_tickers = sorted(
        ticker
        for ticker, pattern in catalog.ticker_patterns
        if pattern.search(uppercase_chunk)
    )

    return mentions_company_names_sorted, mentions_company_names_norm, mentions_tickers
