from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from rapidfuzz import fuzz, process

from app.core.normalization import normalize_company_name as normalize_company_name_shared
from app.core.utils import collapse_spaces

LOGGER = logging.getLogger("entity_resolver")

MATCH_METHOD = Literal["isin_exact", "ticker_exact", "alias_exact", "fuzzy_name"]

ISIN_PATTERN = re.compile(r"(?<![A-Z0-9])([A-Z]{2}[A-Z0-9]{10})(?![A-Z0-9])")
TICKER_PATTERN = re.compile(r"(?<![A-Z0-9])([A-Z]{2,6}(?:\.[A-Z])?)(?![A-Z0-9])")

QUERY_SPLIT_PATTERN = re.compile(r"\b(?:and|or|vs|versus|with|against|compared to)\b|[,;:/]")

SINGLE_TOKEN_ALIAS_BLOCKLIST = {
    "what",
    "which",
    "who",
    "when",
    "where",
    "why",
    "how",
    "is",
    "are",
    "for",
    "about",
    "with",
    "vs",
    "versus",
    "compare",
    "comparing",
    "between",
    "and",
    "or",
    "to",
    "in",
    "of",
    "on",
    "target",
    "price",
    "yield",
    "dividend",
    "ratio",
    "market",
    "inflation",
    "growth",
    "stock",
    "stocks",
    "trend",
    "trends",
    "macro",
    "risk",
    "risks",
    "outlook",
    "company",
    "companies",
    "sector",
    "sectors",
    "valuation",
    "valuations",
    "current",
    "does",
    "do",
    "can",
    "should",
    "could",
    "would",
    "tell",
    "show",
    "give",
    "list",
}


@dataclass(frozen=True)
class CompanyRecord:
    isin: str
    company_name: str
    ticker: str | None


@dataclass(frozen=True)
class ResolvedEntity:
    isin: str
    company_name: str
    ticker: str | None
    confidence: float
    matched_by: MATCH_METHOD


@dataclass(frozen=True)
class RejectedCandidate:
    method: str
    candidate: str
    reason: str
    confidence: float
    isin: str | None = None
    company_name: str | None = None
    ticker: str | None = None


@dataclass(frozen=True)
class EntityResolutionResult:
    entities: list[ResolvedEntity]
    rejected_candidates: list[RejectedCandidate]

def normalize_company_name(value: str, *, remove_the: bool = True) -> str:
    return normalize_company_name_shared(
        value,
        remove_the=remove_the,
        remove_non_alnum=True,
        strip_legal_suffixes=True,
    )


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?;",
        (table_name,),
    ).fetchone()
    return row is not None


def _ticker_variants(raw_ticker: str) -> list[str]:
    cleaned = collapse_spaces(raw_ticker)
    if not cleaned:
        return []
    variants = {cleaned}
    first_token = cleaned.split(" ")[0]
    variants.add(first_token)
    first_token_no_suffix = first_token.split(".")[0]
    variants.add(first_token_no_suffix)
    return [value for value in variants if value]


class EntityResolver:
    def __init__(
        self,
        *,
        db_path: Path = Path("db/equities.db"),
        confidence_threshold: float = 0.80,
        fuzzy_min_score: int = 80,
        ambiguity_margin: float = 0.05,
        max_entities: int = 5,
    ) -> None:
        self.db_path = db_path
        self.confidence_threshold = confidence_threshold
        self.fuzzy_min_score = fuzzy_min_score
        self.ambiguity_margin = ambiguity_margin
        self.max_entities = max_entities

        self.by_isin: dict[str, CompanyRecord] = {}
        self.by_ticker: dict[str, list[CompanyRecord]] = {}
        self.alias_to_companies: dict[str, list[CompanyRecord]] = {}
        self.aliases_sorted_for_exact: list[str] = []
        self.aliases_for_fuzzy: list[str] = []

        self._load_catalog()

    def resolve(self, question: str) -> EntityResolutionResult:
        accepted: dict[str, ResolvedEntity] = {}
        rejected: list[RejectedCandidate] = []

        self._resolve_by_isin(question, accepted=accepted, rejected=rejected)
        self._resolve_by_ticker(question, accepted=accepted, rejected=rejected)
        self._resolve_by_alias_exact(question, accepted=accepted, rejected=rejected)
        self._resolve_by_fuzzy(question, accepted=accepted, rejected=rejected)

        entities = sorted(accepted.values(), key=lambda item: item.confidence, reverse=True)
        if len(entities) > self.max_entities:
            dropped = entities[self.max_entities :]
            entities = entities[: self.max_entities]
            for entity in dropped:
                rejected.append(
                    RejectedCandidate(
                        method="cap_limit",
                        candidate=entity.company_name,
                        reason=f"Dropped due to max_entities={self.max_entities}.",
                        confidence=entity.confidence,
                        isin=entity.isin,
                        company_name=entity.company_name,
                        ticker=entity.ticker,
                    )
                )
        return EntityResolutionResult(entities=entities, rejected_candidates=rejected)

    def _load_catalog(self) -> None:
        if not self.db_path.exists():
            raise FileNotFoundError(f"Equities DB not found: {self.db_path}")

        with sqlite3.connect(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT isin, company_name, ticker
                FROM equities
                WHERE isin IS NOT NULL
                  AND isin <> ''
                  AND company_name IS NOT NULL
                  AND company_name <> '';
                """
            ).fetchall()

            for isin_raw, company_name_raw, ticker_raw in rows:
                isin = str(isin_raw).strip().upper()
                company_name = collapse_spaces(str(company_name_raw))
                ticker = collapse_spaces(str(ticker_raw)) if ticker_raw else None
                if not isin or not company_name:
                    continue
                record = CompanyRecord(
                    isin=isin,
                    company_name=company_name,
                    ticker=ticker or None,
                )
                self.by_isin[isin] = record
                if ticker:
                    for ticker_variant in _ticker_variants(ticker):
                        self.by_ticker.setdefault(ticker_variant, []).append(record)
                base_alias = normalize_company_name(company_name)
                if base_alias:
                    self.alias_to_companies.setdefault(base_alias, []).append(record)

            if _table_exists(connection, "company_aliases"):
                alias_rows = connection.execute(
                    """
                    SELECT alias_normalized, isin
                    FROM company_aliases
                    WHERE alias_normalized IS NOT NULL
                      AND alias_normalized <> ''
                      AND isin IS NOT NULL
                      AND isin <> '';
                    """
                ).fetchall()
                for alias_raw, isin_raw in alias_rows:
                    alias = collapse_spaces(str(alias_raw))
                    isin = str(isin_raw).strip().upper()
                    record = self.by_isin.get(isin)
                    if not alias or record is None:
                        continue
                    self.alias_to_companies.setdefault(alias, []).append(record)

        for alias, records in list(self.alias_to_companies.items()):
            unique_by_isin = {item.isin: item for item in records}
            self.alias_to_companies[alias] = list(unique_by_isin.values())

        filtered_aliases = [
            alias
            for alias in self.alias_to_companies.keys()
            if not (len(alias.split(" ")) == 1 and alias in SINGLE_TOKEN_ALIAS_BLOCKLIST)
        ]

        self.aliases_sorted_for_exact = sorted(
            filtered_aliases,
            key=lambda alias: (len(alias.split(" ")), len(alias)),
            reverse=True,
        )
        self.aliases_for_fuzzy = list(filtered_aliases)
        LOGGER.info(
            "Loaded entity catalog: companies=%s aliases=%s tickers=%s",
            len(self.by_isin),
            len(self.alias_to_companies),
            len(self.by_ticker),
        )

    def _add_entity(
        self,
        *,
        accepted: dict[str, ResolvedEntity],
        record: CompanyRecord,
        confidence: float,
        matched_by: MATCH_METHOD,
    ) -> None:
        normalized_confidence = min(1.0, max(0.0, confidence))
        if normalized_confidence < self.confidence_threshold:
            return
        current = accepted.get(record.isin)
        new_entity = ResolvedEntity(
            isin=record.isin,
            company_name=record.company_name,
            ticker=record.ticker,
            confidence=normalized_confidence,
            matched_by=matched_by,
        )
        if (
            current is not None
            and current.matched_by in {"isin_exact", "ticker_exact", "alias_exact"}
            and matched_by == "fuzzy_name"
        ):
            return
        if current is None or new_entity.confidence > current.confidence:
            accepted[record.isin] = new_entity

    def _resolve_by_isin(
        self,
        question: str,
        *,
        accepted: dict[str, ResolvedEntity],
        rejected: list[RejectedCandidate],
    ) -> None:
        question_upper = question.upper()
        for match in ISIN_PATTERN.findall(question_upper):
            isin = match.strip().upper()
            company = self.by_isin.get(isin)
            if company is None:
                rejected.append(
                    RejectedCandidate(
                        method="isin_exact",
                        candidate=isin,
                        reason="ISIN not found in equities.",
                        confidence=0.0,
                    )
                )
                continue
            self._add_entity(
                accepted=accepted,
                record=company,
                confidence=1.0,
                matched_by="isin_exact",
            )

    def _resolve_by_ticker(
        self,
        question: str,
        *,
        accepted: dict[str, ResolvedEntity],
        rejected: list[RejectedCandidate],
    ) -> None:
        for ticker in TICKER_PATTERN.findall(question):
            symbol = ticker.strip()
            if len(symbol.replace(".", "")) < 2:
                continue
            candidates = self.by_ticker.get(symbol, [])
            if not candidates:
                continue
            if len(candidates) > 1:
                rejected.append(
                    RejectedCandidate(
                        method="ticker_exact",
                        candidate=symbol,
                        reason="Ticker matched multiple companies.",
                        confidence=0.0,
                    )
                )
                continue
            self._add_entity(
                accepted=accepted,
                record=candidates[0],
                confidence=0.99,
                matched_by="ticker_exact",
            )

    def _resolve_by_alias_exact(
        self,
        question: str,
        *,
        accepted: dict[str, ResolvedEntity],
        rejected: list[RejectedCandidate],
    ) -> None:
        normalized_question = f" {normalize_company_name(question, remove_the=False)} "
        for alias in self.aliases_sorted_for_exact:
            if not alias:
                continue
            if f" {alias} " not in normalized_question:
                continue
            candidates = self.alias_to_companies.get(alias, [])
            if len(candidates) == 1:
                self._add_entity(
                    accepted=accepted,
                    record=candidates[0],
                    confidence=0.90,
                    matched_by="alias_exact",
                )
            elif len(candidates) > 1:
                rejected.append(
                    RejectedCandidate(
                        method="alias_exact",
                        candidate=alias,
                        reason="Alias matched multiple companies.",
                        confidence=0.0,
                    )
                )

    def _resolve_by_fuzzy(
        self,
        question: str,
        *,
        accepted: dict[str, ResolvedEntity],
        rejected: list[RejectedCandidate],
    ) -> None:
        if not self.aliases_for_fuzzy:
            return

        normalized_question = normalize_company_name(question, remove_the=False)
        query_candidates = self._build_fuzzy_queries(
            normalized_question,
            include_full_question=not bool(accepted),
        )
        for query in query_candidates:
            fuzzy_candidates = process.extract(
                query,
                self.aliases_for_fuzzy,
                scorer=fuzz.token_set_ratio,
                limit=8,
            )
            if not fuzzy_candidates:
                continue

            scored_by_isin: dict[str, tuple[CompanyRecord, float, str]] = {}
            for alias, score, _ in fuzzy_candidates:
                if score < self.fuzzy_min_score:
                    continue
                for company in self.alias_to_companies.get(alias, []):
                    current = scored_by_isin.get(company.isin)
                    if current is None or score > current[1]:
                        scored_by_isin[company.isin] = (company, float(score), alias)

            if not scored_by_isin:
                continue

            ranked = sorted(scored_by_isin.values(), key=lambda item: item[1], reverse=True)
            top_company, top_score, top_alias = ranked[0]
            top_confidence = top_score / 100.0
            second_score = ranked[1][1] if len(ranked) > 1 else None

            if second_score is not None and (top_score - second_score) < (self.ambiguity_margin * 100):
                top_two_isins = {ranked[0][0].isin, ranked[1][0].isin}
                if top_two_isins.issubset(set(accepted.keys())):
                    continue
                rejected.append(
                    RejectedCandidate(
                        method="fuzzy_name",
                        candidate=query,
                        reason=(
                            f"Ambiguous fuzzy match: top1={top_score:.1f}, "
                            f"top2={second_score:.1f}, required_margin={self.ambiguity_margin:.2f}"
                        ),
                        confidence=top_confidence,
                        isin=top_company.isin,
                        company_name=top_company.company_name,
                        ticker=top_company.ticker,
                    )
                )
                continue

            if top_confidence < self.confidence_threshold:
                rejected.append(
                    RejectedCandidate(
                        method="fuzzy_name",
                        candidate=query,
                        reason=f"Fuzzy confidence below threshold {self.confidence_threshold:.2f}.",
                        confidence=top_confidence,
                        isin=top_company.isin,
                        company_name=top_company.company_name,
                        ticker=top_company.ticker,
                    )
                )
                continue

            self._add_entity(
                accepted=accepted,
                record=top_company,
                confidence=top_confidence,
                matched_by="fuzzy_name",
            )

    def _build_fuzzy_queries(
        self,
        normalized_question: str,
        *,
        include_full_question: bool = True,
    ) -> list[str]:
        if not normalized_question:
            return []
        queries = [normalized_question] if include_full_question else []
        for segment in QUERY_SPLIT_PATTERN.split(normalized_question):
            segment_clean = collapse_spaces(segment)
            if len(segment_clean) < 3:
                continue
            queries.append(segment_clean)
        unique: list[str] = []
        for query in queries:
            if query not in unique:
                unique.append(query)
        return unique[:10]
