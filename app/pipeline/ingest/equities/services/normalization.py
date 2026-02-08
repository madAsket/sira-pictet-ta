from __future__ import annotations

import html
import re
from pathlib import Path

import pandas as pd

from app.core.normalization import (
    normalize_company_name as normalize_company_name_shared,
    normalize_match_text,
)
from app.core.utils import collapse_spaces
from app.domain.equities.schema import COLUMN_SPECS

NULL_TOKENS = {
    "",
    "n/a",
    "na",
    "<na>",
    "nan",
    "null",
    "none",
    "-",
}

COMMON_ALIAS_TAILS = {
    "group",
    "holdings",
    "company",
    "technologies",
    "systems",
    "international",
    "industries",
    "financial",
    "bank",
    "energy",
}


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text)


def normalize_header(text: str) -> str:
    return normalize_match_text(html.unescape(text), remove_non_alnum=True)


def clean_text(value: object) -> str | None:
    if value is None:
        return None
    if pd.isna(value):
        return None
    text = strip_html(html.unescape(str(value)))
    text = collapse_spaces(text)
    if text.casefold() in NULL_TOKENS:
        return None
    return text


def clean_isin(value: object) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    normalized = re.sub(r"\s+", "", text).upper()
    return normalized or None


def clean_ticker(value: object) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    normalized = collapse_spaces(text).upper()
    return normalized or None


def clean_real(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = clean_text(value)
    if text is None:
        return None
    numeric = pd.to_numeric(text.replace(",", ""), errors="coerce")
    if pd.isna(numeric):
        return None
    return float(numeric)


def clean_integer(value: object) -> int | None:
    number = clean_real(value)
    if number is None:
        return None
    return int(round(number))


def clean_date(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        text = clean_text(value)
        if text is None:
            return None
        timestamp = pd.to_datetime(text, errors="coerce")
    if pd.isna(timestamp):
        return None
    return timestamp.date().isoformat()


def normalize_company_name(company_name: str, *, remove_the: bool = True) -> str:
    return normalize_company_name_shared(
        company_name,
        remove_the=remove_the,
        remove_non_alnum=False,
        strip_legal_suffixes=True,
    )


def generate_short_aliases(normalized_name: str) -> list[str]:
    tokens = normalized_name.split()
    if len(tokens) < 2 or len(tokens) > 4:
        return []

    short_aliases: list[str] = []
    for remove_count in (1, 2):
        if len(tokens) - remove_count < 1:
            continue
        tail = tokens[-remove_count:]
        if all(token in COMMON_ALIAS_TAILS for token in tail):
            candidate = " ".join(tokens[:-remove_count]).strip()
            if candidate:
                short_aliases.append(candidate)

    return short_aliases


def build_alias_rows(company_name: str, isin: str, ticker: str | None) -> list[tuple[str, str, str, str, str]]:
    cleaned_company = clean_text(company_name)
    if cleaned_company is None:
        return []

    candidate_rows: list[tuple[str, str, str, str, str]] = []

    def add_alias(alias_text: str, alias_type: str) -> None:
        cleaned_alias = clean_text(alias_text)
        if cleaned_alias is None:
            return
        normalized_alias = normalize_company_name(cleaned_alias)
        if not normalized_alias:
            return
        candidate_rows.append((normalized_alias, isin, cleaned_alias, cleaned_company, alias_type))

    add_alias(cleaned_company, "primary")

    without_the = re.sub(r"^the\s+", "", cleaned_company, flags=re.IGNORECASE).strip()
    if without_the and without_the != cleaned_company:
        add_alias(without_the, "without_the")

    normalized_company = normalize_company_name(cleaned_company)
    if normalized_company and normalized_company != cleaned_company.casefold():
        add_alias(normalized_company, "normalized")

    for short_alias in generate_short_aliases(normalized_company):
        add_alias(short_alias, "short")

    if ticker:
        cleaned_ticker = clean_ticker(ticker)
        if cleaned_ticker:
            candidate_rows.append((cleaned_ticker.casefold(), isin, cleaned_ticker, cleaned_company, "ticker"))

    unique_by_normalized: dict[str, tuple[str, str, str, str, str]] = {}
    for row in candidate_rows:
        alias_normalized = row[0]
        if alias_normalized not in unique_by_normalized:
            unique_by_normalized[alias_normalized] = row

    return list(unique_by_normalized.values())


def read_source_file(input_path: Path) -> pd.DataFrame:
    suffix = input_path.suffix.casefold()
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(input_path)
    if suffix == ".csv":
        return pd.read_csv(input_path)
    raise ValueError(f"Unsupported file format: {input_path.suffix}")


def resolve_column_mapping(dataframe: pd.DataFrame) -> dict[str, str | None]:
    normalized_source_columns: dict[str, str] = {}
    for column_name in dataframe.columns:
        normalized = normalize_header(str(column_name))
        normalized_source_columns.setdefault(normalized, str(column_name))

    mapping: dict[str, str | None] = {}
    for spec in COLUMN_SPECS:
        resolved_column: str | None = None
        for alias in spec.aliases:
            alias_normalized = normalize_header(alias)
            if alias_normalized in normalized_source_columns:
                resolved_column = normalized_source_columns[alias_normalized]
                break
        mapping[spec.name] = resolved_column

    return mapping


def validate_required_mapping(mapping: dict[str, str | None]) -> None:
    missing = [spec.name for spec in COLUMN_SPECS if spec.required and mapping.get(spec.name) is None]
    if missing:
        missing_label = ", ".join(sorted(missing))
        raise ValueError(f"Missing required source columns after mapping: {missing_label}")


def convert_cell(raw_value: object, value_type: str, column_name: str) -> object:
    if column_name == "isin":
        return clean_isin(raw_value)
    if column_name == "ticker":
        return clean_ticker(raw_value)
    if value_type == "text":
        return clean_text(raw_value)
    if value_type == "real":
        return clean_real(raw_value)
    if value_type == "integer":
        return clean_integer(raw_value)
    if value_type == "date":
        return clean_date(raw_value)
    return clean_text(raw_value)


def row_to_record(
    row: dict[str, object],
    mapping: dict[str, str | None],
) -> dict[str, object]:
    record: dict[str, object] = {}
    for spec in COLUMN_SPECS:
        source_column = mapping.get(spec.name)
        raw_value = row.get(source_column) if source_column else None
        record[spec.name] = convert_cell(raw_value, spec.value_type, spec.name)

    company_name = record.get("company_name")
    if isinstance(company_name, str) and company_name:
        record["normalized_company_name"] = normalize_company_name(company_name)
    else:
        record["normalized_company_name"] = None

    return record
