from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Sequence

import pandas as pd

from app.domain.equities.schema import equities_insert_columns
from app.pipeline.ingest.equities.services.normalization import build_alias_rows


@dataclass(frozen=True)
class SkippedEquity:
    isin: str | None
    reason: str
    row_number: int | None = None


@dataclass(frozen=True)
class EquitiesUpsertOutcome:
    added_count: int
    updated_count: int
    alias_rows: int
    skipped: list[SkippedEquity]


def _parse_last_update(value: object) -> pd.Timestamp | None:
    if value is None:
        return None
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    try:
        return timestamp.tz_localize(None)
    except (TypeError, AttributeError):
        return timestamp


def _build_insert_statement(columns: Sequence[str]) -> str:
    placeholders = ", ".join("?" for _ in columns)
    return (
        f"INSERT INTO equities ({', '.join(columns)}) "
        f"VALUES ({placeholders});"
    )


def _build_update_statement(columns: Sequence[str]) -> str:
    update_columns = [column for column in columns if column != "isin"]
    return (
        "UPDATE equities SET "
        + ", ".join(f"{column} = ?" for column in update_columns)
        + " WHERE isin = ?;"
    )


def _load_existing_last_updates(connection: sqlite3.Connection) -> dict[str, pd.Timestamp | None]:
    rows = connection.execute(
        """
        SELECT isin, last_update
        FROM equities
        WHERE isin IS NOT NULL
          AND isin <> '';
        """
    ).fetchall()
    existing: dict[str, pd.Timestamp | None] = {}
    for isin_raw, last_update_raw in rows:
        isin = str(isin_raw).strip().upper()
        if not isin:
            continue
        existing[isin] = _parse_last_update(last_update_raw)
    return existing


def apply_equities_upsert_policy(
    *,
    connection: sqlite3.Connection,
    records: Iterable[dict[str, object]],
    mode: str,
    row_number_start: int = 2,
) -> EquitiesUpsertOutcome:
    mode_clean = mode.strip().lower()
    if mode_clean not in {"replace", "append"}:
        raise ValueError("mode must be one of: replace, append")

    columns = equities_insert_columns()
    insert_statement = _build_insert_statement(columns)
    update_statement = _build_update_statement(columns)
    update_columns = [column for column in columns if column != "isin"]
    alias_insert_statement = """
        INSERT OR IGNORE INTO company_aliases (
            alias_normalized,
            isin,
            alias,
            company_name,
            alias_type
        ) VALUES (?, ?, ?, ?, ?);
    """.strip()

    existing_last_updates: dict[str, pd.Timestamp | None] = (
        _load_existing_last_updates(connection) if mode_clean == "append" else {}
    )
    seen_in_batch: set[str] = set()

    added_count = 0
    updated_count = 0
    alias_rows_count = 0
    skipped: list[SkippedEquity] = []

    records_list = list(records)
    for row_offset, record in enumerate(records_list):
        row_number = row_number_start + row_offset

        isin_raw = record.get("isin")
        company_name_raw = record.get("company_name")
        normalized_name_raw = record.get("normalized_company_name")

        isin = str(isin_raw).strip().upper() if isinstance(isin_raw, str) else None
        if not isin:
            skipped.append(SkippedEquity(isin=None, reason="missing_isin", row_number=row_number))
            continue
        if isin in seen_in_batch:
            skipped.append(SkippedEquity(isin=isin, reason="duplicate_in_file", row_number=row_number))
            continue
        seen_in_batch.add(isin)

        company_name = str(company_name_raw).strip() if isinstance(company_name_raw, str) else ""
        if not company_name:
            skipped.append(SkippedEquity(isin=isin, reason="missing_company_name", row_number=row_number))
            continue

        normalized_name = str(normalized_name_raw).strip() if isinstance(normalized_name_raw, str) else ""
        if not normalized_name:
            skipped.append(SkippedEquity(isin=isin, reason="missing_normalized_company_name", row_number=row_number))
            continue

        incoming_last_update = _parse_last_update(record.get("last_update"))
        has_existing = isin in existing_last_updates

        if mode_clean == "append" and has_existing:
            existing_last_update = existing_last_updates.get(isin)
            if incoming_last_update is None:
                skipped.append(SkippedEquity(isin=isin, reason="missing_last_update", row_number=row_number))
                continue
            if existing_last_update is not None and incoming_last_update <= existing_last_update:
                skipped.append(SkippedEquity(isin=isin, reason="stale_last_update", row_number=row_number))
                continue

            update_values = [record.get(column) for column in update_columns] + [isin]
            connection.execute(update_statement, tuple(update_values))
            connection.execute("DELETE FROM company_aliases WHERE isin = ?;", (isin,))
            alias_rows = build_alias_rows(
                company_name=company_name,
                isin=isin,
                ticker=record.get("ticker") if isinstance(record.get("ticker"), str) else None,
            )
            if alias_rows:
                connection.executemany(alias_insert_statement, alias_rows)
                alias_rows_count += len(alias_rows)
            updated_count += 1
            existing_last_updates[isin] = incoming_last_update
            continue

        values = tuple(record.get(column) for column in columns)
        connection.execute(insert_statement, values)
        alias_rows = build_alias_rows(
            company_name=company_name,
            isin=isin,
            ticker=record.get("ticker") if isinstance(record.get("ticker"), str) else None,
        )
        if alias_rows:
            connection.executemany(alias_insert_statement, alias_rows)
            alias_rows_count += len(alias_rows)
        added_count += 1
        if mode_clean == "append":
            existing_last_updates[isin] = incoming_last_update

    return EquitiesUpsertOutcome(
        added_count=added_count,
        updated_count=updated_count,
        alias_rows=alias_rows_count,
        skipped=skipped,
    )
