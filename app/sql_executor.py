from __future__ import annotations

import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence

from app.core.sqlite_schema import table_column_names

DEFAULT_DB_PATH = Path("db/equities.db")

FORBIDDEN_SQL_PATTERN = re.compile(
    r"\b(insert|update|delete|alter|drop|create|attach|detach|pragma|vacuum|replace|truncate)\b",
    flags=re.IGNORECASE,
)
TABLE_REF_PATTERN = re.compile(r"\b(?:from|join)\s+([`\"\[]?[a-zA-Z_][\w$]*(?:\.[a-zA-Z_][\w$]*)?[`\"\]]?)", re.IGNORECASE)
LIMIT_PATTERN = re.compile(r"\blimit\s+\d+\b", flags=re.IGNORECASE)
WHERE_PATTERN = re.compile(r"\bwhere\b", flags=re.IGNORECASE)
CLAUSE_BOUNDARY_PATTERNS = (
    re.compile(r"\bgroup\s+by\b", flags=re.IGNORECASE),
    re.compile(r"\border\s+by\b", flags=re.IGNORECASE),
    re.compile(r"\blimit\b", flags=re.IGNORECASE),
    re.compile(r"\boffset\b", flags=re.IGNORECASE),
)

DENIED_ACTIONS = {
    sqlite3.SQLITE_INSERT,
    sqlite3.SQLITE_UPDATE,
    sqlite3.SQLITE_DELETE,
    sqlite3.SQLITE_PRAGMA,
    sqlite3.SQLITE_ATTACH,
    sqlite3.SQLITE_DETACH,
    sqlite3.SQLITE_ALTER_TABLE,
    sqlite3.SQLITE_CREATE_TABLE,
    sqlite3.SQLITE_DROP_TABLE,
}


@dataclass(frozen=True)
class SQLExecutionResult:
    sql: str | None
    rows_preview: list[dict[str, Any]]
    error_code: str | None
    error_message: str | None


def _normalize_identifier(identifier: str) -> str:
    normalized = identifier.strip().strip("`\"[]")
    if "." in normalized:
        normalized = normalized.split(".")[-1]
    return normalized.lower()


def _escape_sql_literal(value: str) -> str:
    return value.replace("'", "''")


class SQLExecutor:
    def __init__(
        self,
        *,
        db_path: Path = DEFAULT_DB_PATH,
        preview_limit: int = 5,
        max_limit: int = 50,
    ) -> None:
        self.db_path = db_path
        self.preview_limit = max(1, preview_limit)
        self.max_limit = max(1, max_limit)
        if not self.db_path.exists():
            raise FileNotFoundError(f"Equities DB not found: {self.db_path}")

    def validate_and_execute(
        self,
        sql: str,
        *,
        company_specific: bool,
        entity_isins: Sequence[str],
    ) -> SQLExecutionResult:
        normalized = self._normalize_sql(sql)
        if not normalized:
            return SQLExecutionResult(
                sql=None,
                rows_preview=[],
                error_code="SQL_EMPTY",
                error_message="Generated SQL is empty.",
            )

        if ";" in normalized:
            return SQLExecutionResult(
                sql=normalized,
                rows_preview=[],
                error_code="GUARDRAIL_MULTI_STATEMENT",
                error_message="Only a single SELECT statement is allowed.",
            )

        if not re.match(r"^\s*select\b", normalized, flags=re.IGNORECASE):
            return SQLExecutionResult(
                sql=normalized,
                rows_preview=[],
                error_code="GUARDRAIL_SELECT_ONLY",
                error_message="Only SELECT statements are allowed.",
            )

        forbidden_match = FORBIDDEN_SQL_PATTERN.search(normalized)
        if forbidden_match:
            return SQLExecutionResult(
                sql=normalized,
                rows_preview=[],
                error_code="GUARDRAIL_FORBIDDEN_KEYWORD",
                error_message=f"Forbidden SQL keyword: {forbidden_match.group(1)}.",
            )

        referenced_tables = self._extract_tables(normalized)
        if not referenced_tables:
            return SQLExecutionResult(
                sql=normalized,
                rows_preview=[],
                error_code="GUARDRAIL_TABLE_REQUIRED",
                error_message="SQL must reference the equities table.",
            )
        disallowed = sorted(table for table in referenced_tables if table != "equities")
        if disallowed:
            return SQLExecutionResult(
                sql=normalized,
                rows_preview=[],
                error_code="GUARDRAIL_TABLE_NOT_ALLOWED",
                error_message=f"Only table 'equities' is allowed. Found: {', '.join(disallowed)}.",
            )

        guarded_sql = normalized
        if company_specific:
            normalized_isins = sorted(
                {
                    str(isin).strip().upper()
                    for isin in entity_isins
                    if str(isin).strip()
                }
            )
            if not normalized_isins:
                return SQLExecutionResult(
                    sql=guarded_sql,
                    rows_preview=[],
                    error_code="GUARDRAIL_MISSING_ENTITY_ISIN",
                    error_message="Company-specific SQL requires non-empty entity ISIN list.",
                )
            guarded_sql = self._inject_isin_filter(guarded_sql, normalized_isins)
            if guarded_sql is None:
                return SQLExecutionResult(
                    sql=normalized,
                    rows_preview=[],
                    error_code="GUARDRAIL_ISIN_FILTER_FAILED",
                    error_message="Failed to enforce mandatory ISIN filter for company-specific SQL.",
                )

        if not LIMIT_PATTERN.search(guarded_sql):
            guarded_sql = f"{guarded_sql} LIMIT {self.max_limit}"

        try:
            with sqlite3.connect(self.db_path) as connection:
                connection.row_factory = sqlite3.Row
                allowed_columns = self._load_columns(connection)
                connection.set_authorizer(self._build_authorizer(allowed_columns))
                connection.execute(f"EXPLAIN QUERY PLAN {guarded_sql}").fetchall()
                rows = connection.execute(guarded_sql).fetchmany(self.preview_limit)
        except sqlite3.DatabaseError as exc:
            return SQLExecutionResult(
                sql=guarded_sql,
                rows_preview=[],
                error_code="SQL_EXECUTION_FAILED",
                error_message=f"{exc}",
            )

        rows_preview = [{key: row[key] for key in row.keys()} for row in rows]
        return SQLExecutionResult(
            sql=guarded_sql,
            rows_preview=rows_preview,
            error_code=None,
            error_message=None,
        )

    def _normalize_sql(self, sql: str) -> str:
        cleaned = (sql or "").strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```[a-zA-Z0-9_+-]*\s*", "", cleaned)
            cleaned = re.sub(r"\s*```$", "", cleaned)
        cleaned = cleaned.strip()
        cleaned = cleaned[:-1].strip() if cleaned.endswith(";") else cleaned
        return cleaned

    def _extract_tables(self, sql: str) -> set[str]:
        tables: set[str] = set()
        for matched in TABLE_REF_PATTERN.findall(sql):
            normalized = _normalize_identifier(matched)
            if normalized:
                tables.add(normalized)
        return tables

    def _inject_isin_filter(self, sql: str, entity_isins: Sequence[str]) -> str | None:
        if not entity_isins:
            return None

        condition = "isin IN (" + ", ".join(f"'{_escape_sql_literal(isin)}'" for isin in entity_isins) + ")"
        boundary = len(sql)
        for pattern in CLAUSE_BOUNDARY_PATTERNS:
            match = pattern.search(sql)
            if match and match.start() < boundary:
                boundary = match.start()

        where_match = WHERE_PATTERN.search(sql)
        if where_match and where_match.start() < boundary:
            return f"{sql[:boundary]} AND {condition} {sql[boundary:]}".strip()
        return f"{sql[:boundary]} WHERE {condition} {sql[boundary:]}".strip()

    def _load_columns(self, connection: sqlite3.Connection) -> set[str]:
        allowed = {name.lower() for name in table_column_names(connection, "equities")}
        allowed.add("rowid")
        return allowed

    def _build_authorizer(self, allowed_columns: set[str]) -> Callable[[int, str | None, str | None, str | None, str | None], int]:
        def authorizer(
            action_code: int,
            arg1: str | None,
            arg2: str | None,
            db_name: str | None,
            trigger_name: str | None,
        ) -> int:
            _ = (db_name, trigger_name)
            if action_code in DENIED_ACTIONS:
                return sqlite3.SQLITE_DENY

            if action_code == sqlite3.SQLITE_READ:
                table_name = (arg1 or "").strip().lower()
                column_name = (arg2 or "").strip().lower()
                if table_name != "equities":
                    return sqlite3.SQLITE_DENY
                if column_name and column_name not in allowed_columns:
                    return sqlite3.SQLITE_DENY

            return sqlite3.SQLITE_OK

        return authorizer
