from __future__ import annotations

import re
import sqlite3
from pathlib import Path

from app.core.utils import resolve_from_project_root

TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_table_name(table_name: str) -> str:
    candidate = table_name.strip()
    if not TABLE_NAME_PATTERN.fullmatch(candidate):
        raise ValueError(f"Unsafe table name: {table_name}")
    return candidate


def table_columns_from_connection(
    connection: sqlite3.Connection,
    table_name: str,
) -> list[tuple[str, str]]:
    safe_table_name = _validate_table_name(table_name)
    rows = connection.execute(f"PRAGMA table_info({safe_table_name});").fetchall()
    columns: list[tuple[str, str]] = []
    for row in rows:
        if len(row) < 3:
            continue
        name = str(row[1]).strip()
        sqlite_type = str(row[2]).strip().upper() or "TEXT"
        if name:
            columns.append((name, sqlite_type))
    return columns


def table_columns_from_db(db_path: Path, table_name: str) -> list[tuple[str, str]]:
    target = resolve_from_project_root(db_path)
    if not target.exists():
        return []
    try:
        with sqlite3.connect(target) as connection:
            return table_columns_from_connection(connection, table_name)
    except (sqlite3.Error, ValueError):
        return []


def schema_lines_from_db(db_path: Path, table_name: str) -> list[str]:
    return [f"{name} ({sqlite_type})" for name, sqlite_type in table_columns_from_db(db_path, table_name)]


def table_column_names(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {name for name, _ in table_columns_from_connection(connection, table_name)}
