from __future__ import annotations

import sqlite3

from app.core.sqlite_schema import table_column_names
from app.domain.equities.schema import (
    COLUMN_SPECS,
)
from app.pipeline.ingest.equities.services.sql_schema import (
    CREATE_COMPANY_ALIASES_SQL,
    INDEXES_SQL,
    create_equities_table_sql,
)


def initialize_database(connection: sqlite3.Connection, mode: str) -> None:
    connection.execute("PRAGMA foreign_keys = ON;")
    if mode == "replace":
        connection.execute("DROP TABLE IF EXISTS company_aliases;")
        connection.execute("DROP TABLE IF EXISTS equities;")

    connection.execute(create_equities_table_sql())
    connection.execute(CREATE_COMPANY_ALIASES_SQL)
    for index_sql in INDEXES_SQL:
        connection.execute(index_sql)


def ensure_schema_columns(connection: sqlite3.Connection) -> None:
    existing_columns = table_column_names(connection, "equities")

    for spec in COLUMN_SPECS:
        if spec.name not in existing_columns:
            connection.execute(f"ALTER TABLE equities ADD COLUMN {spec.name} {spec.sqlite_type};")
    if "normalized_company_name" not in existing_columns:
        connection.execute("ALTER TABLE equities ADD COLUMN normalized_company_name TEXT;")
