from __future__ import annotations

from app.domain.equities.schema import COLUMN_SPECS, DERIVED_COLUMNS


def create_equities_table_sql() -> str:
    column_definitions: list[str] = []
    for spec in COLUMN_SPECS:
        if spec.name == "isin":
            column_definitions.append(f"{spec.name} {spec.sqlite_type} PRIMARY KEY")
            continue
        not_null = " NOT NULL" if spec.required else ""
        column_definitions.append(f"{spec.name} {spec.sqlite_type}{not_null}")

    for spec in DERIVED_COLUMNS:
        not_null = " NOT NULL" if spec.required else ""
        column_definitions.append(f"{spec.name} {spec.sqlite_type}{not_null}")

    definition_sql = ",\n            ".join(column_definitions)
    return f"""
        CREATE TABLE IF NOT EXISTS equities (
            {definition_sql}
        );
    """.strip()


CREATE_COMPANY_ALIASES_SQL = """
    CREATE TABLE IF NOT EXISTS company_aliases (
        alias_normalized TEXT NOT NULL,
        isin TEXT NOT NULL,
        alias TEXT NOT NULL,
        company_name TEXT NOT NULL,
        alias_type TEXT NOT NULL,
        PRIMARY KEY (alias_normalized, isin),
        FOREIGN KEY (isin) REFERENCES equities(isin) ON DELETE CASCADE
    );
""".strip()


INDEXES_SQL: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_equities_company_name_norm ON equities(normalized_company_name);",
    "CREATE INDEX IF NOT EXISTS idx_equities_ticker ON equities(ticker);",
    "CREATE INDEX IF NOT EXISTS idx_aliases_norm ON company_aliases(alias_normalized);",
    "CREATE INDEX IF NOT EXISTS idx_aliases_isin ON company_aliases(isin);",
)
