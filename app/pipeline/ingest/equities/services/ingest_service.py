from __future__ import annotations

import logging
import sqlite3

from app.pipeline.ingest.equities.context import EquitiesIngestContext
from app.pipeline.ingest.equities.services.normalization import (
    read_source_file,
    resolve_column_mapping,
    row_to_record,
    validate_required_mapping,
)
from app.pipeline.ingest.equities.services.storage import (
    ensure_schema_columns,
    initialize_database,
)
from app.pipeline.ingest.equities.services.upsert_policy import (
    apply_equities_upsert_policy,
)

LOGGER = logging.getLogger("ingest_equities")


class EquitiesIngestService:
    def parse_input(self, context: EquitiesIngestContext) -> EquitiesIngestContext:
        if context.input_path is None:
            raise ValueError("input_path is required.")
        if context.db_path is None:
            raise ValueError("db_path is required.")
        try:
            context.dataframe = read_source_file(context.input_path)
        except Exception as exc:
            raise ValueError(f"Failed to read source file: {exc}") from exc
        return context

    def map_columns(self, context: EquitiesIngestContext) -> EquitiesIngestContext:
        if context.dataframe is None:
            raise ValueError("Dataframe is not loaded.")
        mapping = resolve_column_mapping(context.dataframe)
        validate_required_mapping(mapping)
        context.mapping = mapping
        return context

    def normalize_rows(self, context: EquitiesIngestContext) -> EquitiesIngestContext:
        if context.dataframe is None:
            raise ValueError("Dataframe is not loaded.")
        if not context.mapping:
            raise ValueError("Column mapping is not resolved.")

        row_dicts = context.dataframe.to_dict(orient="records")
        records_to_insert: list[dict[str, object]] = []
        for row in row_dicts:
            record = row_to_record(row=row, mapping=context.mapping)
            records_to_insert.append(record)

        context.records_to_insert = records_to_insert
        context.skipped_missing = 0
        context.skipped_duplicates = 0
        return context

    def upsert_rows(self, context: EquitiesIngestContext) -> EquitiesIngestContext:
        if context.db_path is None:
            raise ValueError("db_path is required.")

        with sqlite3.connect(context.db_path) as connection:
            initialize_database(connection, context.mode)
            ensure_schema_columns(connection)
            outcome = apply_equities_upsert_policy(
                connection=connection,
                records=context.records_to_insert,
                mode=context.mode,
                row_number_start=2,
            )
            connection.commit()

        skipped_missing_reasons = {
            "missing_isin",
            "missing_company_name",
            "missing_normalized_company_name",
            "missing_last_update",
        }
        skipped_duplicate_reasons = {
            "duplicate_in_file",
            "stale_last_update",
        }
        context.inserted_rows = outcome.added_count
        context.alias_rows = outcome.alias_rows
        context.added_count = outcome.added_count
        context.updated_count = outcome.updated_count
        context.skipped_count = len(outcome.skipped)
        context.skipped_missing = sum(1 for item in outcome.skipped if item.reason in skipped_missing_reasons)
        context.skipped_duplicates = sum(1 for item in outcome.skipped if item.reason in skipped_duplicate_reasons)
        context.skipped = list(outcome.skipped)
        return context

    def log_summary(self, context: EquitiesIngestContext) -> None:
        LOGGER.info("Ingest completed.")
        LOGGER.info("Input file: %s", context.input_path)
        LOGGER.info("DB path: %s", context.db_path)
        LOGGER.info("Mode: %s", context.mode)
        LOGGER.info("Rows inserted into equities: %s", context.inserted_rows)
        LOGGER.info("Rows updated in equities: %s", context.updated_count)
        LOGGER.info("Alias rows prepared for insert: %s", context.alias_rows)
        LOGGER.info("Rows skipped (missing required data): %s", context.skipped_missing)
        LOGGER.info("Rows skipped (duplicate ISIN): %s", context.skipped_duplicates)
        if context.skipped_count:
            LOGGER.info("Rows skipped (total): %s", context.skipped_count)
