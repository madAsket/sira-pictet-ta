from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from app.pipeline.ingest.equities.services.upsert_policy import SkippedEquity


@dataclass
class EquitiesIngestContext:
    input_path: Path | None = None
    db_path: Path | None = None
    mode: str = "replace"

    dataframe: pd.DataFrame | None = None
    mapping: dict[str, str | None] = field(default_factory=dict)
    records_to_insert: list[dict[str, object]] = field(default_factory=list)

    inserted_rows: int = 0
    alias_rows: int = 0
    skipped_missing: int = 0
    skipped_duplicates: int = 0

    added_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    skipped: list[SkippedEquity] = field(default_factory=list)
    debug: dict[str, object] = field(default_factory=dict)
