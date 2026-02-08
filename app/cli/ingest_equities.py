from __future__ import annotations

import argparse
import logging
from pathlib import Path

from app.pipeline.ingest.equities.pipeline import ingest_equities


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest equities XLSX/CSV into SQLite.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/equities.xlsx"),
        help="Path to input equities file (.xlsx).",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("db/equities.db"),
        help="Path to SQLite database file.",
    )
    parser.add_argument(
        "--mode",
        choices=("replace", "append"),
        default="replace",
        help="replace: recreate tables, append: keep current data and add new rows.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    arguments = parse_args()
    ingest_equities(input_path=arguments.input, db_path=arguments.db, mode=arguments.mode)


if __name__ == "__main__":
    main()
