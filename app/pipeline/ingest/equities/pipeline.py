from __future__ import annotations

from pathlib import Path

from app.pipeline.ingest.equities.context import EquitiesIngestContext
from app.pipeline.ingest.equities.orchestrator import EquitiesIngestOrchestrator
from app.pipeline.ingest.equities.services.ingest_service import EquitiesIngestService
from app.pipeline.ingest.equities.stages import (
    MapColumnsStage,
    NormalizeStage,
    ParseStage,
    UpsertStage,
)


class EquitiesIngestPipeline:
    def __init__(
        self,
        *,
        service: EquitiesIngestService | None = None,
    ) -> None:
        ingest_service = service or EquitiesIngestService()
        self.service = ingest_service
        self.orchestrator = EquitiesIngestOrchestrator(
            stages=[
                ParseStage(ingest_service),
                MapColumnsStage(ingest_service),
                NormalizeStage(ingest_service),
                UpsertStage(ingest_service),
            ]
        )

    def process(
        self,
        *,
        input_path: Path,
        db_path: Path,
        mode: str,
    ) -> EquitiesIngestContext:
        context = EquitiesIngestContext(
            input_path=input_path,
            db_path=db_path,
            mode=mode,
        )
        completed = self.orchestrator.run(context)
        self.service.log_summary(completed)
        return completed


def ingest_equities(input_path: Path, db_path: Path, mode: str) -> None:
    pipeline = EquitiesIngestPipeline()
    pipeline.process(
        input_path=input_path,
        db_path=db_path,
        mode=mode,
    )
