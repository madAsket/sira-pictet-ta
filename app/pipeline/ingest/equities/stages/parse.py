from __future__ import annotations

from app.pipeline.ingest.equities.context import EquitiesIngestContext
from app.pipeline.ingest.equities.services.ingest_service import EquitiesIngestService


class ParseStage:
    name = "parse"

    def __init__(self, service: EquitiesIngestService) -> None:
        self.service = service

    def run(self, context: EquitiesIngestContext) -> EquitiesIngestContext:
        return self.service.parse_input(context)
