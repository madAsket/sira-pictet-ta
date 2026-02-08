from __future__ import annotations

from app.pipeline.ingest.pdf.context import PDFIngestContext
from app.pipeline.ingest.pdf.services.ingest_service import PDFIngestService


class UpsertStage:
    name = "upsert"

    def __init__(self, service: PDFIngestService) -> None:
        self.service = service

    def run(self, context: PDFIngestContext) -> PDFIngestContext:
        return self.service.upsert(context)
