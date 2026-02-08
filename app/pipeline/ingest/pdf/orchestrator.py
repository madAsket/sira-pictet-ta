from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from app.pipeline.ingest.contracts import PDFIngestStage
from app.pipeline.ingest.pdf.context import PDFIngestContext


@dataclass
class PDFIngestOrchestrator:
    stages: Sequence[PDFIngestStage] = field(default_factory=list)

    def run(self, context: PDFIngestContext) -> PDFIngestContext:
        current = context
        for stage in self.stages:
            current = stage.run(current)
        return current
