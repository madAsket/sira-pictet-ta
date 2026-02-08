from __future__ import annotations

from app.pipeline.contracts import Stage
from app.pipeline.ingest.equities.context import EquitiesIngestContext
from app.pipeline.ingest.pdf.context import PDFIngestContext

PDFIngestStage = Stage[PDFIngestContext]
EquitiesIngestStage = Stage[EquitiesIngestContext]
