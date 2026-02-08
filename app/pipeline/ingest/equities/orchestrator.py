from __future__ import annotations

from dataclasses import dataclass, field
from typing import Sequence

from app.pipeline.ingest.contracts import EquitiesIngestStage
from app.pipeline.ingest.equities.context import EquitiesIngestContext


@dataclass
class EquitiesIngestOrchestrator:
    stages: Sequence[EquitiesIngestStage] = field(default_factory=list)

    def run(self, context: EquitiesIngestContext) -> EquitiesIngestContext:
        current = context
        for stage in self.stages:
            current = stage.run(current)
        return current
