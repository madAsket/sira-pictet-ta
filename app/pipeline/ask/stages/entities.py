from __future__ import annotations

from dataclasses import asdict

from app.core.errors import AppError, ErrorCode
from app.pipeline.ask.services.entity_resolver import EntityResolver

from app.pipeline.ask.context import AskPipelineContext


class EntityResolutionStage:
    name = "entities"

    def __init__(self, resolver: EntityResolver) -> None:
        self.resolver = resolver

    def run(self, context: AskPipelineContext) -> AskPipelineContext:
        resolution = self.resolver.resolve(question=context.question)
        context.debug["resolved_entities"] = [asdict(item) for item in resolution.entities]
        context.debug["rejected_candidates"] = [asdict(item) for item in resolution.rejected_candidates]
        if resolution.rejected_candidates:
            context.errors.append(
                AppError(
                    code=ErrorCode.REJECTED_CANDIDATES_DEBUG,
                    message="Rejected candidates were captured for debugging.",
                    details=[asdict(item) for item in resolution.rejected_candidates],
                )
            )
        context.entities = [asdict(item) for item in resolution.entities]
        return context
