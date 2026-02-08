from __future__ import annotations

from app.core.errors import AppError, ErrorCode
from app.pipeline.ask.services.final_composer import FinalResponseComposer

from app.pipeline.ask.context import AskPipelineContext


class ComposeStage:
    name = "compose"

    def __init__(self, composer: FinalResponseComposer) -> None:
        self.composer = composer

    def run(self, context: AskPipelineContext) -> AskPipelineContext:
        composed = self.composer.compose(
            question=context.question,
            intent=context.intent,
            entities=context.entities,
            used_sql=context.used_sql,
            used_rag=context.used_rag,
            sql_rows_preview=context.sql_rows_preview if context.sql_result.success else [],
            rag_context_snippets=context.rag_context_snippets if context.rag_result.success else [],
        )
        context.answer = composed.answer
        if composed.error:
            context.errors.append(
                AppError(
                    code=ErrorCode.FINAL_COMPOSER_FALLBACK,
                    message=composed.error,
                )
            )
        return context
