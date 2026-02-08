from __future__ import annotations

from pathlib import Path

from app.core.settings import get_settings
from app.pipeline.ask.orchestrator import AskPipelineOrchestrator
from app.pipeline.ask.services.entity_resolver import EntityResolver
from app.pipeline.ask.services.final_composer import FinalResponseComposer
from app.pipeline.ask.services.intent_router import IntentRouter
from app.pipeline.ask.services.text_to_sql import TextToSQLGenerator
from app.pipeline.ask.stages.compose import ComposeStage
from app.pipeline.ask.stages.entities import EntityResolutionStage
from app.pipeline.ask.stages.intent import IntentClassificationStage
from app.pipeline.ask.stages.rag import RAGBranchStage
from app.pipeline.ask.stages.sql import SQLBranchStage
from app.pipeline.ask.models import PipelineResult
from app.sql_executor import SQLExecutor


class QuestionPipeline:
    def __init__(
        self,
        *,
        db_path: Path = Path("db/equities.db"),
        router: IntentRouter | None = None,
        resolver: EntityResolver | None = None,
        sql_generator: TextToSQLGenerator | None = None,
        sql_executor: SQLExecutor | None = None,
        final_composer: FinalResponseComposer | None = None,
        not_found_template: str | None = None,
    ) -> None:
        settings = get_settings()
        router_instance = router or IntentRouter(db_path=db_path)
        resolver_instance = resolver or EntityResolver(db_path=db_path)
        sql_generator_instance = sql_generator or TextToSQLGenerator(db_path=db_path)
        sql_executor_instance = sql_executor or SQLExecutor(
            db_path=db_path,
            preview_limit=settings.sql_rows_preview_limit,
            max_limit=settings.sql_max_limit,
        )
        composer_instance = final_composer or FinalResponseComposer()

        self.orchestrator = AskPipelineOrchestrator(
            intent_stage=IntentClassificationStage(router_instance),
            entity_stage=EntityResolutionStage(resolver_instance),
            sql_stage=SQLBranchStage(sql_generator_instance, sql_executor_instance),
            rag_stage=RAGBranchStage(),
            compose_stage=ComposeStage(composer_instance),
            not_found_template=not_found_template or settings.entity_not_found_template,
            non_company_override_threshold=settings.non_company_intent_override_threshold,
            composer_debug_flags_enabled=settings.composer_debug_flags_enabled,
        )

    def process(self, question: str) -> PipelineResult:
        return self.orchestrator.process(question=question)
