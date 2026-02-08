from __future__ import annotations

import re
from dataclasses import dataclass

from app.core.errors import AppError, ErrorCode, to_error_dict
from app.pipeline.ask.context import AskPipelineContext
from app.pipeline.ask.stages.compose import ComposeStage
from app.pipeline.ask.stages.entities import EntityResolutionStage
from app.pipeline.ask.stages.intent import IntentClassificationStage
from app.pipeline.ask.stages.rag import RAGBranchStage
from app.pipeline.ask.stages.sql import SQLBranchStage
from app.pipeline.ask.models import IntentType, PipelineResult, intent_usage

COMPANY_HINT_PATTERN = re.compile(r"\b(company|ticker|isin)\b", flags=re.IGNORECASE)
SQL_SCREENING_HINT_PATTERN = re.compile(
    r"\b(top|highest|lowest|rank(?:ing)?|best|worst|screen(?:ing)?|filter|region|sector|industry|"
    r"market cap|dividend yield|target price|p\/e|pe ratio)\b",
    flags=re.IGNORECASE,
)
MACRO_HINT_PATTERN = re.compile(
    r"\b(macro|macroeconomic|inflation|interest rates?|recession|gdp|central bank|policy|"
    r"economic outlook|macro outlook|geopolitical)\b",
    flags=re.IGNORECASE,
)


@dataclass
class AskPipelineOrchestrator:
    intent_stage: IntentClassificationStage
    entity_stage: EntityResolutionStage
    sql_stage: SQLBranchStage
    rag_stage: RAGBranchStage
    compose_stage: ComposeStage
    not_found_template: str
    non_company_override_threshold: float
    composer_debug_flags_enabled: bool

    def run(self, *, question: str) -> AskPipelineContext:
        context = AskPipelineContext(question=question)
        context = self.intent_stage.run(context)
        context = self.entity_stage.run(context)

        if (
            not context.company_specific
            and context.intent_confidence == 0.0
            and (context.entities or COMPANY_HINT_PATTERN.search(context.question))
        ):
            context.company_specific = True
            context.errors.insert(
                0,
                AppError(
                    code=ErrorCode.ROUTER_FALLBACK_COMPANY_SPECIFIC_HEURISTIC,
                    message=(
                        "Router fallback was used; company_specific set by local heuristic "
                        "because company hints were detected."
                    ),
                ),
            )

        if context.company_specific and not context.entities:
            not_found_message = self.not_found_template.format(question=context.question)
            context.errors.insert(
                0,
                AppError(
                    code=ErrorCode.ENTITY_NOT_FOUND,
                    message=not_found_message,
                ),
            )
            context.answer = not_found_message
            context.used_sql = False
            context.used_rag = False
            context.sql = None
            context.sql_rows_preview = []
            context.sources = []
            return context

        effective_intent: IntentType = context.intent
        if not context.company_specific:
            heuristic_intent = self._infer_non_company_intent(context.question)
            if effective_intent == "unknown":
                if heuristic_intent != "unknown":
                    context.errors.insert(
                        0,
                        AppError(
                            code=ErrorCode.NON_COMPANY_UNKNOWN_DEFAULTED_BY_HEURISTIC,
                            message=(
                                "Non-company-specific unknown intent was resolved by local heuristic "
                                f"as {heuristic_intent}."
                            ),
                        ),
                    )
                    effective_intent = heuristic_intent
                else:
                    context.errors.insert(
                        0,
                        AppError(
                            code=ErrorCode.NON_COMPANY_UNKNOWN_DEFAULTED_TO_MACRO,
                            message="Non-company-specific unknown intent defaulted to macro_only.",
                        ),
                    )
                    effective_intent = "macro_only"
            elif (
                heuristic_intent != "unknown"
                and heuristic_intent != effective_intent
                and context.intent_confidence < self.non_company_override_threshold
            ):
                context.errors.insert(
                    0,
                    AppError(
                        code=ErrorCode.NON_COMPANY_LOW_CONFIDENCE_OVERRIDDEN_BY_HEURISTIC,
                        message=(
                            "Low-confidence non-company intent was overridden by local heuristic "
                            f"from {effective_intent} to {heuristic_intent}."
                        ),
                    ),
                )
                effective_intent = heuristic_intent

        context.intent = effective_intent
        context.used_sql, context.used_rag = intent_usage(context.intent)
        if not context.company_specific:
            context.entities = []

        context = self.sql_stage.run(context)
        context = self.rag_stage.run(context)
        context = self.compose_stage.run(context)

        if self.composer_debug_flags_enabled:
            context.errors.append(
                AppError(
                    code=ErrorCode.COMPOSER_DEBUG_FLAGS,
                    message="Composer input flags captured from pipeline outputs.",
                    details={
                        "had_sql_rows": bool(context.sql_rows_preview if context.sql_result.success else []),
                        "had_rag_snippets": bool(
                            context.rag_context_snippets if context.rag_result.success else []
                        ),
                        "entities_used": [
                            str(item.get("isin")).strip()
                            for item in context.entities
                            if str(item.get("isin", "")).strip()
                        ],
                    },
                )
            )
        return context

    def process(self, *, question: str) -> PipelineResult:
        context = self.run(question=question)
        return PipelineResult(
            question=context.question,
            intent=context.intent,
            raw_intent=context.raw_intent,
            company_specific=context.company_specific,
            intent_confidence=context.intent_confidence,
            entities=context.entities,
            used_sql=context.used_sql,
            used_rag=context.used_rag,
            sql=context.sql,
            sql_rows_preview=context.sql_rows_preview,
            answer=context.answer,
            sources=context.sources if context.used_rag else [],
            errors=[to_error_dict(item) for item in context.errors],
        )

    def _infer_non_company_intent(self, question: str) -> IntentType:
        has_sql_signal = bool(SQL_SCREENING_HINT_PATTERN.search(question))
        has_macro_signal = bool(MACRO_HINT_PATTERN.search(question))
        if has_sql_signal and not has_macro_signal:
            return "equity_only"
        if has_macro_signal and not has_sql_signal:
            return "macro_only"
        if has_sql_signal and has_macro_signal:
            return "hybrid"
        return "unknown"
