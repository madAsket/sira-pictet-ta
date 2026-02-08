from __future__ import annotations

from app.core.errors import AppError, ErrorCode
from app.pipeline.ask.services.text_to_sql import TextToSQLGenerator
from app.sql_executor import SQLExecutor

from app.pipeline.ask.context import AskPipelineContext
from app.pipeline.ask.models import SQLBranchResult


class SQLBranchStage:
    name = "sql"

    def __init__(self, generator: TextToSQLGenerator, executor: SQLExecutor) -> None:
        self.generator = generator
        self.executor = executor

    def run(self, context: AskPipelineContext) -> AskPipelineContext:
        if not context.used_sql:
            context.sql_result = SQLBranchResult(sql=None, rows_preview=[], success=False)
            context.sql = None
            context.sql_rows_preview = []
            return context

        generation = self.generator.generate(
            question=context.question,
            entities=context.entities,
            company_specific=context.company_specific,
            intent=context.intent,
        )
        if generation.error:
            context.errors.append(
                AppError(
                    code=ErrorCode.SQL_GENERATION_FAILED,
                    message=generation.error,
                )
            )
            context.sql_result = SQLBranchResult(sql=None, rows_preview=[], success=False)
            context.sql = None
            context.sql_rows_preview = []
            return context

        candidate_sql = generation.sql or ""
        execution = self.executor.validate_and_execute(
            candidate_sql,
            company_specific=context.company_specific,
            entity_isins=[item.get("isin", "") for item in context.entities],
        )
        if execution.error_code:
            error_code = (
                ErrorCode.SQL_GUARDRAIL_BLOCKED
                if execution.error_code.startswith("GUARDRAIL_")
                else ErrorCode.SQL_EXECUTION_FAILED
            )
            context.errors.append(
                AppError(
                    code=error_code,
                    message=execution.error_message or "Unknown SQL failure.",
                    details={"error_code": execution.error_code},
                )
            )
            sql_text = execution.sql or candidate_sql
            context.sql_result = SQLBranchResult(sql=sql_text, rows_preview=[], success=False)
            context.sql = sql_text
            context.sql_rows_preview = []
            return context

        rows_preview = execution.rows_preview
        context.sql_result = SQLBranchResult(sql=execution.sql, rows_preview=rows_preview, success=True)
        context.sql = execution.sql
        context.sql_rows_preview = rows_preview
        return context
