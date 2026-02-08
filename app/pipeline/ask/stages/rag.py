from __future__ import annotations

from app.core.errors import AppError, ErrorCode
from app.pipeline.ask.services.rag_retrieval import retrieve_rag_context

from app.pipeline.ask.context import AskPipelineContext
from app.pipeline.ask.models import RAGBranchResult


class RAGBranchStage:
    name = "rag"

    def run(self, context: AskPipelineContext) -> AskPipelineContext:
        if not context.used_rag:
            context.rag_result = RAGBranchResult(sources=[], context_snippets=[], success=False)
            context.sources = []
            context.rag_context_snippets = []
            return context

        try:
            rag_result = retrieve_rag_context(
                question=context.question,
                entities=context.entities,
            )
        except Exception as exc:
            context.errors.append(
                AppError(
                    code=ErrorCode.RAG_RETRIEVAL_FAILED,
                    message=f"RAG retrieval failed ({exc}).",
                )
            )
            context.rag_result = RAGBranchResult(sources=[], context_snippets=[], success=False)
            context.sources = []
            context.rag_context_snippets = []
            return context

        if not rag_result.context_snippets:
            context.errors.append(
                AppError(
                    code=ErrorCode.RAG_NO_RELEVANT_CHUNKS,
                    message="RAG query returned no text snippets for final composition.",
                )
            )
            context.rag_result = RAGBranchResult(sources=[], context_snippets=[], success=False)
            context.sources = []
            context.rag_context_snippets = []
            return context

        if not rag_result.sources:
            context.errors.append(
                AppError(
                    code=ErrorCode.RAG_NO_RELEVANT_CHUNKS,
                    message="RAG retrieved snippets, but none passed source selection threshold.",
                )
            )

        context.rag_result = RAGBranchResult(
            sources=rag_result.sources,
            context_snippets=rag_result.context_snippets,
            success=True,
        )
        context.sources = rag_result.sources
        context.rag_context_snippets = rag_result.context_snippets
        return context
