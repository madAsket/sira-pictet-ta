from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.pipeline.ask.context import AskPipelineContext
from app.pipeline.contracts import Stage

AskStage = Stage[AskPipelineContext]


@runtime_checkable
class IntentStage(Protocol):
    name: str

    def run(self, context: AskPipelineContext) -> AskPipelineContext:
        ...


@runtime_checkable
class EntityStage(Protocol):
    name: str

    def run(self, context: AskPipelineContext) -> AskPipelineContext:
        ...


@runtime_checkable
class SQLStage(Protocol):
    name: str

    def run(self, context: AskPipelineContext) -> AskPipelineContext:
        ...


@runtime_checkable
class RAGStage(Protocol):
    name: str

    def run(self, context: AskPipelineContext) -> AskPipelineContext:
        ...


@runtime_checkable
class ComposeStage(Protocol):
    name: str

    def run(self, context: AskPipelineContext) -> AskPipelineContext:
        ...
