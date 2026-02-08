from __future__ import annotations

from typing import Protocol, TypeVar, runtime_checkable

ContextT = TypeVar("ContextT")


@runtime_checkable
class Stage(Protocol[ContextT]):
    name: str

    def run(self, context: ContextT) -> ContextT:
        ...
