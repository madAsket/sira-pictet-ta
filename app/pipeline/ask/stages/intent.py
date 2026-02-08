from __future__ import annotations

from app.pipeline.ask.services.intent_router import IntentRouter

from app.pipeline.ask.context import AskPipelineContext


class IntentClassificationStage:
    name = "intent"

    def __init__(self, router: IntentRouter) -> None:
        self.router = router

    def run(self, context: AskPipelineContext) -> AskPipelineContext:
        decision = self.router.classify(question=context.question)
        context.intent = decision.intent
        context.raw_intent = decision.raw_intent
        context.company_specific = bool(decision.company_specific)
        context.intent_confidence = float(decision.confidence)
        context.debug["intent_reason"] = decision.reason
        return context
