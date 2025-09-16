# TODO: Implementar o middleware de contexto de APM.

from typing import Any

from fastpubsub.datastructures import Message
from fastpubsub.logger import logger
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.observability import get_apm_provider


class DistributedTracePropagateMiddleware(BaseMiddleware):
    def __init__(self):
        self.apm = get_apm_provider()

    async def on_message(self, message: Message):
        self.apm.set_distributed_trace_context(headers=message.attributes)
        return await self.next_call.on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        if not attributes:
            attributes = {}

        headers = self.apm.get_distributed_trace_context()
        attributes.update(headers)

        return await self.next_call.on_publish(data, ordering_key, attributes)


class DistributedTraceContextualizeLogsMiddleware(BaseMiddleware):
    def __init__(self):
        self.apm = get_apm_provider()

    async def on_message(self, message: Message):
        trace_id = self.apm.get_trace_id()
        span_id = self.apm.get_span_id()

        with logger.contextualize(trace_id=trace_id, span_id=span_id):
            return await self.next_call.on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        trace_id = self.apm.get_trace_id()
        span_id = self.apm.get_span_id()

        with logger.contextualize(trace_id=trace_id, span_id=span_id):
            return await self.next_call.on_publish(data, ordering_key, attributes)
