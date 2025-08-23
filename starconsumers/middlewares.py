import asyncio
from copy import deepcopy
from typing import Any

from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage

from starconsumers import observability
from starconsumers.datastructures import (
    DecoratedCallable,
    MessageMiddleware,
    MiddlewareContainer,
    TopicMessage,
)
from starconsumers.exceptions import DropException, RetryException
from starconsumers.logger import logger


class APMTransactionMiddleware(MessageMiddleware):
    def __call__(self, message: PubSubMessage) -> Any:
        apm = observability.get_apm_provider()
        with apm.background_transaction("MessageMiddleware"):
            apm.set_distributed_trace_context(message.attributes)
            return super().__call__(message)


class APMLogContextMiddleware(MessageMiddleware):
    def __call__(self, message: PubSubMessage) -> Any:
        apm = observability.get_apm_provider()

        trace_id = apm.get_trace_id()
        span_id = apm.get_span_id()
        with logger.contextualize(
            trace_id=trace_id, span_id=span_id, message_id=message.message_id
        ):
            return super().__call__(message)


class BasicExceptionHandler(MessageMiddleware):
    def __call__(self, message: PubSubMessage) -> Any:
        try:
            response = super().__call__(message)
            message.ack()
            logger.info(f"Message {message.message_id} successfully processed.")
            return response
        except DropException:
            logger.info(f"DROP: Message {message.message_id} will be dropped")
            message.ack()
            return None
        except RetryException:
            logger.warning(f"RETRY: Message {message.message_id} will be retried")
            message.nack()
            return None
        except Exception:
            logger.exception(f"Unhandled exception on message {message.message_id}", stacklevel=5)
            message.nack()
            return None


class MessageSerializerMiddleware(MessageMiddleware):
    def __call__(self, message: PubSubMessage) -> Any:
        serialized_message = TopicMessage(
            id=message.message_id,
            size=message.size,
            data=message.data,
            attributes=message.attributes,
            delivery_attempt=message.delivery_attempt,
        )

        return super().__call__(serialized_message)


class AsyncContextMiddleware(MessageMiddleware):
    def __init__(self, next_call: MessageMiddleware) -> None:
        self.next_call = next_call
        self.is_coroutine = asyncio.iscoroutinefunction(next_call)

    def __call__(self, message: TopicMessage) -> Any:
        if not self.is_coroutine:
            return super().__call__(message)

        return asyncio.run(super().__call__(message))


class MiddlewareChainBuilder:
    def __init__(
        self,
        handler: DecoratedCallable,
        local_middlewares: list[MiddlewareContainer],
        global_middlewares: list[MiddlewareContainer],
    ):
        self.handler = handler
        self.middleware_chain = deepcopy(global_middlewares) + deepcopy(local_middlewares)

    def _build_chain_beginning(self) -> list[MiddlewareContainer]:
        return [
            MiddlewareContainer(APMTransactionMiddleware),
            MiddlewareContainer(APMLogContextMiddleware),
            MiddlewareContainer(BasicExceptionHandler),
            MiddlewareContainer(MessageSerializerMiddleware),
        ]

    def _build_chain_end(self) -> list[MiddlewareContainer]:
        return [MiddlewareContainer(AsyncContextMiddleware)]

    def build(self) -> MessageMiddleware | DecoratedCallable:
        chain_beginning = self._build_chain_beginning()
        chain_end = self._build_chain_end()

        middlewares = chain_beginning + self.middleware_chain + chain_end

        next_call = self.handler
        for cls, args, kwargs in reversed(middlewares):
            next_call = cls(*args, next_call=next_call, **kwargs)

        return next_call
