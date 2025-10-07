from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import anyio
from anyio import create_task_group
from google.api_core.exceptions import (
    Aborted,
    Cancelled,
    DeadlineExceeded,
    GatewayTimeout,
    GoogleAPICallError,
    InternalServerError,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
    Unauthenticated,
    Unauthorized,
    Unknown,
    from_grpc_error,
)
from google.pubsub_v1 import ReceivedMessage
from grpc import RpcError

from fastpubsub.clients.pubsub import PubSubClient
from fastpubsub.datastructures import Message
from fastpubsub.exceptions import Drop, Retry
from fastpubsub.logger import logger
from fastpubsub.observability import get_apm_provider
from fastpubsub.pubsub.subscriber import Subscriber

RETRYABLE_EXCEPTIONS = (
    Aborted,
    DeadlineExceeded,
    GatewayTimeout,
    InternalServerError,
    ResourceExhausted,
    ServiceUnavailable,
    Unknown,
)

FATAL_EXCEPTIONS = (
    GoogleAPICallError,
    Cancelled,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    Unauthenticated,
    Unauthorized,
)


class MessageConsumeTask:
    def __init__(self, subscriber: Subscriber) -> None:
        self.ready = False
        self.should_exit = False

        self.subscriber = subscriber
        self.apm = get_apm_provider()
        self.client = PubSubClient(self.subscriber.project_id)

    async def poll(self) -> None:
        logger.debug(f"The message poll loop started for {self.subscriber.name}")

        async with create_task_group() as tg:
            while not self.should_exit:
                try:
                    messages = await self.client.pull(self.subscriber.subscription_name)

                    self.ready = True
                    for received_message in messages:
                        message = await self._deserialize_message(received_message)
                        tg.start_soon(self._handle, message)

                    await anyio.sleep(0.05)
                except Exception as e:
                    self.ready = False
                    if self._should_terminate(e):
                        self.should_exit = True
                        logger.exception(
                            "A non-recoverable exception happened "
                            f"message handler {self.subscriber.name}."
                        )
                    elif not self._should_recover(e):
                        logger.warning(
                            f"An recoverable error happened. We will try to recover from it: {e}."
                        )
                    else:
                        logger.warning(
                            "A unknown error happened, We will try to recover but not guaranteed.",
                            exc_info=True,
                        )

    async def _handle(self, message: Message) -> Any:
        with self._contextualize(message=message):
            try:
                callstack = await self.subscriber.build_callstack()
                response = await callstack.on_message(message)
                await self.client.ack([message.ack_id], self.subscriber.subscription_name)
                logger.info("Message successfully processed.")
                return response
            except Drop:
                await self.client.ack([message.ack_id], self.subscriber.subscription_name)
                logger.info("Message will be dropped.")
                return
            except Retry:
                await self.client.nack([message.ack_id], self.subscriber.subscription_name)
                logger.warning("Message processing will be retried later.")
                return
            except Exception:
                await self.client.nack([message.ack_id], self.subscriber.subscription_name)
                logger.exception("Unhandled exception on message", stacklevel=5)
                return

    async def _deserialize_message(self, message: ReceivedMessage) -> Message:
        delivery_attempt = 0
        if message.delivery_attempt is not None:
            delivery_attempt = message.delivery_attempt

        wrapped_message = message.message

        return Message(
            id=wrapped_message.message_id,
            data=wrapped_message.data,
            size=len(wrapped_message.data),
            ack_id=message.ack_id,
            attributes=dict(wrapped_message.attributes),
            delivery_attempt=delivery_attempt,
        )

    @contextmanager
    def _contextualize(self, message: Message) -> Generator[None]:
        with self.apm.background_transaction(name=self.subscriber.name):
            self.apm.set_distributed_trace_context(message.attributes)
            context = {
                "name": self.subscriber.name,
                "span_id": self.apm.get_span_id(),
                "trace_id": self.apm.get_trace_id(),
                "message_id": message.id,
                "topic_name": self.subscriber.topic_name,
            }
            with logger.contextualize(**context):
                yield

    def task_ready(self) -> bool:
        return self.ready

    def task_alive(self) -> bool:
        return not self.should_exit and self.ready

    def shutdown(self) -> None:
        self.should_exit = True

    def _should_recover(self, exception: Exception) -> bool:
        wrapped_exception = exception
        if isinstance(exception, RpcError):
            wrapped_exception = from_grpc_error(exception)

        if isinstance(wrapped_exception, RETRYABLE_EXCEPTIONS):
            return True

        return False

    def _should_terminate(self, exception: Exception) -> bool:
        wrapped_exception = exception
        if isinstance(exception, RpcError):
            wrapped_exception = from_grpc_error(exception)

        if isinstance(wrapped_exception, FATAL_EXCEPTIONS):
            return True

        return False
