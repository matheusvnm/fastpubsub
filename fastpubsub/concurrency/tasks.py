"""Subscriber task for polling messages."""

import asyncio
from collections.abc import Awaitable, Callable, Generator
from contextlib import contextmanager
from typing import Any
from weakref import WeakSet

from google.api_core.exceptions import (
    Aborted,
    Cancelled,
    DeadlineExceeded,
    GatewayTimeout,
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

RETRYABLE_GCP_EXCEPTIONS = (
    Aborted,
    DeadlineExceeded,
    GatewayTimeout,
    InternalServerError,
    ResourceExhausted,
    ServiceUnavailable,
    Unknown,
)

FATAL_GCP_EXCEPTIONS = (
    Cancelled,
    InvalidArgument,
    NotFound,
    PermissionDenied,
    Unauthenticated,
    Unauthorized,
)


class PubSubPollTask:
    """A task for polling messages from a Pub/Sub subscription."""

    def __init__(self, subscriber: Subscriber) -> None:
        """Initializes the PubSubPollTask.

        Args:
            subscriber: The subscriber to poll messages for.
        """
        self.ready = False
        self.running = False
        self.subscriber = subscriber

        self.apm = get_apm_provider()
        self.tasks: WeakSet[asyncio.Task[Callable[[ReceivedMessage], Awaitable[Any]]]] = WeakSet()

    async def start(self) -> None:
        """Starts the message polling loop."""
        logger.info(f"The {self.subscriber.name} handler is waiting for messages.")

        self.running = True
        while self.running:
            try:
                await self._consume_messages()
            except asyncio.CancelledError:
                logger.info(f"The {self.subscriber.name} handler is turning off...")
                self.shutdown()
                raise
            except Exception as e:
                self._on_exception(e)

    async def _consume_messages(self) -> None:
        taskpool_size = self._get_taskpool_size()
        if taskpool_size == 0:
            await asyncio.sleep(1)
            return

        client = PubSubClient(self.subscriber.project_id)
        received_messages = await client.pull(
            subscription_name=self.subscriber.subscription_name, max_messages=taskpool_size
        )

        self.ready = True
        logger.debug(f"We have got {len(received_messages)} messages to taskify")
        for received_message in received_messages:
            message_consume_coroutine = self._consume(received_message)
            task = asyncio.create_task(message_consume_coroutine)
            self.tasks.add(task)

    async def _deserialize_message(self, received_message: ReceivedMessage) -> Message:
        wrapped_message = received_message.message

        delivery_attempt = 0
        if received_message.delivery_attempt is not None:
            delivery_attempt = received_message.delivery_attempt

        size = len(wrapped_message.data)
        attributes = dict(wrapped_message.attributes)

        return Message(
            id=wrapped_message.message_id,
            data=wrapped_message.data,
            size=size,
            attributes=attributes,
            delivery_attempt=delivery_attempt,
        )

    async def _consume(self, received_message: ReceivedMessage) -> Any:
        message = await self._deserialize_message(received_message)
        with self._contextualize(message=message):
            client = PubSubClient(self.subscriber.project_id)
            try:
                callstack = await self.subscriber._build_callstack()
                response = await callstack.on_message(message)
                await client.ack([received_message.ack_id], self.subscriber.subscription_name)
                logger.info("Message successfully processed.")
                return response
            except Drop:
                await client.ack([received_message.ack_id], self.subscriber.subscription_name)
                logger.info("Message will be dropped.")
                return
            except Retry:
                await client.nack([received_message.ack_id], self.subscriber.subscription_name)
                logger.warning("Message processing will be retried later.")
                return
            except Exception:
                await client.nack([received_message.ack_id], self.subscriber.subscription_name)
                logger.exception("Unhandled exception on message", stacklevel=5)
                return

    @contextmanager
    def _contextualize(self, message: Message) -> Generator[None]:
        with self.apm.start_trace(name=self.subscriber.name, context=message.attributes):
            context = {
                "name": self.subscriber.name,
                "span_id": self.apm.get_span_id(),
                "trace_id": self.apm.get_trace_id(),
                "message_id": message.id,
                "topic_name": self.subscriber.topic_name,
            }
            with logger.contextualize(**context):
                yield

    def _on_exception(self, e: Exception) -> None:
        self.ready = False
        if self._should_terminate(e):
            self.running = False
            logger.exception(
                f"A non-recoverable exception happened on message handler {self.subscriber.name}."
            )
            return

        if not self._should_recover(e):
            logger.warning(
                "An recoverable error ocurred, we will try to recover from it.",
                exc_info=True,
            )
            return

        logger.warning(
            "A unhandled error ocurred, trying to recover with no guarantees.",
            exc_info=True,
        )

    def _should_recover(self, exception: Exception) -> bool:
        wrapped_exception = exception
        if isinstance(exception, RpcError):
            wrapped_exception = from_grpc_error(exception)  # type: ignore[no-untyped-call]

        if isinstance(wrapped_exception, RETRYABLE_GCP_EXCEPTIONS):
            return True

        return False

    def _should_terminate(self, exception: Exception) -> bool:
        wrapped_exception = exception
        if isinstance(exception, RpcError):
            wrapped_exception = from_grpc_error(exception)  # type: ignore[no-untyped-call]

        if isinstance(wrapped_exception, FATAL_GCP_EXCEPTIONS):
            return True

        return False

    def task_ready(self) -> bool:
        """Checks if the task is ready.

        Returns:
            True if the task is ready, False otherwise.
        """
        return self.ready

    def task_alive(self) -> bool:
        """Checks if the task is alive.

        Returns:
            True if the task is alive, False otherwise.
        """
        return self.running

    def shutdown(self) -> None:
        """Shuts down the task."""
        self.running = False
        self._cancel_tasks()

    def _cancel_tasks(self) -> None:
        """Cancel all the message consuming task alive."""
        for task in self.tasks:
            task.cancel()

    def _get_taskpool_size(self) -> int:
        """Get the ammount of tasks actively running.

        Returns:
            The number of tasks running.
        """
        max_messages = self.subscriber.control_flow_policy.max_messages
        taskpool_size = max_messages - len(self.tasks)

        logger.debug(
            f"The maximum queue size is {max_messages} with {taskpool_size} task slots empty."
        )
        return taskpool_size
