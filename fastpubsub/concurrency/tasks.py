"""Subscriber task for polling messages."""

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Generator
from concurrent.futures import Future
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
from google.cloud.pubsub_v1.subscriber.exceptions import AcknowledgeError, AcknowledgeStatus
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage
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


@contextmanager
def _contextualize(subscriber: Subscriber, message: Message) -> Generator[None]:
    apm = get_apm_provider()
    with apm.start_trace(name=subscriber.name, context=message.attributes):
        context = {
            "name": subscriber.name,
            "span_id": apm.get_span_id(),
            "trace_id": apm.get_trace_id(),
            "message_id": message.id,
            "topic_name": subscriber.topic_name,
        }
        with logger.contextualize(**context):
            yield


class MessageMapper:
    """A mapper used to deserialize a Pub/Sub message into a fastpubsub.Message class."""

    def convert(self, received_message: ReceivedMessage | PubSubMessage) -> Message:
        """Converts a Pub/Sub message into a fastpubsub.Message.

        Args:
            received_message: The message received from the subscription.

        Returns:
            A fastpubsub.Message object.
        """
        delivery_attempt = 0
        if received_message.delivery_attempt is not None:
            delivery_attempt = received_message.delivery_attempt

        if isinstance(received_message, ReceivedMessage):
            wrapped_message = received_message.message
            size = len(wrapped_message.data)
            attributes = dict(wrapped_message.attributes)

            return Message(
                id=wrapped_message.message_id,
                data=wrapped_message.data,
                size=size,
                attributes=attributes,
                delivery_attempt=delivery_attempt,
            )

        return Message(
            id=received_message.message_id,
            data=received_message.data,
            size=received_message.size,
            attributes=dict(received_message.attributes),
            delivery_attempt=delivery_attempt,
        )


class PollerTask(ABC):
    """A abstract class for polling tasks."""

    def __init__(self, subscriber: Subscriber):
        """Initializes the PubSubPollTask.

        Args:
            subscriber: The subscriber to poll messages for.
        """
        self.subscriber = subscriber
        self.client = PubSubClient(self.subscriber.project_id)

    @abstractmethod
    async def start(self) -> None:
        """Starts the message polling loop."""
        ...

    @abstractmethod
    def task_ready(self) -> bool:
        """Checks if the task is ready.

        Returns:
            True if the task is ready, False otherwise.
        """
        ...

    @abstractmethod
    def task_alive(self) -> bool:
        """Checks if the task is alive.

        Returns:
            True if the task is alive, False otherwise.
        """
        ...

    @abstractmethod
    def shutdown(self) -> None:
        """Shuts down the task."""
        ...


class PubSubPullTask(PollerTask):
    """A task for polling messages from a Pub/Sub subscription using Unary Pull."""

    def __init__(self, subscriber: Subscriber) -> None:
        """Initializes the PubSubPollTask.

        Args:
            subscriber: The subscriber to poll messages for.
        """
        super().__init__(subscriber)

        self.ready = False
        self.running = False
        self.tasks: WeakSet[asyncio.Task[Callable[[ReceivedMessage], Awaitable[Any]]]] = WeakSet()

    async def start(self) -> None:
        """Starts the message polling loop."""
        logger.info(f"The {self.subscriber.name} handler is waiting for messages.")

        self.running = True
        while self.running:
            try:
                await self._consume_messages()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._on_exception(e)

    async def _consume_messages(self) -> None:
        taskpool_size = self._get_taskpool_size()
        if taskpool_size == 0:
            await asyncio.sleep(1)
            return

        received_messages = await self.client.pull(
            subscription_name=self.subscriber.subscription_name, max_messages=taskpool_size
        )

        self.ready = True
        logger.debug(f"We have got {len(received_messages)} messages to taskify")
        for received_message in received_messages:
            message_consume_coroutine = self._consume(received_message)
            task = asyncio.create_task(message_consume_coroutine)
            self.tasks.add(task)

    async def _consume(self, received_message: ReceivedMessage) -> Any:
        mapper = MessageMapper()
        message = mapper.convert(received_message)
        with _contextualize(subscriber=self.subscriber, message=message):
            try:
                callstack = self.subscriber._build_callstack()
                response = await callstack.on_message(message)
                await self.client.ack([received_message.ack_id], self.subscriber.subscription_name)
                logger.info("Message successfully processed.")
                return response
            except Drop:
                await self.client.ack([received_message.ack_id], self.subscriber.subscription_name)
                logger.info("Message will be dropped.")
                return
            except Retry:
                await self.client.nack([received_message.ack_id], self.subscriber.subscription_name)
                logger.warning("Message processing will be retried later.")
                return
            except Exception:
                await self.client.nack([received_message.ack_id], self.subscriber.subscription_name)
                logger.exception("Unhandled exception on message", stacklevel=5)
                return

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
        logger.info(f"The {self.subscriber.name} handler is turning off...")
        self.running = False
        self.ready = False
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


class PubSubStreamingPullTask(PollerTask):
    """A task for polling messages from a Pub/Sub subscription using Streaming Pull."""

    def __init__(self, subscriber: Subscriber) -> None:
        """Initializes the PubSubPollTask.

        Args:
            subscriber: The subscriber to poll messages for.
        """
        super().__init__(subscriber)
        self.task: StreamingPullFuture | None
        self.loop = asyncio.get_running_loop()

    async def start(self) -> None:
        """Starts the message polling loop."""
        logger.info(f"The {self.subscriber.name} handler is waiting for messages.")
        future = self.client.subscribe(
            callback=self._consume,
            subscription_name=self.subscriber.subscription_name,
            max_messages=self.subscriber.control_flow_policy.max_messages,
        )

        self.task = future

    def _consume(self, received_message: PubSubMessage) -> Any:
        mapper = MessageMapper()
        message = mapper.convert(received_message)
        with _contextualize(subscriber=self.subscriber, message=message):
            try:
                callstack = self.subscriber._build_callstack()
                coroutine = callstack.on_message(message)
                task = asyncio.run_coroutine_threadsafe(coroutine, self.loop)
                response = task.result()

                future = received_message.ack_with_response()
                self._wait_acknowledge_response(future=future)
                logger.info("Message successfully processed.")
                return response
            except Drop:
                future = received_message.ack_with_response()
                self._wait_acknowledge_response(future=future)
                logger.info("Message will be dropped.")
                return
            except Retry:
                future = received_message.nack_with_response()
                self._wait_acknowledge_response(future=future)
                logger.warning("Message processing will be retried later.")
                return
            except Exception:
                future = received_message.nack_with_response()
                self._wait_acknowledge_response(future=future)
                logger.exception("Unhandled exception on message", stacklevel=5)
                return

    def _wait_acknowledge_response(self, future: Future[Any]) -> None:
        try:
            future.result(timeout=60)
        except AcknowledgeError as e:
            self._on_acknowledge_failed(e)
        except TimeoutError:
            logger.error("The acknowledge response took too long. The message will be retried.")

    def _on_acknowledge_failed(self, e: AcknowledgeError) -> None:
        match e.error_code:
            case AcknowledgeStatus.PERMISSION_DENIED:
                logger.exception(
                    "The subscriber does not have permission to ack/nack the message or the "
                    "subscription does not exists anymore.",
                    stacklevel=5,
                )
            case AcknowledgeStatus.FAILED_PRECONDITION:
                logger.exception(
                    "The subscription is detached or the subscriber "
                    "does not have access to encryption keys.",
                    stacklevel=5,
                )
            case AcknowledgeStatus.INVALID_ACK_ID:
                logger.info(
                    "The message ack_id expired. It will be redelivered later.", exc_info=True
                )
            case _:
                logger.exception("Some unknown error happened during ack/nack.", stacklevel=5)

    def task_ready(self) -> bool:
        """Checks if the task is ready.

        Returns:
            True if the task is ready, False otherwise.
        """
        if not self.task or not isinstance(self.task, StreamingPullFuture):
            return False

        return bool(self.task.running())

    def task_alive(self) -> bool:
        """Checks if the task is alive.

        Returns:
            True if the task is alive, False otherwise.
        """
        if not self.task or not isinstance(self.task, StreamingPullFuture):
            return False

        return not bool(self.task.done())

    def shutdown(self) -> None:
        """Shuts down the task."""
        logger.info(f"The {self.subscriber.name} handler is turning off...")
        if self.task and self.task.running():
            self.task.cancel()
            self.task.result()
