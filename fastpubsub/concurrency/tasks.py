"""Subscriber task for polling messages."""

import asyncio
import logging
from concurrent.futures import Future
from typing import Any, cast

from google.cloud.pubsub_v1.subscriber.exceptions import AcknowledgeError, AcknowledgeStatus
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage

from fastpubsub.concurrency.utils import apply_async
from fastpubsub.datastructures import PullMessage
from fastpubsub.exceptions import Drop, Retry
from fastpubsub.logger import FastPubSubLogger
from fastpubsub.subscriber import Subscriber

logger: FastPubSubLogger = cast(FastPubSubLogger, logging.getLogger(__name__))


class MessageMapper:
    """A mapper used to deserialize a Pub/Sub message into a fastpubsub.Message class."""

    def __init__(self, subscriber: Subscriber):
        """Initializes the MessageMapper.

        Args:
            subscriber: The subscriber to poll messages for.
        """
        self._subscriber = subscriber

    def convert(self, received_message: PubSubMessage) -> PullMessage:
        """Converts a Pub/Sub message into a fastpubsub.Message.

        Args:
            received_message: The message received from the subscription.

        Returns:
            A fastpubsub.Message object.
        """
        delivery_attempt = 0
        if received_message.delivery_attempt is not None:
            delivery_attempt = received_message.delivery_attempt

        return PullMessage(
            id=received_message.message_id,
            data=received_message.data,
            size=received_message.size,
            attributes=dict(received_message.attributes),
            delivery_attempt=delivery_attempt,
            project_id=self._subscriber.project_id,
            topic_name=self._subscriber.topic_name,
            subscriber_name=self._subscriber.name,
        )


class PubSubStreamingPullTask:
    """A task for polling messages from a Pub/Sub subscription with StreamingPull API."""

    def __init__(self, subscriber: Subscriber) -> None:
        """Initializes the PubSubPollTask.

        Args:
            subscriber: The subscriber to poll messages for.
        """
        from fastpubsub.clients.pubsub import PubSubClient

        self.subscriber: Subscriber = subscriber
        self.client = PubSubClient(self.subscriber.project_id)
        self.mapper = MessageMapper(self.subscriber)
        self.loop = asyncio.get_running_loop()
        self.polling: StreamingPullFuture | None = None
        self.processing_messages: set[str] = set()

    async def start(self) -> None:
        """Starts the message polling loop."""
        logger.info(f"The {self.subscriber.name} handler is waiting for messages.")
        self.polling = await self.client.subscribe(
            callback=self._on_message,
            subscription_name=self.subscriber.subscription_name,
            max_messages=self.subscriber.control_flow_policy.max_messages,
        )

    def _on_message(self, received_message: PubSubMessage) -> Any:
        coroutine = self._consume(received_message)
        return self.loop.create_task(coroutine)

    async def _consume(self, received_message: PubSubMessage) -> Any:
        message = self.mapper.convert(received_message)
        self.processing_messages.add(message.id)
        try:
            with logger.contextualize(
                message_id=message.id,
                topic_name=message.topic_name,
                subscriber_name=message.subscriber_name,
            ):
                try:
                    callstack = self.subscriber._build_callstack()
                    response = await callstack.on_message(message)
                    future = received_message.ack_with_response()
                    await self._wait_acknowledge_response(future=future)
                    logger.info("The message successfully processed.")
                    return response
                except Drop:
                    future = received_message.ack_with_response()
                    await self._wait_acknowledge_response(future=future)
                    logger.info("The message will be dropped.")
                    return
                except Retry:
                    future = received_message.nack_with_response()
                    await self._wait_acknowledge_response(future=future)
                    logger.warning("The message will be retried later.")
                    return
                except Exception:
                    future = received_message.nack_with_response()
                    await self._wait_acknowledge_response(future=future)
                    logger.exception("Unhandled exception on message", stacklevel=5)
                    return
        finally:
            self.processing_messages.discard(message.id)

    async def _wait_acknowledge_response(self, future: Future[Any]) -> None:
        try:
            await apply_async(future.result, timeout=60)
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
        if not self.polling or not isinstance(self.polling, StreamingPullFuture):
            return False

        return bool(self.polling.running())

    def task_alive(self) -> bool:
        """Checks if the task is alive.

        Returns:
            True if the task is alive, False otherwise.
        """
        if not self.polling or not isinstance(self.polling, StreamingPullFuture):
            return False

        return not bool(self.polling.done())

    async def shutdown(self) -> None:
        """Shuts down the task."""
        SHUTDOWN_TIMEOUT = 60
        CYCLES_TICKS = 1

        if self.polling and self.polling.running():
            self.polling.cancel()

        elapsed_time = 0
        while self.processing_messages and elapsed_time < SHUTDOWN_TIMEOUT:
            logger.info(f"Waiting {self.subscriber.name} messages finish processing...")
            await asyncio.sleep(CYCLES_TICKS)
            elapsed_time += CYCLES_TICKS

        logger.info(f"The {self.subscriber.name} handler is turning off...")
