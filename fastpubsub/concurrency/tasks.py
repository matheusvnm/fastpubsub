"""Subscriber task for polling messages."""

import asyncio
import logging
from concurrent.futures import Future
from typing import Any, cast

from google.cloud.pubsub_v1.subscriber.exceptions import AcknowledgeError, AcknowledgeStatus
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage

from fastpubsub.clients.pubsub import PubSubClient
from fastpubsub.datastructures import Message
from fastpubsub.exceptions import Drop, Retry
from fastpubsub.logger import FastPubSubLogger
from fastpubsub.pubsub.subscriber import Subscriber

logger: FastPubSubLogger = cast(FastPubSubLogger, logging.getLogger(__name__))


class MessageMapper:
    """A mapper used to deserialize a Pub/Sub message into a fastpubsub.Message class."""

    def __init__(self, subscriber: Subscriber):
        """Initializes the MessageMapper.

        Args:
            subscriber: The subscriber to poll messages for.
        """
        self._subscriber = subscriber

    def convert(self, received_message: PubSubMessage) -> Message:
        """Converts a Pub/Sub message into a fastpubsub.Message.

        Args:
            received_message: The message received from the subscription.

        Returns:
            A fastpubsub.Message object.
        """
        delivery_attempt = 0
        if received_message.delivery_attempt is not None:
            delivery_attempt = received_message.delivery_attempt

        return Message(
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
        self.subscriber: Subscriber = subscriber
        self.mapper = MessageMapper(self.subscriber)
        self.client = PubSubClient(self.subscriber.project_id)
        self.task: StreamingPullFuture | None = None
        self.loop = asyncio.get_running_loop()

    def start(self) -> None:
        """Starts the message polling loop."""
        logger.info(f"The {self.subscriber.name} handler is waiting for messages.")
        future = self.client.subscribe(
            callback=self._on_message,
            subscription_name=self.subscriber.subscription_name,
            max_messages=self.subscriber.control_flow_policy.max_messages,
        )

        self.task = future

    def _on_message(self, received_message: PubSubMessage) -> Any:
        coroutine = self._consume(received_message)
        return self.loop.create_task(coroutine)

    async def _consume(self, received_message: PubSubMessage) -> Any:
        message = self.mapper.convert(received_message)
        with logger.contextualize(
            message_id=message.id,
            topic_name=message.topic_name,
            subscriber_name=message.subscriber_name,
        ):
            try:
                callstack = self.subscriber._build_callstack()
                response = await callstack.on_message(message)
                future = received_message.ack_with_response()
                self._wait_acknowledge_response(future=future)
                logger.info("The message successfully processed.")
                return response
            except Drop:
                future = received_message.ack_with_response()
                self._wait_acknowledge_response(future=future)
                logger.info("The message will be dropped.")
                return
            except Retry:
                future = received_message.nack_with_response()
                self._wait_acknowledge_response(future=future)
                logger.warning("The message will be retried later.")
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
