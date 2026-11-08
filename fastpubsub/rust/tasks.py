"""Subscriber task that receives messages from the Rust core."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, cast

from fastpubsub_core import AckHandle, Subscription

from fastpubsub.datastructures import Message
from fastpubsub.exceptions import Drop, Retry
from fastpubsub.logger import FastPubSubLogger
from fastpubsub.rust.client import (
    MAX_BATCH_SIZE,
    build_subscription_path,
    read_endpoint,
)

if TYPE_CHECKING:
    from fastpubsub.pubsub.subscriber import Subscriber

logger: FastPubSubLogger = cast(FastPubSubLogger, logging.getLogger(__name__))

type ReceivedMessage = tuple[str, bytes, dict[str, str], int, AckHandle]


class PubSubStreamingPullTask:
    """A task that runs the handler for each message that Rust receives."""

    def __init__(self, subscriber: Subscriber) -> None:
        """Initializes the PubSubStreamingPullTask.

        Args:
            subscriber: The subscriber to receive messages for.
        """
        self.loop = asyncio.get_running_loop()
        self.subscriber = subscriber
        self.subscription: Subscription | None = None
        self.tasks: set[asyncio.Task[Any]] = set()

    async def start(self) -> None:
        """Starts the message stream."""
        logger.info(
            f"The {self.subscriber.name} handler is waiting for messages."
        )
        self.subscription = await asyncio.to_thread(
            Subscription,
            build_subscription_path(
                self.subscriber.project_id, self.subscriber.subscription_name
            ),
            self.subscriber.control_flow_policy.max_messages,
            MAX_BATCH_SIZE,
            self.loop.call_soon_threadsafe,
            self._dispatch,
            read_endpoint(),
        )

    def _dispatch(self, batch: list[ReceivedMessage]) -> None:
        for message_id, data, attributes, delivery_attempt, ack in batch:
            message = Message(
                id=message_id,
                data=data,
                size=len(data),
                attributes=attributes,
                delivery_attempt=delivery_attempt,
                project_id=self.subscriber.project_id,
                topic_name=self.subscriber.topic_name,
                subscriber_name=self.subscriber.name,
            )
            task = self.loop.create_task(self._consume(message, ack))
            self.tasks.add(task)
            task.add_done_callback(self.tasks.discard)

    async def _consume(self, message: Message, ack: AckHandle) -> Any:
        with logger.contextualize(
            message_id=message.id,
            topic_name=message.topic_name,
            subscriber_name=message.subscriber_name,
        ):
            try:
                response = await self.subscriber.callstack.on_message(message)
                ack.ack()
                logger.info("The message successfully processed.")
                return response
            except Drop:
                ack.ack()
                logger.info("The message will be dropped.")
            except Retry:
                ack.nack()
                logger.warning("The message will be retried later.")
            except Exception:
                ack.nack()
                logger.exception(
                    "Unhandled exception on message", stacklevel=5
                )

    def task_ready(self) -> bool:
        """Checks if the task is ready.

        Returns:
            True if the message stream runs, False otherwise.
        """
        return bool(self.subscription and self.subscription.is_running())

    def task_alive(self) -> bool:
        """Checks if the task is alive.

        Returns:
            True if the message stream runs, False otherwise.
        """
        return self.task_ready()

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Stops the stream and waits for the messages in progress.

        Args:
            timeout: The maximum time to wait, in seconds.
        """
        logger.info(f"The {self.subscriber.name} handler is turning off...")
        if self.subscription:
            await asyncio.to_thread(self.subscription.shutdown, timeout)
        if self.tasks:
            await asyncio.wait(set(self.tasks), timeout=timeout)
        logger.info(f"The {self.subscriber.name} handler is shutdown...")
