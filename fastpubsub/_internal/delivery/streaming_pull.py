"""The task that receives the messages of one subscriber."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import TYPE_CHECKING, Any, cast

from fastpubsub_core import AckHandle, Subscription

from fastpubsub._internal.connection import Connection
from fastpubsub._internal.event_loop_bridge import EventLoopBridge
from fastpubsub.datastructures import Message
from fastpubsub.exceptions import Drop, Retry
from fastpubsub.logger import FastPubSubLogger

if TYPE_CHECKING:
    from fastpubsub.pubsub.subscriber import Subscriber

logger: FastPubSubLogger = cast(FastPubSubLogger, logging.getLogger(__name__))

type ReceivedMessage = tuple[str, bytes, dict[str, str], int, AckHandle]


class PubSubStreamingPullTask:
    """A task that runs the handler for each message that the core receives.

    The core gives the server twice the ``max_messages`` limit, because
    the server stops the stream too early at the exact limit. The task
    keeps the ``max_messages`` limit itself: it runs at most that many
    handlers at the same time, and the other messages wait in a queue.
    """

    def __init__(self, subscriber: Subscriber) -> None:
        """Initializes the PubSubStreamingPullTask.

        Args:
            subscriber: The subscriber to receive messages for.
        """
        self.loop = asyncio.get_running_loop()
        self.subscriber = subscriber
        self.subscription: Subscription | None = None
        self.tasks: set[asyncio.Task[Any]] = set()
        self.waiting: deque[ReceivedMessage] = deque()
        self.is_closing = False

    async def start(self) -> None:
        """Starts the message stream."""
        logger.info(
            f"The {self.subscriber.name} handler is waiting for messages."
        )
        self.subscription = await asyncio.to_thread(
            Subscription,
            Connection(self.subscriber.project_id).subscription_path(
                self.subscriber.subscription_name
            ),
            self.subscriber.control_flow_policy.max_messages,
            EventLoopBridge.get(),
            self._dispatch,
            Connection.read_endpoint(),
        )

    def _dispatch(self, batch: list[ReceivedMessage]) -> None:
        """Receives a batch of messages from the core.

        The messages wait in a queue. During the shutdown, the task nacks
        them at once. Otherwise it starts handlers up to ``max_messages``.

        Args:
            batch: The received messages.
        """
        self.waiting.extend(batch)
        if self.is_closing:
            self._nack_waiting()
            return
        self._start_waiting()

    def _nack_waiting(self) -> None:
        """Nacks all messages in the queue, so Pub/Sub sends them again."""
        while self.waiting:
            *_, ack = self.waiting.popleft()
            ack.nack()

    def _start_waiting(self) -> None:
        """Starts handlers for the waiting messages.

        At most ``max_messages`` handlers run at the same time.
        """
        limit = self.subscriber.control_flow_policy.max_messages
        while self.waiting and len(self.tasks) < limit:
            message_id, data, attributes, delivery_attempt, ack = (
                self.waiting.popleft()
            )
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
            task = asyncio.eager_task_factory(
                self.loop, self._consume(message, ack)
            )
            self.tasks.add(task)
            task.add_done_callback(self._finish_task)

    def _finish_task(self, task: asyncio.Task[Any]) -> None:
        """Removes a finished handler and starts the next waiting message.

        Args:
            task: The finished handler task.
        """
        self.tasks.discard(task)
        if not self.is_closing:
            self._start_waiting()

    async def _consume(self, message: Message, ack: AckHandle) -> Any:
        """Runs the handler and acks or nacks the message.

        A success or ``Drop`` acks the message. ``Retry`` or a different
        exception nacks it.

        Args:
            message: The message for the handler.
            ack: The acknowledgement handle of the message.

        Returns:
            The return value of the handler, or None on an error.
        """
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
        self.is_closing = True
        self._nack_waiting()
        if self.subscription:
            await asyncio.to_thread(self.subscription.shutdown, timeout)
        if self.tasks:
            await asyncio.wait(set(self.tasks), timeout=timeout)
        logger.info(f"The {self.subscriber.name} handler is shutdown...")
