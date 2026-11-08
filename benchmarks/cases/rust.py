"""Benchmark case that uses FastPubSub with the Rust transport.

The case keeps the FastPubSub Python path: the broker, the middleware
chain, the ``Message`` object, the log context and the publisher chain.
Only the transport changes. The ``fastpubsub_core`` module pulls the
messages, sends them to the event loop in batches, and publishes.
"""

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import patch

from fastpubsub_core import AckHandle, Publisher, Subscription

from benchmarks.cases.fastpubsub import FastPubSubAsyncCase
from benchmarks.resources import PROJECT_ID
from benchmarks.workload import MAX_MESSAGES, build_attributes
from fastpubsub.concurrency.tasks import logger
from fastpubsub.datastructures import Message
from fastpubsub.exceptions import Drop, Retry
from fastpubsub.pubsub.subscriber import Subscriber

MAX_BATCH_SIZE = 256

type ReceivedMessage = tuple[str, bytes, dict[str, str], int, AckHandle]


def read_endpoint() -> str | None:
    """Read the emulator endpoint.

    Returns:
        The emulator URL, or None for the real Pub/Sub service.
    """
    emulator_host = os.getenv("PUBSUB_EMULATOR_HOST")
    if not emulator_host:
        return None
    return f"http://{emulator_host}"


def resolve_futures(
    outcomes: list[tuple[asyncio.Future[str], bool, str]],
) -> None:
    """Complete a batch of publish futures on the event loop.

    Args:
        outcomes: For each publish: the future, True when the publish
            succeeded, and the message id or the error text.
    """
    for future, is_success, value in outcomes:
        if future.done():
            continue
        if is_success:
            future.set_result(value)
        else:
            future.set_exception(RuntimeError(value))


class RustPubSubClient:
    """A replacement for ``PubSubClient`` that publishes through Rust."""

    publishers: dict[str, Publisher] = {}

    def __init__(self, project_id: str) -> None:
        self.project_id = project_id

    def _get_publisher(self, topic_name: str) -> Publisher:
        topic_path = f"projects/{self.project_id}/topics/{topic_name}"
        publisher = self.publishers.get(topic_path)
        if publisher is None:
            loop = asyncio.get_running_loop()
            publisher = Publisher(
                topic_path,
                MAX_BATCH_SIZE,
                loop.call_soon_threadsafe,
                resolve_futures,
                endpoint=read_endpoint(),
            )
            self.publishers[topic_path] = publisher
        return publisher

    async def create_topic(self, topic_name: str) -> None:
        """Do nothing. The benchmark creates its topics."""

    async def publish(
        self,
        topic_name: str,
        *,
        data: bytes,
        ordering_key: str,
        attributes: dict[str, str] | None,
    ) -> None:
        """Publish a message through Rust and wait for the message id."""
        publisher = self._get_publisher(topic_name)
        future: asyncio.Future[str] = (
            asyncio.get_running_loop().create_future()
        )
        publisher.publish(data, attributes or {}, future, ordering_key)
        message_id = await future
        logger.info(
            f"Message published for topic {topic_name} with id {message_id}"
        )


class RustDispatcher:
    """Run the FastPubSub handler for each message that Rust delivers."""

    def __init__(self, subscriber: Subscriber) -> None:
        self.subscriber = subscriber
        self.loop = asyncio.get_running_loop()
        self.tasks: set[asyncio.Task[Any]] = set()

    def dispatch(self, batch: list[ReceivedMessage]) -> None:
        """Create one task for each message of a batch.

        Args:
            batch: The messages that Rust received.
        """
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

    async def _consume(self, message: Message, ack: AckHandle) -> None:
        with logger.contextualize(
            message_id=message.id,
            topic_name=message.topic_name,
            subscriber_name=message.subscriber_name,
        ):
            try:
                await self.subscriber.callstack.on_message(message)
                ack.ack()
                logger.info("The message successfully processed.")
            except Drop:
                ack.ack()
                logger.info("The message will be dropped.")
            except Retry:
                ack.nack()
                logger.warning("The message will be retried later.")
            except Exception:
                ack.nack()
                logger.exception("Unhandled exception on message")


class FastPubSubRustCase(FastPubSubAsyncCase):
    """FastPubSub with an ``async def`` handler and the Rust transport."""

    name = "fastpubsub-rust"
    description = "FastPubSub, async def handler, Rust transport"

    @asynccontextmanager
    async def run(self) -> AsyncIterator[None]:
        """Start the echo loop and stop it at exit."""
        self._register_handler()
        (subscriber,) = self.broker.router.get_subscribers().values()
        dispatcher = RustDispatcher(subscriber)
        client_patch = patch(
            "fastpubsub.middlewares.di.PubSubClient", RustPubSubClient
        )
        client_patch.start()
        subscription = Subscription(
            f"projects/{PROJECT_ID}/subscriptions/"
            f"{self.resources.subscription_name}",
            MAX_MESSAGES,
            MAX_BATCH_SIZE,
            asyncio.get_running_loop().call_soon_threadsafe,
            dispatcher.dispatch,
            endpoint=read_endpoint(),
        )
        try:
            payload = self.scenario.build_payload()
            await asyncio.gather(
                *(
                    self.publisher.publish(
                        payload,
                        attributes=build_attributes(),
                        autocreate=False,
                    )
                    for _ in range(self.scenario.in_flight_messages)
                )
            )
            yield
        finally:
            subscription.stop()
            client_patch.stop()
            RustPubSubClient.publishers.clear()
