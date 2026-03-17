"""Baseline google-cloud-pubsub benchmark case.

This case measures the performance of using the google-cloud-pubsub
library directly, without FastPubSub. It serves as a baseline to
measure the overhead that FastPubSub adds.

The echo pattern is the same as the basic case:
1. Subscribe to a topic
2. When a message is received, publish it back to the same topic
3. Count each message processed
"""

import asyncio
import json
import logging
import queue
import time
from collections.abc import AsyncIterator
from concurrent.futures import Future
from contextlib import asynccontextmanager, suppress

from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage
from google.cloud.pubsub_v1.types import FlowControl

# Disable logging for accurate benchmark timing
logging.getLogger("google").setLevel(logging.CRITICAL)

# Benchmark configuration (same as basic case)
PROJECT_ID = "fastpubsub-benchmark"
TOPIC_NAME = "bench-baseline-topic"
SUBSCRIPTION_NAME = "bench-baseline-subscription"

# Test message payload (consistent with FastStream benchmarks)
TEST_MESSAGE = {
    "name": "John",
    "age": 39,
    "fullname": "LongString" * 8,
    "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
}


class BaselinePubSubTestCase:
    """Baseline benchmark using pure google-cloud-pubsub library.

    This measures the baseline performance of the google-cloud-pubsub
    library without any FastPubSub overhead.
    """

    case_name = "baseline"
    description = "Pure google-cloud-pubsub (baseline)"

    def __init__(self, num_msgs: int) -> None:
        """Initialize the benchmark case."""
        self.num_msgs = num_msgs
        self._EVENTS_QUEUE: queue.Queue[int] = queue.Queue()
        self._subscriber_client: SubscriberClient | None = None
        self._publisher_client: PublisherClient | None = None
        self._streaming_pull_future: StreamingPullFuture | None = None

    def _create_topic(self) -> str:
        """Create the benchmark topic if it doesn't exist.

        Returns:
            str: The topic path.
        """
        topic_path: str = PublisherClient.topic_path(PROJECT_ID, TOPIC_NAME)

        with suppress(AlreadyExists):
            self._publisher_client.create_topic(name=topic_path)  # type: ignore[union-attr]

        return topic_path

    def _create_subscription(self, topic_path: str) -> str:
        """Create the benchmark subscription if it doesn't exist.

        Args:
            topic_path: The topic path to subscribe to.

        Returns:
            str: The subscription path.
        """
        subscription_path: str = SubscriberClient.subscription_path(
            PROJECT_ID, SUBSCRIPTION_NAME
        )

        with suppress(AlreadyExists):
            self._subscriber_client.create_subscription(  # type: ignore[union-attr]
                name=subscription_path,
                topic=topic_path,
            )

        return subscription_path

    def _on_message(self, message: PubSubMessage) -> None:
        """Handle incoming message callback.

        Args:
            message: The received PubSub message.
        """
        self._EVENTS_QUEUE.put_nowait(1)

        # Acknowledge the message just like FastPubSub
        message.ack_with_response()

        # Echo message back to create infinite loop (Do not create topic)
        topic_path = PublisherClient.topic_path(PROJECT_ID, TOPIC_NAME)
        future: Future[str] = self._publisher_client.publish(  # type: ignore[union-attr]
            topic=topic_path,
            data=message.data,
        )

        # On FastPubSub we always wait
        future.result()

    @asynccontextmanager
    async def start(self) -> AsyncIterator[float]:
        """Start the benchmark.

        Sets up the PubSub clients, creates topic/subscription,
        starts subscribing, and publishes the initial message.

        Yields:
            float: Timestamp when the benchmark started.
        """

        # Create clients
        self._publisher_client = PublisherClient()
        self._subscriber_client = SubscriberClient()

        try:
            # Create topic and subscription
            topic_path = self._create_topic()
            subscription_path = self._create_subscription(topic_path)

            # Start subscribing
            self._streaming_pull_future = self._subscriber_client.subscribe(
                subscription=subscription_path,
                callback=self._on_message,
                flow_control=FlowControl(max_messages=1000),
            )

            # Give subscription time to establish
            await asyncio.sleep(0.5)

            # Record start time
            start_time = time.time()

            # Publish initial messages to start the echo loop

            for _ in range(self.num_msgs):
                initial_message = json.dumps(TEST_MESSAGE).encode()
                future: Future[str] = self._publisher_client.publish(
                    topic=topic_path,
                    data=initial_message,
                )
                future.result(timeout=10)  # Wait for initial publish

            yield start_time

        finally:
            # Cancel the streaming pull
            if self._streaming_pull_future:
                self._streaming_pull_future.cancel()
                self._streaming_pull_future = None

            # Close clients
            if self._subscriber_client:
                self._subscriber_client.close()
                self._subscriber_client = None

            if self._publisher_client:
                self._publisher_client = None

    def get_total_processed_msgs(self) -> int:
        """Get the sum of processed messages.

        Returns:
            The total number of processed messages.
        """

        processed_messages = 0
        while True:
            try:
                processed_messages += self._EVENTS_QUEUE.get_nowait()
            except queue.Empty:
                break
        return processed_messages
