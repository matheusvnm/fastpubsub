"""Basic benchmark case using FastPubSub framework.

This case measures FastPubSub's performance by creating an echo loop:
1. Subscribe to a topic
2. When a message is received, publish it back to the same topic
3. Count each message processed

This creates an infinite loop of messages, allowing us to measure
Events Per Second (EPS) for the FastPubSub framework.
"""

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastpubsub import Message, PubSubBroker

# Disable logging for accurate benchmark timing
logging.getLogger("fastpubsub").setLevel(logging.CRITICAL)
logging.getLogger("google").setLevel(logging.CRITICAL)

# Benchmark configuration
PROJECT_ID = "fastpubsub-benchmark"
TOPIC_NAME = "bench-topic"
SUBSCRIPTION_NAME = "bench-subscription"

# Test message payload (consistent with FastStream benchmarks)
TEST_MESSAGE = {
    "name": "John",
    "age": 39,
    "fullname": "LongString" * 8,
    "children": [{"name": "Mike", "age": 8, "fullname": "LongString" * 8}],
}


class BasicTestCase:
    """Benchmark case for FastPubSub Message processing.

    This measures the performance of FastPubSub's message handling
    without any additional processing or validation.
    """

    case_name = "basic"
    description = "FastPubSub Message processing"

    def __init__(self) -> None:
        """Initialize the benchmark case."""
        self.EVENTS_PROCESSED = 0
        self._broker: PubSubBroker | None = None
        self._shutdown_event: asyncio.Event | None = None

    def _setup_broker(self) -> PubSubBroker:
        """Create and configure the broker with echo subscriber.

        Returns:
            PubSubBroker: Configured broker instance.
        """
        # Create broker with logging disabled for accurate timing
        broker = PubSubBroker(project_id=PROJECT_ID)

        # Get publisher for echo responses
        publisher = broker.publisher(TOPIC_NAME)

        # Reference to self for closure
        test_case = self

        @broker.subscriber(
            alias="benchmark",
            topic_name=TOPIC_NAME,
            subscription_name=SUBSCRIPTION_NAME,
            autocreate=True,
            max_messages=1000,  # Flow control
            ack_deadline_seconds=60,
        )
        async def handle(message: Message) -> None:
            """Handle incoming message and echo it back."""
            test_case.EVENTS_PROCESSED += 1
            # Echo message back to create infinite loop
            await publisher.publish(message.data)

        return broker

    @asynccontextmanager
    async def start(self) -> AsyncIterator[float]:
        """Start the benchmark.

        Sets up the broker, starts message processing, and publishes
        the initial message to start the echo loop.

        Yields:
            float: Timestamp when the benchmark started.
        """
        self.EVENTS_PROCESSED = 0
        self._shutdown_event = asyncio.Event()
        self._broker = self._setup_broker()

        try:
            # Start the broker (creates subscriptions and starts pulling)
            await self._broker.start()

            # Record start time
            start_time = time.time()

            # Publish initial message to start the echo loop
            publisher = self._broker.publisher(TOPIC_NAME)
            await publisher.publish(TEST_MESSAGE)

            yield start_time

        finally:
            # Shutdown the broker
            if self._broker:
                self._broker.shutdown()
