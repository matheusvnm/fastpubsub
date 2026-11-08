"""Benchmark cases that use the google-cloud-pubsub library directly.

The handler runs in a thread of the client. It waits with ``time.sleep``.

The blocking case does the same work as FastPubSub for each message. It
publishes the message, waits for the publish result, acknowledges the
message and waits for the acknowledge result.

The non-blocking case never waits for a result in the callback. It
acknowledges the message when the publish completes. It shows the upper
limit of the google-cloud-pubsub library for this pattern.
"""

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from concurrent.futures import Future, ThreadPoolExecutor, wait
from contextlib import asynccontextmanager, suppress

from google.cloud.pubsub import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage
from google.cloud.pubsub_v1.subscriber.scheduler import ThreadScheduler
from google.cloud.pubsub_v1.types import FlowControl

from benchmarks.resources import RunResources
from benchmarks.workload import (
    MAX_MESSAGES,
    THREAD_LIMIT,
    Recorder,
    Scenario,
    build_attributes,
    run_cpu_work,
)

logging.getLogger("google").setLevel(logging.CRITICAL)


class GoogleBlockingCase:
    """The google-cloud-pubsub client with blocking waits in the callback."""

    name = "google-blocking"
    description = (
        f"google-cloud-pubsub, {THREAD_LIMIT} threads, waits for publish "
        "and ack results"
    )

    def __init__(
        self,
        scenario: Scenario,
        cpu_iterations: int,
        resources: RunResources,
        recorder: Recorder,
    ) -> None:
        self.scenario = scenario
        self.cpu_iterations = cpu_iterations
        self.resources = resources
        self.recorder = recorder
        self.publisher_client = PublisherClient()

    def _run_workload(self, message: PubSubMessage) -> None:
        self.recorder.record(message.message_id, dict(message.attributes))
        if self.scenario.cpu_milliseconds:
            run_cpu_work(self.cpu_iterations)
        if self.scenario.wait_milliseconds:
            time.sleep(self.scenario.wait_seconds)

    def _publish(self, data: bytes) -> Future[str]:
        future: Future[str] = self.publisher_client.publish(
            topic=self.resources.topic_path,
            data=data,
            **build_attributes(),
        )
        return future

    def _on_message(self, message: PubSubMessage) -> None:
        self._run_workload(message)
        self._publish(message.data).result()
        message.ack_with_response().result()

    @asynccontextmanager
    async def run(self) -> AsyncIterator[None]:
        """Start the echo loop and stop it at exit."""
        subscriber_client = SubscriberClient()
        streaming_pull_future: StreamingPullFuture = (
            subscriber_client.subscribe(
                subscription=self.resources.subscription_path,
                callback=self._on_message,
                flow_control=FlowControl(max_messages=MAX_MESSAGES),
                scheduler=ThreadScheduler(
                    executor=ThreadPoolExecutor(max_workers=THREAD_LIMIT)
                ),
                await_callbacks_on_shutdown=True,
            )
        )
        try:
            payload = self.scenario.build_payload()
            initial_futures = [
                self._publish(payload)
                for _ in range(self.scenario.in_flight_messages)
            ]
            await asyncio.to_thread(wait, initial_futures, timeout=60)
            yield
        finally:
            streaming_pull_future.cancel()
            with suppress(Exception):
                await asyncio.to_thread(
                    streaming_pull_future.result, timeout=30
                )
            subscriber_client.close()
            self.publisher_client.transport.close()


class GoogleNonBlockingCase(GoogleBlockingCase):
    """The google-cloud-pubsub client without waits for results."""

    name = "google-nonblocking"
    description = (
        f"google-cloud-pubsub, {THREAD_LIMIT} threads, acks when the "
        "publish completes (upper limit)"
    )

    def _on_message(self, message: PubSubMessage) -> None:
        self._run_workload(message)
        future = self._publish(message.data)
        future.add_done_callback(lambda _: message.ack())
