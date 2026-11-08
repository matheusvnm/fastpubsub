"""Benchmark cases that use FastPubSub.

The async case uses an ``async def`` handler. It waits with
``asyncio.sleep`` on the event loop.

The sync case uses a ``def`` handler. FastPubSub runs it in the thread
pool. It waits with ``time.sleep`` and publishes through the event loop.
"""

import asyncio
import functools
import logging
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import anyio.from_thread

from benchmarks.resources import PROJECT_ID, RunResources
from benchmarks.workload import (
    ACK_DEADLINE_SECONDS,
    MAX_MESSAGES,
    Recorder,
    Scenario,
    build_attributes,
    run_cpu_work,
)
from fastpubsub import Message, PubSubBroker

logging.getLogger("fastpubsub").setLevel(logging.CRITICAL)
logging.getLogger("google").setLevel(logging.CRITICAL)


class FastPubSubAsyncCase:
    """FastPubSub with an ``async def`` handler."""

    name = "fastpubsub-async"
    description = "FastPubSub, async def handler on the event loop"

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
        self.broker = PubSubBroker(project_id=PROJECT_ID)
        self.publisher = self.broker.publisher(resources.topic_name)

    def _register_handler(self) -> None:
        @self.broker.subscriber(
            alias="benchmark",
            topic_name=self.resources.topic_name,
            subscription_name=self.resources.subscription_name,
            autocreate=False,
            autoupdate=False,
            max_messages=MAX_MESSAGES,
            ack_deadline_seconds=ACK_DEADLINE_SECONDS,
        )
        async def handle(message: Message) -> None:
            self.recorder.record(message.id, message.attributes)
            if self.scenario.cpu_milliseconds:
                run_cpu_work(self.cpu_iterations)
            if self.scenario.wait_milliseconds:
                await asyncio.sleep(self.scenario.wait_seconds)
            await self.publisher.publish(
                message.data, attributes=build_attributes(), autocreate=False
            )

    @asynccontextmanager
    async def run(self) -> AsyncIterator[None]:
        """Start the echo loop and stop it at exit."""
        self._register_handler()
        await self.broker.start()
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
            await self.broker.shutdown()


class FastPubSubSyncCase(FastPubSubAsyncCase):
    """FastPubSub with a ``def`` handler in the thread pool."""

    name = "fastpubsub-sync"
    description = "FastPubSub, def handler in the anyio thread pool"

    def _register_handler(self) -> None:
        @self.broker.subscriber(
            alias="benchmark",
            topic_name=self.resources.topic_name,
            subscription_name=self.resources.subscription_name,
            autocreate=False,
            autoupdate=False,
            max_messages=MAX_MESSAGES,
            ack_deadline_seconds=ACK_DEADLINE_SECONDS,
        )
        def handle(message: Message) -> None:
            self.recorder.record(message.id, message.attributes)
            if self.scenario.cpu_milliseconds:
                run_cpu_work(self.cpu_iterations)
            if self.scenario.wait_milliseconds:
                time.sleep(self.scenario.wait_seconds)
            anyio.from_thread.run(
                functools.partial(
                    self.publisher.publish,
                    message.data,
                    attributes=build_attributes(),
                    autocreate=False,
                )
            )
