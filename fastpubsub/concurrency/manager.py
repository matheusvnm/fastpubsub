"""Task manager for subscriber tasks."""

import asyncio
import logging
from typing import cast

from fastpubsub.clients.factory import PubSubClientFactory
from fastpubsub.concurrency.tasks import PubSubStreamingPullTask
from fastpubsub.logger import FastPubSubLogger
from fastpubsub.pubsub.subscriber import Subscriber

logger: FastPubSubLogger = cast(FastPubSubLogger, logging.getLogger(__name__))


class AsyncTaskManager:
    """Public-facing controller for managing a fleet of subscriber tasks."""

    def __init__(self) -> None:
        """Initializes the AsyncTaskManager."""
        self._tasks: list[PubSubStreamingPullTask] = []

    def create_task(self, subscriber: Subscriber) -> None:
        """Registers a subscriber configuration to be managed."""
        self._tasks.append(PubSubStreamingPullTask(subscriber))

    async def start(self) -> None:
        """Starts the subscribers tasks process using a task group."""
        for task in self._tasks:
            await task.start()

    def alive(self) -> dict[str, bool]:
        """Checks if the tasks are alive.

        Returns:
            A dictionary mapping task names to their liveness status.
        """
        liveness: dict[str, bool] = {}
        for pull_task in self._tasks:
            liveness[pull_task.subscriber.name] = pull_task.task_alive()
        return liveness

    def ready(self) -> dict[str, bool]:
        """Checks if the tasks are ready.

        Returns:
            A dictionary mapping task names to their readiness status.
        """
        readiness: dict[str, bool] = {}
        for task in self._tasks:
            readiness[task.subscriber.name] = task.task_ready()
        return readiness

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Gracefully shuts down all tasks, waiting for message completion.

        Two-step process:
        1. Cancel all StreamingPullFutures to stop NEW messages from arriving
        2. Wait for IN-FLIGHT messages to complete (or timeout)

        Args:
            timeout: Maximum time to wait for in-flight messages per subscription (seconds).
        """
        logger.info(f"Starting graceful shutdown with {timeout}s timeout...")

        try:
            async with asyncio.timeout(delay=timeout):
                async with asyncio.TaskGroup() as tg:
                    for task in self._tasks:
                        if task.task_alive():
                            tg.create_task(task.shutdown(timeout=timeout))
        except TimeoutError as e:
            logger.warning(f"A timeout happened while turning of a subscriber {e}")
        finally:
            self._tasks.clear()
            await PubSubClientFactory.close_all()
