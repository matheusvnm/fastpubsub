"""Task manager for subscriber tasks."""

import asyncio

from fastpubsub.concurrency.tasks import PubSubPollTask
from fastpubsub.pubsub.subscriber import Subscriber


class AsyncTaskManager:
    """Public-facing controller for managing a fleet of subscriber tasks."""

    def __init__(self) -> None:
        """Initializes the AsyncTaskManager."""
        self._tasks: list[PubSubPollTask] = []
        self._running_tasks: dict[PubSubPollTask, asyncio.Task[None]] = {}

    async def create_task(self, subscriber: Subscriber) -> None:
        """Registers a subscriber configuration to be managed."""
        self._tasks.append(PubSubPollTask(subscriber))

    async def start(self) -> None:
        """Starts the subscribers tasks process using a task group."""
        for polltask in self._tasks:
            atask = asyncio.create_task(polltask.start())
            self._running_tasks[polltask] = atask

    async def alive(self) -> dict[str, bool]:
        """Checks if the tasks are alive.

        Returns:
            A dictionary mapping task names to their liveness status.
        """
        liveness: dict[str, bool] = {}
        for polltask, atask in self._running_tasks.items():
            liveness[polltask.subscriber.name] = (
                bool(atask) and not atask.done() and polltask.task_alive()
            )
        return liveness

    async def ready(self) -> dict[str, bool]:
        """Checks if the tasks are ready.

        Returns:
            A dictionary mapping task names to their readiness status.
        """
        readiness: dict[str, bool] = {}
        for polltask, atask in self._running_tasks.items():
            readiness[polltask.subscriber.name] = (
                bool(atask) and not atask.done() and polltask.task_ready()
            )
        return readiness

    async def shutdown(self) -> None:
        """Terminates the manager process and all its children gracefully."""
        if self._running_tasks:
            for atask in self._running_tasks.values():
                atask.cancel()

            self._running_tasks.clear()
