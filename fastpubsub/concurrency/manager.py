"""Task manager for subscriber tasks."""

import asyncio

from fastpubsub.concurrency.tasks import PollerTask, PubSubPullTask, PubSubStreamingPullTask
from fastpubsub.datastructures import PullMethod
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.pubsub.subscriber import Subscriber


class AsyncTaskManager:
    """Public-facing controller for managing a fleet of subscriber tasks."""

    def __init__(self) -> None:
        """Initializes the AsyncTaskManager."""
        self._tasks: list[PollerTask] = []

    async def create_task(self, subscriber: Subscriber) -> None:
        """Registers a subscriber configuration to be managed."""
        method = subscriber.delivery_policy.pull_method
        match method:
            case PullMethod.UNARY_PULL:
                self._tasks.append(PubSubPullTask(subscriber))
            case PullMethod.STREAMING_PULL:
                self._tasks.append(PubSubStreamingPullTask(subscriber))
            case _:
                raise FastPubSubException(f"The pull method {method} is not supported.")

    async def start(self) -> None:
        """Starts the subscribers tasks process using a task group."""
        for pull_task in self._tasks:
            asyncio.create_task(pull_task.start())

    async def alive(self) -> dict[str, bool]:
        """Checks if the tasks are alive.

        Returns:
            A dictionary mapping task names to their liveness status.
        """
        liveness: dict[str, bool] = {}
        for pull_task in self._tasks:
            liveness[pull_task.subscriber.name] = pull_task.task_alive()
        return liveness

    async def ready(self) -> dict[str, bool]:
        """Checks if the tasks are ready.

        Returns:
            A dictionary mapping task names to their readiness status.
        """
        readiness: dict[str, bool] = {}
        for pull_task in self._tasks:
            readiness[pull_task.subscriber.name] = pull_task.task_ready()
        return readiness

    async def shutdown(self) -> None:
        """Terminates the manager process and all its children gracefully."""
        for pull_task in self._tasks:
            pull_task.shutdown()

        self._tasks.clear()
