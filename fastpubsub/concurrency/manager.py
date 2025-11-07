"""Task manager for subscriber tasks."""

from fastpubsub.concurrency.tasks import PubSubStreamingPullTask
from fastpubsub.pubsub.subscriber import Subscriber


class AsyncTaskManager:
    """Public-facing controller for managing a fleet of subscriber tasks."""

    def __init__(self) -> None:
        """Initializes the AsyncTaskManager."""
        self._tasks: list[PubSubStreamingPullTask] = []

    def create_task(self, subscriber: Subscriber) -> None:
        """Registers a subscriber configuration to be managed."""
        self._tasks.append(PubSubStreamingPullTask(subscriber))

    def start(self) -> None:
        """Starts the subscribers tasks process using a task group."""
        for task in self._tasks:
            task.start()

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

    def shutdown(self) -> None:
        """Terminates the manager process and all its children gracefully."""
        for task in self._tasks:
            task.shutdown()

        self._tasks.clear()
