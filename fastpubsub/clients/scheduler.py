"""Schedulers for allocating messages from clients to handlers."""

import asyncio
import functools
import logging
import queue
import threading
import warnings
from asyncio.events import AbstractEventLoop
from collections.abc import Awaitable, Callable
from typing import Any, cast
from weakref import WeakKeyDictionary

from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage
from google.cloud.pubsub_v1.subscriber.scheduler import Scheduler

logger = logging.getLogger(__name__)


class AsyncScheduler(Scheduler):  # type: ignore[misc]
    """An asyncio-based scheduler for typical I/O-bound message processing.

    It must not be shared across different SubscriberClient objects.
    """

    def __init__(self, loop: AbstractEventLoop) -> None:
        """Initializes an asyncio-based schedule for typical I/O-bound message processing."""
        self._loop = loop

        self._queue: queue.Queue[Any] = queue.Queue()

        # Track scheduled handles (pending callbacks not yet executed)
        self._pending_task_creations: WeakKeyDictionary[asyncio.Handle, PubSubMessage] = (
            WeakKeyDictionary()
        )

        # Track executing tasks (running coroutines)
        self._executing_tasks: dict[int, PubSubMessage] = {}
        self._executing_lock = threading.Lock()

        # Tracks if the tasks can be executed (before closing).
        self.closed = False

    @property
    def queue(self) -> queue.Queue[Any]:
        """A thread-safe queue for communication between callbacks and the scheduling thread."""
        return self._queue

    def schedule(
        self, callback: Callable[[PubSubMessage], Any], *args: list[Any], **kwargs: dict[str, Any]
    ) -> None:
        """Schedule the callback to be called asynchronously in the event loop thread.

        Args:
            callback: The function to call.
            args: Positional arguments passed to the callback.
            kwargs: Key-word arguments passed to the callback.
        """
        try:
            message = cast(PubSubMessage, args[0])
            if self.closed:
                logger.debug(
                    f"The message {message.message_id} will be nacked. "
                    "The subscriber is shutting down..."
                )
                message.nack()
                return

            wrapped_callback = functools.partial(callback, message)
            pending_task = self._loop.call_soon_threadsafe(wrapped_callback)
            self._pending_task_creations[pending_task] = message
        except RuntimeError:
            warnings.warn(
                "Scheduling a callback after executor shutdown.",
                category=RuntimeWarning,
                stacklevel=2,
            )

    def register_task_execution(
        self, task: asyncio.Task[Callable[[PubSubMessage], Awaitable[Any]]], message: PubSubMessage
    ) -> None:
        """Register a task for tracking.

        This should be called by the callback when it creates a task.

        Args:
            task: The asyncio.Task to track.
            message: The PubSubMessage being processed.
        """
        task_id = id(task)
        with self._executing_lock:
            self._executing_tasks[task_id] = message

        # Add done callback to automatically remove from tracking when complete
        task.add_done_callback(lambda t: self.deregister_executed_task(t))

    def deregister_executed_task(
        self, task: asyncio.Task[Callable[[PubSubMessage], Awaitable[Any]]]
    ) -> None:
        """Called when a task completes - removes it from tracking.

        Args:
            task: The completed task
        """
        with self._executing_lock:
            self._executing_tasks.pop(id(task), None)

    def get_in_flight_count(self) -> tuple[int, int]:
        """Get the count of in-flight messages.

        Returns:
            A tuple of (pending_handles, executing_tasks) counts.
        """
        pending = len(self._pending_task_creations)
        with self._executing_lock:
            executing = len(self._executing_tasks)
        return pending, executing

    async def wait_for_completion(self, timeout: float = 30.0) -> bool:
        """Wait for all in-flight messages to complete.

        Args:
            timeout: Maximum time to wait (seconds).

        Returns:
            True if all messages completed, False if timeout occurred.
        """
        self.closed = True
        start_time = self._loop.time()

        while self._loop.time() - start_time < timeout:
            pending, executing = self.get_in_flight_count()

            if pending == 0 and executing == 0:
                logger.debug("All asynchronous tasks were completed successfully.")
                return True

            logger.debug(
                f"Waiting for {pending} pending and {executing} executing asynchronous tasks."
            )
            await asyncio.sleep(0.5)

        logger.warning(f"Timeout after {timeout}s waiting for messages completion")
        return False

    def shutdown(self, await_msg_callbacks: bool = True) -> list[PubSubMessage]:
        """Shuts down the scheduler and cancels executing tasks.

        Args:
            await_msg_callbacks:
                If ``True`` (default), the method will cancel the executing callbacks remaining.
                This will allow a graceful termination of the messages execution.
                If ``False``, the method will not cancel the callbacks.

        Returns:
            The messages dispatched to the asyncio loop that are currently
            executed but did not complete yet.
        """
        dropped_messages = []

        pending_items = list(self._pending_task_creations.items())
        for handle, message in pending_items:
            if not handle.cancelled():
                dropped_messages.append(message)
                if await_msg_callbacks:
                    handle.cancel()

        with self._executing_lock:
            for message in self._executing_tasks.values():
                dropped_messages.append(message)

        if dropped_messages:
            logger.warning(f"Scheduler shutdown: {len(dropped_messages)} messages will be nacked.")
            return dropped_messages

        logger.debug("Scheduler shutdown: All asynchronous tasks completed successfully.")
        return dropped_messages
