"""Schedulers for allocating messages from clients to handlers."""

import asyncio
import functools
import queue
import warnings
from collections.abc import Callable
from typing import Any, cast
from weakref import WeakKeyDictionary

from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage
from google.cloud.pubsub_v1.subscriber.scheduler import Scheduler


class AsyncScheduler(Scheduler):  # type: ignore[misc]
    """An asyncio-based scheduler for typical I/O-bound message processing.

    It must not be shared across different SubscriberClient objects.
    """

    def __init__(self) -> None:
        """Initializes an asyncio-based schedule for typical I/O-bound message processing."""
        self._queue: queue.Queue[Any] = queue.Queue()
        self._loop: asyncio.AbstractEventLoop = asyncio.get_running_loop()
        self._tasks: WeakKeyDictionary[asyncio.Handle, PubSubMessage] = WeakKeyDictionary()

    @property
    def queue(self) -> queue.Queue[Any]:
        """A thread-safe queue for communication between callbacks and the scheduling thread."""
        return self._queue

    def schedule(
        self, callback: Callable[[Any], Any], *args: list[Any], **kwargs: dict[str, Any]
    ) -> None:
        """Schedule the callback to be called asynchronously in the event loop thread.

        Args:
            callback: The function to call.
            args: Positional arguments passed to the callback.
            kwargs: Key-word arguments passed to the callback.
        """
        try:
            wrapped_callback = functools.partial(callback, *args, **kwargs)
            handle = self._loop.call_soon_threadsafe(wrapped_callback)

            self._tasks[handle] = cast(PubSubMessage, args[0])
        except RuntimeError:
            warnings.warn(
                "Scheduling a callback after executor shutdown.",
                category=RuntimeWarning,
                stacklevel=2,
            )

    def shutdown(self, await_msg_callbacks: bool = True) -> list[PubSubMessage]:
        """Shuts down the scheduler and immediately cancel all executing tasks.

        Args:
            await_msg_callbacks:
                If ``True`` (default), the method will cancel the executing callbacks remaining.
                This will allow a graceful termination of the messages execution.
                If ``False``, the method will not cancel the callbacks.

        Returns:
            The messages dispatched to the asyncio loop that are currently
            executed but did not completed yet.
        """
        dropped_messages = []
        for handle, message in self._tasks.items():
            dropped_messages.append(message)
            if not handle.cancelled() and await_msg_callbacks:
                handle.cancel()

        return dropped_messages
