"""The bridges between the core and the asyncio event loops."""

import asyncio
import logging
from collections.abc import Callable
from typing import Any, ClassVar

from fastpubsub_core import Bridge

logger = logging.getLogger(__name__)


class EventLoopBridge:
    """Keeps one core ``Bridge`` for each asyncio event loop.

    A core ``Bridge`` owns one thread. The thread collects the received
    messages and the publish results, and gives them to its event loop in
    batches, through ``call_soon_threadsafe``. The loop then runs
    ``run_callbacks`` with the batch.

    The class needs no lock on free-threaded Python. Only the thread of an
    event loop creates or reads the entry of that loop, and each single
    ``dict`` operation is atomic.
    """

    MAX_BATCH_SIZE: ClassVar[int] = 256
    _bridges: ClassVar[dict[asyncio.AbstractEventLoop, Bridge]] = {}

    @classmethod
    def get(cls) -> Bridge:
        """Gets or creates the bridge of the running event loop.

        Returns:
            The core bridge of the running loop.
        """
        loop = asyncio.get_running_loop()
        bridge = cls._bridges.get(loop)
        if bridge is None:
            bridge = Bridge(
                cls.MAX_BATCH_SIZE,
                loop.call_soon_threadsafe,
                cls.run_callbacks,
            )
            cls._bridges[loop] = bridge
        return bridge

    @classmethod
    def clear(cls) -> None:
        """Forgets all bridges.

        A bridge thread stops when no subscription and no publisher uses
        its bridge.
        """
        cls._bridges.clear()

    @staticmethod
    def run_callbacks(
        calls: list[tuple[Callable[[list[Any]], None], list[Any]]],
    ) -> None:
        """Runs the callbacks of one batch on the event loop.

        A failed callback does not stop the other callbacks of the batch.

        Args:
            calls: For each callback, the entries to give it.
        """
        for callback, entries in calls:
            try:
                callback(entries)
            except Exception:
                logger.exception("A bridge callback failed")

    @staticmethod
    def resolve_futures(
        outcomes: list[tuple[asyncio.Future[str], bool, Any]],
    ) -> None:
        """Completes a batch of publish futures on the event loop.

        A future that is done already, for example a cancelled one, stays
        as it is.

        Args:
            outcomes: For each publish: the future, True when the publish
                succeeded, and the message ID or the exception.
        """
        for future, is_success, value in outcomes:
            if future.done():
                continue
            if is_success:
                future.set_result(value)
            else:
                future.set_exception(value)
