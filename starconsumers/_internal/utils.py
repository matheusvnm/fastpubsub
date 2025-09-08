import asyncio
import functools
import inspect
import signal
from collections.abc import Awaitable, Callable
from contextlib import suppress
from types import FrameType
from typing import Any, ParamSpec, TypeVar, cast

from starlette.concurrency import run_in_threadpool

from starconsumers._internal.compat import IS_WINDOWS

HANDLED_SIGNALS: tuple[int, ...] = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)

if IS_WINDOWS:  # pragma: py-not-win32
    # Windows signal 21. Sent by Ctrl+Break.
    HANDLED_SIGNALS += (signal.SIGBREAK,)  # type: ignore[attr-defined]


P = ParamSpec("P")
T = TypeVar("T")


class AsyncRunner:
    """
    A pickleable async decorator that converts a synchronous function to an
    asynchronous one by running it in a thread pool.

    If the decorated function is already a coroutine, it is left unchanged.
    """

    def __init__(self, func: Callable[P, T]):
        self.func = func
        self.is_async = inspect.iscoroutinefunction(func)
        functools.update_wrapper(self, func)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        """
        Makes the instance callable and executes the core async logic.
        This method is what runs when you call the decorated function.
        """
        if self.is_async:
            async_func = cast(Callable[P, Awaitable[T]], self.func)
            return await async_func(*args, **kwargs)
        return await run_in_threadpool(self.func, *args, **kwargs)

    def __reduce__(self):
        """
        Provides the "recipe" for pickling.

        It tells pickle: "To rebuild this object, call the AsyncConverter
        class with the original function (self.func) as the argument."
        """
        return (AsyncRunner, (self.func,))


def set_exit(
    func: Callable[[int, FrameType | None], Any],
    *,
    sync: bool = False,
) -> None:
    """Set exit handler for signals.

    Args:
        func: A callable object that takes an integer
              and an optional frame type as arguments and returns any value.
        sync: set sync or async signal callback.
    """
    if not sync:
        with suppress(NotImplementedError):
            loop = asyncio.get_running_loop()

            for sig in HANDLED_SIGNALS:
                loop.add_signal_handler(sig, func, sig, None)

            return

    # Windows or sync mode
    for sig in HANDLED_SIGNALS:
        signal.signal(sig, func)
