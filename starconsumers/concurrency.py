import asyncio
import inspect
import signal
import sys
from collections.abc import Callable
from contextlib import suppress
from types import FrameType, FunctionType
from typing import Any, ParamSpec, TypeVar

IS_WINDOWS = sys.platform in {"win32", "cygwin", "msys"}

HANDLED_SIGNALS: tuple[int, ...] = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)

if IS_WINDOWS:  # pragma: py-not-win32
    # Windows signal 21. Sent by Ctrl+Break.
    HANDLED_SIGNALS += (signal.SIGBREAK,)  # type: ignore[attr-defined]


P = ParamSpec("P")
T = TypeVar("T")


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


def ensure_async_callable(obj: Callable[P, T]):
    if isinstance(obj, FunctionType):
        if not inspect.iscoroutinefunction(obj):
            raise TypeError(f"The function {obj} must be async.")
        return

    if inspect.isclass(obj):
        if not callable(obj):
            raise TypeError(f"The class {obj} must implement a async def __call__.")

    if not inspect.iscoroutinefunction(obj.__call__):
        raise TypeError(f"The class {obj} __call__ function must be async.")
