




from asyncio import iscoroutine
from functools import wraps
import functools
import inspect
from typing import Awaitable, Callable, ParamSpec, TypeVar, Union, cast
from starconsumers._internal.types import AsyncCallable, SyncCallable
from starlette.concurrency import run_in_threadpool
import anyio



P = ParamSpec("P")
T = TypeVar("T")

"""
def to_async(
    func: Callable[F_Spec, F_Return] | Callable[F_Spec, Awaitable[F_Return]],
) -> Callable[F_Spec, Awaitable[F_Return]]:
    "Converts a synchronous function to an asynchronous function.
    if is_coroutine_callable(func):
        return cast("Callable[F_Spec, Awaitable[F_Return]]", func)

    func = cast("Callable[F_Spec, F_Return]", func)

    @wraps(func)
    async def to_async_wrapper(*args: F_Spec.args, **kwargs: F_Spec.kwargs) -> F_Return:
        """Wraps a function to make it asynchronous."""
        return await run_in_threadpool(func, *args, **kwargs)

    return to_async_wrapper


"""

def to_async(
    func: Callable[P, T],
) -> AsyncCallable:
    """Converts a synchronous function to an asynchronous function."""
    if inspect.iscoroutinefunction(func):
        return cast("Callable[P, Awaitable[T]]", func)

    func = cast("Callable[P, T]", func)

    @wraps(func)
    async def to_async_wrapper(*args: P, **kwargs: T) -> T:
        """Wraps a function to make it asynchronous."""
        func = functools.partial(func, *args, **kwargs)
        return await anyio.to_thread.run_sync(func)

    return to_async_wrapper

