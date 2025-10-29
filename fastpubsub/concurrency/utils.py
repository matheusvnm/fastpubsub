"""Concurrency utilities."""

import functools
import inspect
from collections.abc import Callable
from types import FunctionType
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

import anyio
import anyio.to_thread

from fastpubsub.types import AsyncCallable, AsyncDecoratedCallable

if TYPE_CHECKING:
    from fastpubsub.middlewares.base import BaseMiddleware


P = ParamSpec("P")
T = TypeVar("T")


def ensure_async_callable_function(
    callable_object: Callable[[], Any] | AsyncCallable | AsyncDecoratedCallable,
) -> None:
    """Ensures that a callable is an async function.

    Args:
        callable_object: The callable to check.
    """
    if not isinstance(callable_object, FunctionType):
        raise TypeError(f"The object must be a function type but it is {callable_object}.")

    if not inspect.iscoroutinefunction(callable_object):
        raise TypeError(f"The function {callable_object} must be async.")


def ensure_async_middleware(middleware: type["BaseMiddleware"]) -> None:
    """Ensures that a middleware is an async middleware.

    Args:
        middleware: The middleware to check.
    """
    from fastpubsub.middlewares.base import BaseMiddleware

    if not issubclass(middleware, BaseMiddleware):
        raise TypeError(f"The object {middleware} must be a {BaseMiddleware.__name__}.")

    if not inspect.iscoroutinefunction(middleware.on_message):
        raise TypeError(f"The on_message method must be async on {middleware}.")

    if not inspect.iscoroutinefunction(middleware.on_publish):
        raise TypeError(f"The on_publish method must be async on {middleware}.")


async def apply_async(func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """Transforms a blocking sync callable into a async callable.

    Args:
        func: The sync callable to be transformed.
        *args: The positional arguments used on the callable.
        **kwargs: The keyword arguments used on the callable.

    Returns:
        The same return of the callable but after awaiting for
        its computation.
    """
    func = functools.partial(func, *args, **kwargs)
    return await anyio.to_thread.run_sync(func, abandon_on_cancel=False)


async def apply_async_cancellable(func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
    """Transforms a blocking sync callable into a async callable that can be cancelled.

    Args:
        func: The sync callable to be transformed.
        *args: The positional arguments used on the callable.
        **kwargs: The keyword arguments used on the callable.

    Returns:
        The same return of the callable but after awaiting for
        its computation.
    """
    func = functools.partial(func, *args, **kwargs)
    return await anyio.to_thread.run_sync(func, abandon_on_cancel=True)
