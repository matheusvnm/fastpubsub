"""Concurrency utilities."""

import inspect
from collections.abc import Callable
from types import FunctionType
from typing import Any

from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.types import AsyncCallable, AsyncDecoratedCallable


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


def ensure_async_middleware(middleware: type[BaseMiddleware]) -> None:
    """Ensures that a middleware is an async middleware.

    Args:
        middleware: The middleware to check.
    """
    if not issubclass(middleware, BaseMiddleware):
        raise TypeError(f"The object {middleware} must be a {BaseMiddleware.__name__}.")

    if not inspect.iscoroutinefunction(middleware.on_message):
        raise TypeError(f"The on_message method must be async on {middleware}.")

    if not inspect.iscoroutinefunction(middleware.on_publish):
        raise TypeError(f"The on_publish method must be async on {middleware}.")
