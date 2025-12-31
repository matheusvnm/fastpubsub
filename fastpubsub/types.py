"""Type definitions for FastPubSub."""

from collections.abc import Awaitable, Callable
from typing import Any

# V2: We wait a return because in further releases we will allow chaining handlers/publishers
# Using Callable[..., Awaitable[Any]] to support DI with varying handler signatures
AsyncDecoratedCallable = Callable[..., Awaitable[Any]]
SubscribedCallable = Callable[[AsyncDecoratedCallable], AsyncDecoratedCallable]

AsyncCallable = Callable[..., Awaitable[Any]]
NoArgAsyncCallable = Callable[[], Awaitable[None]]
