"""Type definitions for FastPubSub."""

from collections.abc import Awaitable, Callable
from typing import Any

# V2: We wait a return because in further releases we will allow chaining handlers/publishers
AsyncDecoratedCallable = Callable[[Any], Awaitable[Any]]
SubscribedCallable = Callable[[AsyncDecoratedCallable], AsyncDecoratedCallable]

AsyncCallable = Callable[[Any], Awaitable[None]]
NoArgAsyncCallable = Callable[[], Awaitable[None]]
