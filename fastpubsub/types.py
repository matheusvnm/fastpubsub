"""Type definitions for FastPubSub."""

from collections.abc import Awaitable, Callable
from typing import Any

AsyncDecoratedCallable = Callable[[Any], Awaitable[Any]]
SubscribedCallable = Callable[[AsyncDecoratedCallable], AsyncDecoratedCallable]

AsyncCallable = Callable[[Any], Awaitable[None]]
NoArgAsyncCallable = Callable[[], Awaitable[None]]
