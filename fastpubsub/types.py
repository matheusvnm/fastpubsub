"""Type definitions for FastPubSub."""

from collections.abc import Awaitable, Callable
from typing import Any

AsyncDecoratedCallable = Callable[[Any], Awaitable[Any]]
MessageHandler = Callable[[Any], Any]
SubscribedCallable = Callable[[MessageHandler], MessageHandler]

AsyncCallable = Callable[[Any], Awaitable[None]]
NoArgAsyncCallable = Callable[[], Awaitable[None]]
