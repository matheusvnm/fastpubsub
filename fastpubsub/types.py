from collections.abc import Awaitable, Callable
from typing import Any, Coroutine, ParamSpec, TypeVar

from fastapi import Request, Response

DecoratedCallable = Callable[[Any], Any]
SubscribedCallable = Callable[[DecoratedCallable], DecoratedCallable]


AsyncCallable = Callable[[Any], Awaitable[None]]
NoArgAsyncCallable = Callable[[], Awaitable[None]]
SyncCallable = Callable[[Any], None]

AsyncRequestHandler = Callable[[Request, Any], Coroutine[Any, Any, Response]]
ExceptionMarker = int | type[Exception]


