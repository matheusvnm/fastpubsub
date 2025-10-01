from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

from fastapi import Request, Response

# V2: We wait a return because in further releases we will allow chaining handlers/publishers
AsyncDecoratedCallable = Callable[[Any], Awaitable[Any]]
SubscribedCallable = Callable[[AsyncDecoratedCallable], AsyncDecoratedCallable]

AsyncCallable = Callable[[Any], Awaitable[None]]
NoArgAsyncCallable = Callable[[], Awaitable[None]]

AsyncRequestHandler = Callable[[Request, Any], Coroutine[Any, Any, Response]]
ExceptionMarker = int | type[Exception]
