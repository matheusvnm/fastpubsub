from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, TypeVar

DecoratedCallable = Callable[[Any], Any]
SubscribedCallable = Callable[[DecoratedCallable], DecoratedCallable]


AsyncCallable = Callable[[Any], Awaitable[None]]
SyncCallable = Callable[[Any], None]


P_HookParams = ParamSpec("P_HookParams")
T_HookReturn = TypeVar("T_HookReturn")

CallableHook = Callable[P_HookParams, T_HookReturn]
