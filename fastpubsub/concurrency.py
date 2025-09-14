import inspect
from collections.abc import Callable
from types import FunctionType
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


def ensure_async_callable(obj: Callable[P, T]):
    if isinstance(obj, FunctionType):
        if not inspect.iscoroutinefunction(obj):
            raise TypeError(f"The function {obj} must be async.")
        return

    if inspect.isclass(obj):
        if not callable(obj):
            raise TypeError(f"The class {obj} must implement a async def __call__.")

    if not inspect.iscoroutinefunction(obj.__call__):
        raise TypeError(f"The class {obj} __call__ function must be async.")
