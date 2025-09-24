import pytest

from fastpubsub.concurrency.ipc import ProcessInfo
from fastpubsub.concurrency.utils import (
    ensure_async_callable_function,
    ensure_async_middleware,
    get_process_info,
)
from fastpubsub.middlewares.base import BaseMiddleware


def test_enforce_asynchronous_function_type():
    async def some_async_function(): ...
    def some_sync_function(): ...

    class MyCallableObject:
        def __call__(self, *args, **kwds):
            pass

        def method(self):
            pass

        async def async_method(self):
            pass

    invalid_datastructures = [
        some_sync_function,
        lambda x: x,
        MyCallableObject,
        MyCallableObject(),
        MyCallableObject().method,
        MyCallableObject().async_method,
    ]

    for invalid_ds in invalid_datastructures:
        with pytest.raises(TypeError):
            ensure_async_callable_function(invalid_ds)

    ensure_async_callable_function(some_async_function)


def test_enforce_async_middleware_function(first_middleware: type[BaseMiddleware]):
    class UnsupportedTypeMiddleware: ...

    class SyncOnMessageMiddleware(BaseMiddleware):
        def on_message(self, *args, **kwargs):
            pass

    class SyncOnPublishMiddleware(BaseMiddleware):
        def on_publish(self, *args, **kwargs):
            pass

    invalid_middlewares = [
        UnsupportedTypeMiddleware,
        SyncOnMessageMiddleware,
        SyncOnPublishMiddleware,
    ]

    for invalid_middleware in invalid_middlewares:
        with pytest.raises(TypeError):
            ensure_async_middleware(invalid_middleware)

    ensure_async_middleware(first_middleware)


def test_get_process_info():
    process_info = get_process_info()
    assert isinstance(process_info, ProcessInfo)
