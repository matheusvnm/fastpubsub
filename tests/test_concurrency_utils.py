import pytest

from fastpubsub.concurrency.ipc import ProcessInfo
from fastpubsub.concurrency.utils import (
    ensure_async_callable_function,
    ensure_async_middleware,
    get_process_info,
)
from fastpubsub.middlewares.base import BaseMiddleware


class TestEnsureAsyncCallable:
    @pytest.mark.parametrize(
        "invalid_callable",
        [
            (lambda: "sync lambda"),
            "a string",
            123,
            object(),
        ],
    )
    def test_with_invalid_types_raises_exception(self, invalid_callable):
        with pytest.raises(TypeError):
            ensure_async_callable_function(invalid_callable)

    def test_with_valid_async_function_succeeds(self):
        async def some_async_function():
            pass

        ensure_async_callable_function(some_async_function)


class TestEnsureAsyncMiddleware:
    class UnsupportedTypeMiddleware:
        pass

    class SyncOnMessageMiddleware(BaseMiddleware):
        def on_message(self, *args, **kwargs):
            pass

    class SyncOnPublishMiddleware(BaseMiddleware):
        def on_publish(self, *args, **kwargs):
            pass

    @pytest.mark.parametrize(
        "invalid_middleware",
        [
            UnsupportedTypeMiddleware,
            SyncOnMessageMiddleware,
            SyncOnPublishMiddleware,
        ],
    )
    def test_with_invalid_middleware_raises_exception(self, invalid_middleware):
        with pytest.raises(TypeError):
            ensure_async_middleware(invalid_middleware)

    def test_with_valid_middleware_succeeds(self, first_middleware: type[BaseMiddleware]):
        ensure_async_middleware(first_middleware)


class TestGetProcessInfo:
    def test_returns_process_info_instance(self):
        process_info = get_process_info()
        assert isinstance(process_info, ProcessInfo)
