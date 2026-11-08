import threading
from concurrent.futures import Future

import pytest

from fastpubsub.concurrency.utils import (
    ensure_async_callable_function,
    ensure_async_middleware,
    ensure_function,
    wait_future,
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
    def test_with_invalid_middleware_raises_exception(
        self, invalid_middleware
    ):
        with pytest.raises(TypeError):
            ensure_async_middleware(invalid_middleware)

    def test_with_valid_middleware_succeeds(
        self, first_middleware: type[BaseMiddleware]
    ):
        ensure_async_middleware(first_middleware)


class TestEnsureFunction:
    @pytest.mark.parametrize("invalid_object", ["a string", 123, object()])
    def test_with_invalid_types_raises_exception(self, invalid_object):
        with pytest.raises(TypeError):
            ensure_function(invalid_object)


class TestWaitFuture:
    @pytest.mark.asyncio
    async def test_returns_result_of_done_future(self):
        future: Future[str] = Future()
        future.set_result("message-id")

        assert await wait_future(future) == "message-id"

    @pytest.mark.asyncio
    async def test_returns_result_set_by_other_thread(self):
        future: Future[str] = Future()
        timer = threading.Timer(0.01, future.set_result, args=["message-id"])
        timer.start()

        assert await wait_future(future) == "message-id"

    @pytest.mark.asyncio
    async def test_raises_exception_of_future(self):
        future: Future[str] = Future()
        threading.Timer(
            0.01, future.set_exception, args=[ValueError("failed")]
        ).start()

        with pytest.raises(ValueError, match="failed"):
            await wait_future(future)
