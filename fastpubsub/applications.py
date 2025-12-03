"""FastPubSub application and lifecycle management."""

from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import ConfigDict, validate_call
from starlette.applications import Starlette
from starlette.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR

from fastpubsub.broker import PubSubBroker
from fastpubsub.concurrency.utils import ensure_async_callable_function
from fastpubsub.types import NoArgAsyncCallable


class Application:
    """Manages the lifecycle of a FastPubSub application."""

    def __init__(
        self,
        broker: PubSubBroker,
        on_startup: Sequence[NoArgAsyncCallable] | None = None,
        on_shutdown: Sequence[NoArgAsyncCallable] | None = None,
        after_startup: Sequence[NoArgAsyncCallable] | None = None,
        after_shutdown: Sequence[NoArgAsyncCallable] | None = None,
    ):
        """Initializes the Application.

        Args:
            broker: The PubSubBroker instance.
            on_startup: A sequence of callables to run on startup.
            on_shutdown: A sequence of callables to run on shutdown.
            after_startup: A sequence of callables to run after startup.
            after_shutdown: A sequence of callables to run after shutdown.
        """
        self.broker = broker

        self._on_startup: list[NoArgAsyncCallable] = []
        if on_startup and isinstance(on_startup, Sequence):
            for func in on_startup:
                self.on_startup(func)

        self._on_shutdown: list[NoArgAsyncCallable] = []
        if on_shutdown and isinstance(on_shutdown, Sequence):
            for func in on_shutdown:
                self.on_shutdown(func)

        self._after_startup: list[NoArgAsyncCallable] = []
        if after_startup and isinstance(after_startup, Sequence):
            for func in after_startup:
                self.after_startup(func)

        self._after_shutdown: list[NoArgAsyncCallable] = []
        if after_shutdown and isinstance(after_shutdown, Sequence):
            for func in after_shutdown:
                self.after_shutdown(func)

    @validate_call(config=ConfigDict(strict=True))
    def on_startup(self, func: NoArgAsyncCallable) -> NoArgAsyncCallable:
        """Decorator to register a function to run on startup.

        Args:
            func: The function to run on startup.

        Returns:
            The decorated function.
        """
        ensure_async_callable_function(func)
        self._on_startup.append(func)
        return func

    @validate_call(config=ConfigDict(strict=True))
    def on_shutdown(self, func: NoArgAsyncCallable) -> NoArgAsyncCallable:
        """Decorator to register a function to run on shutdown.

        Args:
            func: The function to run on shutdown.

        Returns:
            The decorated function.
        """
        ensure_async_callable_function(func)
        self._on_shutdown.append(func)
        return func

    @validate_call(config=ConfigDict(strict=True))
    def after_startup(self, func: NoArgAsyncCallable) -> NoArgAsyncCallable:
        """Decorator to register a function to run after startup.

        Args:
            func: The function to run after startup.

        Returns:
            The decorated function.
        """
        ensure_async_callable_function(func)
        self._after_startup.append(func)
        return func

    @validate_call(config=ConfigDict(strict=True))
    def after_shutdown(self, func: NoArgAsyncCallable) -> NoArgAsyncCallable:
        """Decorator to register a function to run after shutdown.

        Args:
            func: The function to run after shutdown.

        Returns:
            The decorated function.
        """
        ensure_async_callable_function(func)
        self._after_shutdown.append(func)
        return func

    async def _start(self) -> None:
        async with self._start_hooks():
            await self.broker.start()

    @asynccontextmanager
    async def _start_hooks(self) -> AsyncIterator[None]:
        for func in self._on_startup:
            await func()

        yield

        for func in self._after_startup:
            await func()

    async def _shutdown(self) -> None:
        async with self._shutdown_hooks():
            self.broker.shutdown()

    @asynccontextmanager
    async def _shutdown_hooks(self) -> AsyncIterator[None]:
        for func in self._on_shutdown:
            await func()

        yield

        for func in self._after_shutdown:
            await func()


class FastPubSub(FastAPI, Application):
    """A FastAPI integration application for managing Pub/Sub consumers."""

    def __init__(
        self,
        broker: PubSubBroker,
        *,
        on_startup: Sequence[NoArgAsyncCallable] | None = None,
        on_shutdown: Sequence[NoArgAsyncCallable] | None = None,
        after_startup: Sequence[NoArgAsyncCallable] | None = None,
        after_shutdown: Sequence[NoArgAsyncCallable] | None = None,
        liveness_url: str = "/consumers/alive",
        readiness_url: str = "/consumers/ready",
        **extras: Any,
    ):
        """Initializes the FastPubSub application.

        Args:
            broker: The PubSubBroker instance.
            on_startup: A sequence of callables to run on startup.
            on_shutdown: A sequence of callables to run on shutdown.
            after_startup: A sequence of callables to run after startup.
            after_shutdown: A sequence of callables to run after shutdown.
            liveness_url: A url path for the readiness endpoint.
            readiness_url: A url path for the readiness endpoint.
            **extras: Extra arguments to pass to the FastAPI constructor.
        """
        self.lifespan_context = extras.pop("lifespan", None)
        super().__init__(
            lifespan=self._run,
            **extras,
        )

        super(Starlette, self).__init__(
            broker,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            after_startup=after_startup,
            after_shutdown=after_shutdown,
        )

        self.add_api_route(path=liveness_url, endpoint=self._get_liveness, methods=["GET"])
        self.add_api_route(path=readiness_url, endpoint=self._get_readiness, methods=["GET"])

    @asynccontextmanager
    async def _run(self, app: "FastPubSub") -> AsyncGenerator[None]:
        if not self.lifespan_context:
            await self._start()
            yield
            await self._shutdown()
        else:
            async with self.lifespan_context(app):
                await self._start()
                yield
                await self._shutdown()

    async def _get_liveness(self, _: Request) -> JSONResponse:
        alive = self.broker.alive()

        status_code = HTTP_200_OK
        if not alive:
            status_code = HTTP_500_INTERNAL_SERVER_ERROR

        return JSONResponse(content={"alive": alive}, status_code=status_code)

    async def _get_readiness(self, _: Request) -> JSONResponse:
        ready = self.broker.ready()

        status_code = HTTP_200_OK
        if not ready:
            status_code = HTTP_500_INTERNAL_SERVER_ERROR

        return JSONResponse(content={"ready": ready}, status_code=status_code)
