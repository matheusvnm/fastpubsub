"""StarConsumers application."""

import os
from collections.abc import AsyncIterator, Callable, Coroutine, Sequence
from contextlib import asynccontextmanager
from typing import Any

import anyio
from fastapi import Depends, FastAPI, Request, Response, routing
from fastapi.middleware import Middleware
from fastapi.responses import JSONResponse
from starlette.routing import BaseRoute

from starconsumers._internal.types import CallableHook
from starconsumers.broker import Broker
from starconsumers.concurrency import ensure_async_callable, set_exit
from starconsumers.logger import logger


class StarConsumers:
    def __init__(
        self,
        broker: Broker,
        on_startup: list[CallableHook] = None,
        on_shutdown: list[CallableHook] = None,
        after_startup: list[CallableHook] = None,
        after_shutdown: list[CallableHook] = None,
    ):
        self.broker = broker

        self._on_startup = []
        if on_startup and isinstance(on_startup, list):
            for func in on_startup:
                self.on_startup(func)

        self._on_shutdown = []
        if on_shutdown and isinstance(on_shutdown, list):
            for func in on_shutdown:
                self.on_shutdown(func)

        self._after_startup = []
        if after_startup and isinstance(after_startup, list):
            for func in after_startup:
                self.after_startup(func)

        self._after_shutdown = []
        if after_shutdown and isinstance(after_shutdown, list):
            for func in after_shutdown:
                self.after_shutdown(func)

        self.should_exit = False

    def on_startup(self, func: CallableHook) -> CallableHook:
        ensure_async_callable(func)
        self._on_startup.append(func)
        return func

    def on_shutdown(self, func: CallableHook) -> CallableHook:
        ensure_async_callable(func)
        self._on_shutdown.append(func)
        return func

    def after_startup(self, func: CallableHook) -> CallableHook:
        ensure_async_callable(func)
        self._after_startup.append(func)
        return func

    def after_shutdown(self, func: CallableHook) -> CallableHook:
        ensure_async_callable(func)
        self._after_shutdown.append(func)
        return func

    async def run(self, selected_subscribers: set[str] = None):
        """Run StarConsumers Application."""
        set_exit(lambda *_: self.stop(), sync=False)

        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._start, selected_subscribers)

                while not self.should_exit:
                    # TODO: Checar se todos os processos estão vivos
                    # TODO: Se algum não estiver, devemos reiniciar
                    # TODO: Usar o process manager para isso.
                    # TODO: Em caso de duvida ver o keep_subprocess_alive do uvicorn.
                    await anyio.sleep(0.5)

                await self._shutdown()
                tg.cancel_scope.cancel()
        except ExceptionGroup as e:
            for ex in e.exceptions:
                raise ex from None

    async def _start(self, selected_subscribers: set[str] = None) -> None:
        async with self._start_hooks():
            await self.broker.start(selected_subscribers=selected_subscribers)

    @asynccontextmanager
    async def _start_hooks(self) -> AsyncIterator[None]:
        logger.info(f"Starting a StarConsumers child processes")
        for func in self._on_startup:
            await func()

        yield

        for func in self._after_startup:
            await func()

        logger.info(f"The StarConsumers child processes started")

    def stop(self):
        """Stop application manually."""
        self.should_exit = True

    async def _shutdown(self) -> None:
        async with self._shutdown_hooks():
            await self.broker.shutdown()

    @asynccontextmanager
    async def _shutdown_hooks(self) -> AsyncIterator[None]:
        logger.info(f"Terminating the StarConsumers child process")
        for func in self._on_shutdown:
            await func()

        yield

        for func in self._after_shutdown:
            await func()


class FastConsumers(FastAPI, StarConsumers):
    # TODO: Adicionar na adição de subscribers (handlers) uma validação para impedir adição de Depends() do fastapi e outros tipos do mesmo

    def __init__(
        self,
        broker: Broker,
        *,
        on_startup: list[CallableHook] = None,
        on_shutdown: list[CallableHook] = None,
        after_startup: list[CallableHook] = None,
        after_shutdown: list[CallableHook] = None,
        debug: bool = False,
        title: str = "StarConsumers",
        summary: str | None = None,
        description: str = "",
        version: str = "0.1.0",
        openapi_url: str = "/openapi.json",
        openapi_tags: str = None,
        servers: list[dict[str, str | Any]] = None,
        dependencies: Sequence[Depends] = None,  #
        default_response_class: type[Response] = JSONResponse,
        redirect_slashes: bool = True,
        docs_url: str = "/docs",
        redoc_url: str = "/redoc",
        swagger_ui_oauth2_redirect_url: str | None = None,
        middleware: Sequence[Middleware] | None = None,
        exception_handlers: dict[
            int | type[Exception] | Callable[[Request, Any], Coroutine[Any, Any, Response]]
        ]
        | None = None,
        lifespan: Callable | None = None,
        terms_of_service: str | None = None,
        contact: dict[str, str | Any] | None = None,
        license_info: dict[str, str | Any] | None = None,
        openapi_prefix: str = "",
        root_path: str = "",
        root_path_in_servers: bool = True,
        responses: dict[int | str, dict[str, Any]] | None = None,
        callbacks: list[BaseRoute] | None = None,
        webhooks: routing.APIRouter | None = None,
        include_in_schema: bool = True,
        swagger_ui_parameters: dict[str, Any] | None = None,
        separate_input_output_schemas: bool = True,
        **extra: Any,
    ):
        from starlette.applications import Starlette
        super().__init__(
            debug=debug,
            title=title,
            summary=summary,
            description=description,
            version=version,
            openapi_url=openapi_url,
            openapi_tags=openapi_tags,
            servers=servers,
            dependencies=dependencies,
            default_response_class=default_response_class,
            redirect_slashes=redirect_slashes,
            docs_url=docs_url,
            redoc_url=redoc_url,
            swagger_ui_oauth2_redirect_url=swagger_ui_oauth2_redirect_url,
            middleware=middleware,
            exception_handlers=exception_handlers,
            lifespan=self.run,
            terms_of_service=terms_of_service,
            contact=contact,
            license_info=license_info,
            openapi_prefix=openapi_prefix,
            root_path=root_path,
            root_path_in_servers=root_path_in_servers,
            responses=responses,
            callbacks=callbacks,
            webhooks=webhooks,
            include_in_schema=include_in_schema,
            swagger_ui_parameters=swagger_ui_parameters,
            separate_input_output_schemas=separate_input_output_schemas,
            **extra,
        )

        super(Starlette, self).__init__(
            broker,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            after_startup=after_startup,
            after_shutdown=after_shutdown,
        )

        self.lifespan_context = lifespan
        # TODO: Create setup method for that
        self.add_api_route(path="/starconsumers/health", endpoint=self._health, methods=["GET"])

    @asynccontextmanager
    async def run(self, app: FastAPI):
        # TODO: Precisamos spawnar um processo para gerenciar os workers
        # TODO: O processo de start precisa ser feito dentro desse process

        # TODO: Checar se todos os processos estão vivos
        # TODO: Se algum não estiver, devemos reiniciar
        # TODO: Usar o process manager para isso.
        # TODO: Em caso de duvida ver o keep_subprocess_alive do uvicorn.
        subscribers = str(os.getenv("STARCONSUMERS_SUBSCRIBERS", "")).split(",")
        if self.lifespan_context:
            async with self.lifespan_context(app):
                await self._start(subscribers)
                yield
                await self._shutdown()
        else:
            await self._start(subscribers)
            yield
            await self._shutdown()

    async def _health(self, request: Any):
        return {"health": "ok"}
