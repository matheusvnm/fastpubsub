from collections.abc import AsyncGenerator, AsyncIterator, Callable, Coroutine, Sequence
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, Request, Response, routing
from fastapi.middleware import Middleware
from fastapi.responses import JSONResponse
from starlette.applications import Starlette
from starlette.routing import BaseRoute
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from fastpubsub.broker import PubSubBroker
from fastpubsub.concurrency.utils import ensure_async_callable
from fastpubsub.logger import logger
from fastpubsub.observability import get_apm_provider
from fastpubsub.types import CallableHook


class Application:
    def __init__(
        self,
        broker: PubSubBroker,
        on_startup: tuple[CallableHook] = None,
        on_shutdown: tuple[CallableHook] = None,
        after_startup: tuple[CallableHook] = None,
        after_shutdown: tuple[CallableHook] = None,
    ):
        self.broker = broker

        self._on_startup = []
        if on_startup and isinstance(on_startup, tuple):
            for func in on_startup:
                self.on_startup(func)

        self._on_shutdown = []
        if on_shutdown and isinstance(on_shutdown, tuple):
            for func in on_shutdown:
                self.on_shutdown(func)

        self._after_startup = []
        if after_startup and isinstance(after_startup, tuple):
            for func in after_startup:
                self.after_startup(func)

        self._after_shutdown = []
        if after_shutdown and isinstance(after_shutdown, tuple):
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

    async def _start(self) -> None:
        apm = get_apm_provider()
        apm.initialize()
        with apm.background_transaction(name="start"):
            context = {
                "span_id": apm.get_span_id(),
                "trace_id": apm.get_trace_id(),
            }

            with logger.contextualize(**context):
                async with self.start_hooks():
                    await self.broker.start()

    @asynccontextmanager
    async def start_hooks(self) -> AsyncIterator[None]:
        logger.info("Starting FastPubSub processes")
        for func in self._on_startup:
            await func()

        yield

        for func in self._after_startup:
            await func()

        logger.info("The FastPubSub processes started")

    async def _shutdown(self) -> None:
        apm = get_apm_provider()
        with apm.background_transaction(name="shutdown"):
            context = {
                "span_id": apm.get_span_id(),
                "trace_id": apm.get_trace_id(),
            }
            with logger.contextualize(**context):
                async with self.shutdown_hooks():
                    await self.broker.shutdown()

    @asynccontextmanager
    async def shutdown_hooks(self) -> AsyncIterator[None]:
        logger.info("Terminating FastPubSub processes")
        for func in self._on_shutdown:
            await func()

        yield

        for func in self._after_shutdown:
            await func()

        logger.info("The FastPubSub processes terminated")


class FastPubSub(FastAPI, Application):
    # : Adicionar na adição de subscribers (handlers) uma validação para
    #  impedir adição de Depends() do fastapi e outros tipos do mesmo

    def __init__(
        self,
        broker: PubSubBroker,
        *,
        on_startup: list[CallableHook] = None,
        on_shutdown: list[CallableHook] = None,
        after_startup: list[CallableHook] = None,
        after_shutdown: list[CallableHook] = None,
        debug: bool = False,
        title: str = "FastPubSub",
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
        info_url: str = "/consumers/info",
        liveness_url: str = "/consumers/alive",
        readiness_url: str = "/consumers/ready",
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
        self.add_api_route(path=info_url, endpoint=self._get_info, methods=["GET"])
        self.add_api_route(path=liveness_url, endpoint=self._get_liveness, methods=["GET"])
        self.add_api_route(path=readiness_url, endpoint=self._get_readiness, methods=["GET"])


    @asynccontextmanager
    async def run(self, app: "FastPubSub") -> AsyncGenerator[None]:
        if not self.lifespan_context:
            await self._start()
            yield
            await self._shutdown()
        else:
            async with self.lifespan_context(app):
                await self._start()
                yield
                await self._shutdown()

    async def _get_info(self, request: Request) -> JSONResponse:
        return self.broker.info()

    async def _get_liveness(self, request: Request, response: Response) -> JSONResponse:
        alive = self.broker.alive()
        if not alive:
            response.status_code = HTTP_500_INTERNAL_SERVER_ERROR
            return {"alive": alive}

        return {"alive": alive}

    async def _get_readiness(self, request: Request, response: Response) -> JSONResponse:
        ready = self.broker.ready()
        if not ready:
            response.status_code = HTTP_500_INTERNAL_SERVER_ERROR
            return {"ready": ready}

        return {"ready": ready}
