from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request, Response, routing
from fastapi.middleware import Middleware
from fastapi.params import Depends
from fastapi.responses import JSONResponse
from pydantic import validate_call
from starlette.applications import Starlette
from starlette.routing import BaseRoute
from starlette.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR
from starlette.types import Lifespan

from fastpubsub.broker import PubSubBroker
from fastpubsub.concurrency.utils import ensure_async_callable_function
from fastpubsub.logger import logger
from fastpubsub.observability import get_apm_provider
from fastpubsub.types import AsyncRequestHandler, ExceptionMarker, NoArgAsyncCallable


class Application:
    def __init__(
        self,
        broker: PubSubBroker,
        on_startup: Sequence[NoArgAsyncCallable] | None = None,
        on_shutdown: Sequence[NoArgAsyncCallable] | None = None,
        after_startup: Sequence[NoArgAsyncCallable] | None = None,
        after_shutdown: Sequence[NoArgAsyncCallable] | None = None,
    ):
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

    @validate_call
    def on_startup(self, func: NoArgAsyncCallable) -> NoArgAsyncCallable:
        ensure_async_callable_function(func)
        self._on_startup.append(func)
        return func

    @validate_call
    def on_shutdown(self, func: NoArgAsyncCallable) -> NoArgAsyncCallable:
        ensure_async_callable_function(func)
        self._on_shutdown.append(func)
        return func

    @validate_call
    def after_startup(self, func: NoArgAsyncCallable) -> NoArgAsyncCallable:
        ensure_async_callable_function(func)
        self._after_startup.append(func)
        return func

    @validate_call
    def after_shutdown(self, func: NoArgAsyncCallable) -> NoArgAsyncCallable:
        ensure_async_callable_function(func)
        self._after_shutdown.append(func)
        return func

    # V1: Create a contextualizer
    async def _start(self) -> None:
        # TODO: Create a module for context and a extensible
        # class for context add (LoggerContextualizer.contextualize(name=name, message=None))
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
    # V2: Add message serialization via pydantic

    def __init__(
        self,
        broker: PubSubBroker,
        *,
        on_startup: Sequence[NoArgAsyncCallable] | None = None,
        on_shutdown: Sequence[NoArgAsyncCallable] | None = None,
        after_startup: Sequence[NoArgAsyncCallable] | None = None,
        after_shutdown: Sequence[NoArgAsyncCallable] | None = None,
        debug: bool = False,
        title: str = "FastPubSub",
        summary: str | None = None,
        description: str = "",
        version: str = "0.1.0",
        openapi_url: str = "/openapi.json",
        openapi_tags: list[dict[str, Any]] | None = None,
        servers: list[dict[str, str | Any]] | None = None,
        dependencies: Sequence[Depends] | None = None,  #
        default_response_class: type[Response] = JSONResponse,
        redirect_slashes: bool = True,
        docs_url: str = "/docs",
        redoc_url: str = "/redoc",
        info_url: str = "/consumers/info",
        liveness_url: str = "/consumers/alive",
        readiness_url: str = "/consumers/ready",
        swagger_ui_oauth2_redirect_url: str | None = None,
        middleware: Sequence[Middleware] | None = None,
        exception_handlers: dict[ExceptionMarker, AsyncRequestHandler] | None = None,
        lifespan: Lifespan["FastPubSub"] | None = None,
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

    async def _get_info(self, _: Request) -> JSONResponse:
        content = self.broker.info()
        return JSONResponse(content=content)

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
