"""StarConsumers application."""
import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable, List, Optional, ParamSpec, TypeVar, Union

from starconsumers._internal.utils import to_async
from starconsumers.broker import Broker
from starconsumers._internal.types import AsyncCallable, CallableHook, SyncCallable



# TODO: Add context manager to start/end the application. aenter aexit.


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

    def on_startup(self, func: CallableHook) -> CallableHook:
        self._on_startup.append(to_async(func))
        return func

    def on_shutdown(self, func: CallableHook) -> CallableHook:
        self._on_shutdown.append(to_async(func))
        return func

    def after_startup(self, func: CallableHook) -> CallableHook:
        self._after_startup.append(to_async(func))
        return func
    
    def after_shutdown(self, func: CallableHook) -> CallableHook:
        self._after_shutdown.append(to_async(func))
        return func

    async def start(self) -> None:
        with self._start_hooks():
            await self.broker.start()

    @asynccontextmanager
    async def _start_hooks(self) -> AsyncIterator[None]:
        for func in self._on_startup:
            await func()

        yield 

        for func in self._after_startup:
            await func()

    async def shutdown(self) -> None:
        with self._shutdown_hooks():
            await self.broker.shutdown()

    @asynccontextmanager
    async def _shutdown_hooks(self) -> AsyncIterator[None]:
        for func in self._on_shutdown:
            await func()

        yield 

        for func in self._after_shutdown:
            await func()

