"""StarConsumers application."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import anyio

from starconsumers._internal.types import CallableHook
from starconsumers._internal.utils import AsyncRunner, set_exit
from starconsumers.broker import Broker
from starconsumers.logger import logger

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

        self.running = True

    def on_startup(self, func: CallableHook) -> CallableHook:
        self._on_startup.append(AsyncRunner(func))
        return func

    def on_shutdown(self, func: CallableHook) -> CallableHook:
        self._on_shutdown.append(AsyncRunner(func))
        return func

    def after_startup(self, func: CallableHook) -> CallableHook:
        self._after_startup.append(AsyncRunner(func))
        return func

    def after_shutdown(self, func: CallableHook) -> CallableHook:
        self._after_shutdown.append(AsyncRunner(func))
        return func

    async def run(self):
        """Run StarConsumers Application."""
        set_exit(lambda *_: self.stop(), sync=False)

        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._start)

                while self.running:
                    await anyio.sleep(0.1)

                await self._shutdown()
                tg.cancel_scope.cancel()
        except ExceptionGroup as e:
            for ex in e.exceptions:
                raise ex from None

    async def _start(self) -> None:
        async with self._start_hooks():
            await self.broker.start()

    @asynccontextmanager
    async def _start_hooks(self) -> AsyncIterator[None]:
        logger.info("StarConsumers app starting...")
        for func in self._on_startup:
            await func()

        yield

        for func in self._after_startup:
            await func()

        logger.info("StarConsumers app started successfully! To exit, press CTRL+C")

    def stop(self):
        """Stop application manually."""
        self.running = False

    async def _shutdown(self) -> None:
        async with self._shutdown_hooks():
            await self.broker.shutdown()

    @asynccontextmanager
    async def _shutdown_hooks(self) -> AsyncIterator[None]:
        for func in self._on_shutdown:
            await func()

        yield

        for func in self._after_shutdown:
            await func()
