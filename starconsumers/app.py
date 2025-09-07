"""StarConsumers application."""
import asyncio
from typing import Any, Awaitable, Callable, List, Optional, Union

from starconsumers.broker import Broker

HookCallable = Callable[[], Union[None, Awaitable[None]]]


class StarConsumers:
    def __init__(
        self,
        broker: Broker,
        on_startup: Optional[List[HookCallable]] = None,
        after_startup: Optional[List[HookCallable]] = None,
        on_shutdown: Optional[List[HookCallable]] = None,
        after_shutdown: Optional[List[HookCallable]] = None,
    ):
        self.broker = broker
        self._on_startup = on_startup or []
        self._after_startup = after_startup or []
        self._on_shutdown = on_shutdown or []
        self._after_shutdown = after_shutdown or []

    def on_startup(self, func: HookCallable) -> None:
        self._on_startup.append(func)

    def after_startup(self, func: HookCallable) -> None:
        self._after_startup.append(func)

    def on_shutdown(self, func: HookCallable) -> None:
        self._on_shutdown.append(func)

    def after_shutdown(self, func: HookCallable) -> None:
        self._after_shutdown.append(func)


    

    async def start(self) -> None:
        """Start the application."""
        for func in self._on_startup:
            if asyncio.iscoroutinefunction(func):
                await func()
            else:
                func()

        await self.broker.start()

        for func in self._after_startup:
            if asyncio.iscoroutinefunction(func):
                await func()
            else:
                func()

    async def shutdown(self) -> None:
        """Shutdown the application."""
        for func in self._on_shutdown:
            if asyncio.iscoroutinefunction(func):
                await func()
            else:
                func()

        await self.broker.shutdown()

        for func in self._after_shutdown:
            if asyncio.iscoroutinefunction(func):
                await func()
            else:
                func()
