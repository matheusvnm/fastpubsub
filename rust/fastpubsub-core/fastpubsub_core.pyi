import asyncio
from collections.abc import Callable
from typing import Any

class AckHandle:
    def ack(self) -> None: ...
    def nack(self) -> None: ...

class Subscription:
    def __init__(
        self,
        subscription_path: str,
        max_messages: int,
        max_batch_size: int,
        call_soon_threadsafe: Callable[..., Any],
        dispatch: Callable[[list[tuple[str, bytes, dict[str, str], int, AckHandle]]], None],
        endpoint: str | None = None,
    ) -> None: ...
    def is_running(self) -> bool: ...
    def stop(self) -> None: ...

class Publisher:
    def __init__(
        self,
        topic_path: str,
        max_batch_size: int,
        call_soon_threadsafe: Callable[..., Any],
        resolve_batch: Callable[
            [list[tuple[asyncio.Future[str], bool, str]]], None
        ],
        endpoint: str | None = None,
    ) -> None: ...
    def publish(
        self,
        data: bytes,
        attributes: dict[str, str],
        future: asyncio.Future[str],
        ordering_key: str | None = None,
    ) -> None: ...
