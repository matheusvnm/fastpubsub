import asyncio
from collections.abc import Callable
from typing import Any, TypedDict

class CoreError(RuntimeError): ...
class NotFoundError(CoreError): ...
class AlreadyExistsError(CoreError): ...

class SubscriptionSettings(TypedDict, total=False):
    filter: str
    ack_deadline_seconds: int
    enable_message_ordering: bool
    enable_exactly_once_delivery: bool
    min_backoff_seconds: int
    max_backoff_seconds: int
    dead_letter_topic: str
    max_delivery_attempts: int

class AckHandle:
    def ack(self) -> None: ...
    def nack(self) -> None: ...

class Bridge:
    def __init__(
        self,
        max_batch_size: int,
        call_soon_threadsafe: Callable[..., Any],
        run_callbacks: Callable[
            [list[tuple[Callable[[list[Any]], None], list[Any]]]], None
        ],
    ) -> None: ...

class Subscription:
    def __init__(
        self,
        subscription_path: str,
        max_outstanding_messages: int,
        bridge: Bridge,
        dispatch: Callable[
            [list[tuple[str, bytes, dict[str, str], int, AckHandle]]], None
        ],
        endpoint: str | None = None,
    ) -> None: ...
    def is_running(self) -> bool: ...
    def shutdown(self, timeout_seconds: float) -> bool: ...

class Publisher:
    def __init__(
        self,
        topic_path: str,
        bridge: Bridge,
        resolve_batch: Callable[
            [list[tuple[asyncio.Future[str], bool, Any]]], None
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
    def flush(self) -> None: ...

class AdminClient:
    def __init__(self, endpoint: str | None = None) -> None: ...
    def create_topic(self, topic_path: str) -> None: ...
    def delete_topic(self, topic_path: str) -> None: ...
    def create_subscription(
        self,
        subscription_path: str,
        topic_path: str,
        settings: SubscriptionSettings,
    ) -> None: ...
    def update_subscription(
        self,
        subscription_path: str,
        topic_path: str,
        settings: SubscriptionSettings,
        update_fields: list[str],
    ) -> None: ...
    def delete_subscription(self, subscription_path: str) -> None: ...
