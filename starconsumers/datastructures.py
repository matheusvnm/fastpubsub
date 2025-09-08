from dataclasses import dataclass


@dataclass(frozen=True)
class Message:
    id: str
    size: int
    data: bytes
    attributes: dict[str, str]
    delivery_attempt: int | None = 0


@dataclass(frozen=True)
class MessageControlFlowPolicy:
    max_messages: int = 10
    max_bytes: int = 1024 * 1024 * 10  # 10MB


@dataclass(frozen=True)
class MessageDeliveryPolicy:
    filter_expression: str
    ack_deadline_seconds: int
    enable_message_ordering: bool
    enable_exactly_once_delivery: bool


@dataclass(frozen=True)
class MessageRetryPolicy:
    min_backoff_delay_secs: int
    max_backoff_delay_secs: int


@dataclass(frozen=True)
class DeadLetterPolicy:
    topic_name: str
    max_delivery_attempts: int


@dataclass(frozen=True)
class LifecyclePolicy:
    autocreate: bool
    autoupdate: bool
