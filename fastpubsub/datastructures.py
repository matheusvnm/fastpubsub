"""Data structures for FastPubSub."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Message:
    """A class to represent a Pub/Sub message."""

    id: str
    size: int
    data: bytes
    ack_id: str
    attributes: dict[str, str]
    delivery_attempt: int


@dataclass(frozen=True)
class MessageControlFlowPolicy:
    """A class to represent a message control flow policy."""

    max_messages: int


@dataclass(frozen=True)
class MessageDeliveryPolicy:
    """A class to represent a message delivery policy."""

    filter_expression: str
    ack_deadline_seconds: int
    enable_message_ordering: bool
    enable_exactly_once_delivery: bool


@dataclass(frozen=True)
class MessageRetryPolicy:
    """A class to represent a message retry policy."""

    min_backoff_delay_secs: int
    max_backoff_delay_secs: int


@dataclass(frozen=True)
class DeadLetterPolicy:
    """A class to represent a dead-letter policy."""

    topic_name: str
    max_delivery_attempts: int


@dataclass(frozen=True)
class LifecyclePolicy:
    """A class to represent a lifecycle policy."""

    autocreate: bool
    autoupdate: bool
