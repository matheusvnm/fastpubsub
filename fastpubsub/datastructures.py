"""Data structures for FastPubSub."""

from dataclasses import dataclass

from pydantic import BaseModel, ConfigDict, Field


@dataclass(frozen=True)
class Message:
    """A class to represent a Pub/Sub message sent via Pull."""

    id: str
    size: int
    data: bytes
    attributes: dict[str, str]
    delivery_attempt: int
    project_id: str
    topic_name: str
    subscriber_name: str


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


# TODO: Create an example for that
class PushMessageContent(BaseModel):
    """A class to represent a Pub/Sub message data sent via Push."""

    model_config = ConfigDict(validate_by_alias=True)

    id: str = Field(alias="messageId", title="The message id")
    data: str = Field(title="The message content base64-encoded")
    publish_time: str = Field(alias="publishTime", title="The publish datetime of the message")
    attributes: dict[str, str] = Field({}, title="The attributes of the message")


class PushMessage(BaseModel):
    """A class to represent a Pub/Sub message sent via Push."""

    subscription: str
    message: PushMessageContent
