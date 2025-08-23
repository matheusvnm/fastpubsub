from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, Union

from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage
from pydantic import BaseModel


@dataclass(frozen=True)
class TopicMessage:
    id: str
    size: int
    data: bytes
    delivery_attempt: int
    attributes: dict[str, str]


type DecoratedCallable = Callable[[TopicMessage], Any]


@dataclass
class MessageMiddleware:
    next_call: Union["MessageMiddleware", DecoratedCallable]

    def __call__(self, message: TopicMessage | PubSubMessage) -> Any:
        return self.next_call(message)


class MiddlewareFactory:
    def __init__(
        self,
        cls: type[MessageMiddleware],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.cls = cls
        self.args = args
        self.kwargs = kwargs

    def __iter__(self) -> Iterator[Any]:
        as_tuple = (self.cls, self.args, self.kwargs)
        return iter(as_tuple)


@dataclass(frozen=True)
class DeadLetterPolicy:
    topic_name: str
    max_delivery_attempts: int


@dataclass(frozen=True)
class TopicSubscription:
    name: str
    project_id: str
    topic_name: str
    filter_expression: str
    ack_deadline_seconds: int
    enable_message_ordering: bool
    enable_exactly_once_delivery: bool
    dead_letter_policy: DeadLetterPolicy | None


@dataclass(frozen=True)
class Task:
    autocreate: bool
    handler: MessageMiddleware | DecoratedCallable
    subscription: TopicSubscription


class ProcessSocketConnectionAddress(BaseModel):
    ip: str
    port: int
    hostname: str


class ProcessSocketConnection(BaseModel):
    status: str
    address: ProcessSocketConnectionAddress | None


class ProcessInfo(BaseModel):
    name: str
    num_threads: int = 0
    running: bool = False
    connections: list[ProcessSocketConnection] = []
