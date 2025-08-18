
from dataclasses import dataclass
from typing import Union
from starconsumers.types import DecoratedCallable


@dataclass
class MessageMiddleware:
    next_call: Union["MessageMiddleware", DecoratedCallable] = None

    def __call__(self, *args, **kwargs):
        if not self.next_call:
            return 

        return self.next_call(*args, **kwargs)


@dataclass(frozen=True)
class DeadLetterPolicy:
    topic_name: str
    delivery_attempts: int


@dataclass(frozen=True)
class TopicSubscription:
    name: str
    project_id: str
    topic_name: str
    filter_expression: str
    ack_deadline_seconds: int
    enable_message_ordering: bool
    enable_exactly_once_delivery: str
    dead_letter_policy: DeadLetterPolicy


@dataclass(frozen=True)
class Task:
    autocreate: bool
    handler: MessageMiddleware
    subscription: TopicSubscription

