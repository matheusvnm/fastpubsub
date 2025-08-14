
from dataclasses import dataclass
from starconsumers.types import DecoratedCallable


@dataclass(frozen=True)
class MessageHandler:
    target: DecoratedCallable

    def on_message(self, *args, **kwargs):
        self.target(*args, **kwargs)


@dataclass(frozen=True)
class DeadLetterPolicy:
    topic_name: str
    delivery_attempts: int


@dataclass(frozen=True)
class Subscription:
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
    handler: MessageHandler
    subscription: Subscription

