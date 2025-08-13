from dataclasses import dataclass
from typing import Any, Callable, TypeVar

DecoratedCallable = TypeVar("DecoratedCallable", bound=Callable[..., Any])


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


class Consumer:
    def __init__(self, project_id: str, topic_name: str, autocreate: bool = True):
        self.project_id = project_id
        self.topic_name = topic_name
        self.autocreate = autocreate

        self.handlers: dict[str, MessageHandler] = {}
        self.subscriptions: dict[str, Subscription] = {}

    def task(
        self,
        *,
        subscription_name: str,
        filter_expression: str = None,
        dead_letter_topic: str = None,
        max_delivery_attempts: int = 5,
        ack_deadline_seconds: int = 60,
        enable_message_ordering: bool = False,
        enable_exactly_once_delivery: bool = False,
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        def decorator(func: DecoratedCallable) -> DecoratedCallable:
            handler = MessageHandler(target=func)
            self.add_handler(subscription_name, handler)

            dead_letter_policy = None
            if dead_letter_topic:
                dead_letter_policy = DeadLetterPolicy(topic_name=dead_letter_topic, max_delivery_attempts=max_delivery_attempts)

            subscription = Subscription(name=subscription_name, 
                                        project_id=self.project_id,
                                        topic_name=self.topic_name,
                                        filter_expression=filter_expression,
                                        ack_deadline_seconds=ack_deadline_seconds,
                                        enable_message_ordering=enable_message_ordering,
                                        enable_exactly_once_delivery=enable_exactly_once_delivery,
                                        dead_letter_policy=dead_letter_policy)
            self.add_subscription(subscription_name, subscription)
            return func

        return decorator

    def add_handler(self, name: str, handler: MessageHandler):
        self.handlers[name] = handler

    def add_subscription(self, name: str, subscription: Subscription):
        self.subscriptions[name] = subscription


consumer = Consumer(project_id="abc", topic_name="topic")


@consumer.task(subscription_name="some_subscription")
def some_handler():
    print("ola")


@consumer.task(subscription_name="some_other_sub")
def some_handler():
    print("ola")

print(consumer.handlers)
print(consumer.subscriptions)
