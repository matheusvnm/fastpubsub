from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any, Union

from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage


@dataclass(frozen=True)
class TopicMessage:
    id: str
    size: int
    data: bytes
    delivery_attempt: int
    attributes: dict[str, str]


type DecoratedCallable = Callable[[TopicMessage], Any]


@dataclass(frozen=True)
class DeadLetterPolicy:
    topic_name: str
    max_delivery_attempts: int


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
class MessageControlFlowPolicy:
    max_bytes: int
    max_messages: int


@dataclass(frozen=True)
class SubscriptionLifecyclePolicy:
    autocreate: bool
    autoupdate: bool


@dataclass(frozen=True)
class TopicSubscription:
    name: str
    project_id: str
    topic_name: str
    retry_policy: MessageRetryPolicy
    delivery_policy: MessageDeliveryPolicy
    lifecycle_policy: SubscriptionLifecyclePolicy
    control_flow_policy: MessageControlFlowPolicy
    dead_letter_policy: DeadLetterPolicy | None


@dataclass(frozen=True)
class Task:
    handler: DecoratedCallable
    subscription: TopicSubscription


@dataclass(frozen=True)
class TasksRegister:
    tasks: dict[str, Task] = field(default_factory=dict)

    def register(self, name: str, task: Task) -> None:
        if not name or not isinstance(name, str) or len(name.strip()) == 0:
            raise ValueError(f"The task name must be a non empty str type not {name=}")

        if name in self.tasks:
            new_subscription = task.subscription.name
            existing_subscription = self.tasks[name].subscription.name
            raise ValueError(
                f"Duplicated task name {name} for subscriptions"
                f"{new_subscription} and {existing_subscription}"
            )

        func = task.handler
        if func.__code__.co_argcount != 1:
            raise TypeError(f"Task function '{func.__name__}' must accept exactly one argument.")

        arg_name = func.__code__.co_varnames[0]
        arg_type = func.__annotations__.get(arg_name)

        if arg_type is not TopicMessage:
            raise TypeError(
                f"Task function '{func.__name__}' argument must be of type {TopicMessage}, "
                f"but it is {arg_type}."
            )

        self.tasks[name.casefold()] = task

    def get(self) -> dict[str, Task]:
        return self.tasks


@dataclass
class MessageMiddleware:
    next_call: Union["MessageMiddleware", DecoratedCallable]

    def __call__(self, message: TopicMessage | PubSubMessage) -> Any:
        return self.next_call(message)


class MiddlewareContainer:
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
class MiddlewaresRegister:
    middlewares: list[MiddlewareContainer] = field(default_factory=list)

    def register(
        self, middleware: type[MessageMiddleware], *args: list[Any], **kwargs: dict[str, Any]
    ) -> None:
        if not (isinstance(middleware, type) and issubclass(middleware, MessageMiddleware)):
            raise ValueError(f"The middleware must implement {MessageMiddleware.__name__} class")

        self.middlewares.append(MiddlewareContainer(middleware, *args, **kwargs))

    def get(self) -> list[MiddlewareContainer]:
        return self.middlewares


@dataclass(frozen=True)
class WrappedTask:
    handler: MessageMiddleware | DecoratedCallable
    subscription: TopicSubscription
