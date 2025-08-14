from typing import Callable

from starconsumers.datastructures import DeadLetterPolicy, MessageHandler, Subscription, Task
from starconsumers.types import DecoratedCallable



class TopicConsumer:
    def __init__(self, project_id: str, topic_name: str, autocreate: bool = True):
        self.project_id = project_id
        self.topic_name = topic_name
        self.autocreate = autocreate

        self.task_map: dict[str, Task] = {}

    def task(
        self,
        name: str,
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
            dead_letter_policy = None
            if dead_letter_topic:
                dead_letter_policy = DeadLetterPolicy(topic_name=dead_letter_topic, 
                                                      max_delivery_attempts=max_delivery_attempts)


            handler = MessageHandler(target=func)
            subscription = Subscription(name=subscription_name, 
                                        project_id=self.project_id,
                                        topic_name=self.topic_name,
                                        filter_expression=filter_expression,
                                        ack_deadline_seconds=ack_deadline_seconds,
                                        enable_message_ordering=enable_message_ordering,
                                        enable_exactly_once_delivery=enable_exactly_once_delivery,
                                        dead_letter_policy=dead_letter_policy)
            task = Task(handler=handler, subscription=subscription, autocreate=self.autocreate)
            self.task_map[name.casefold()] = task
            return func

        return decorator




