from dataclasses import dataclass
from typing import Any, AsyncGenerator, Callable, Iterable, TypeVar

from fastapi import FastAPI
from starlette.types import Scope, Receive, Send

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


@dataclass(frozen=True)
class Task:
    handler: MessageHandler
    subscription: Subscription
    autocreate: bool


class ProcessManager:
    pass

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

    def filter(self, tasks_names: set[str]) -> list[Task]:
        tasks = []
        for task_name in tasks_names:
           if task_name in self.task_map:
               tasks.append(tasks)
        return tasks 

class StarConsumers:

    def __init__(self, title: str = "StarConsumers", summary: str = None, description: str = ""):
        self.process_manager = ProcessManager()
        self.active_tasks: list[Task] = []
        self.all_tasks_map: dict[str, Task] = {}

        self.server = FastAPI(title=title, summary=summary, description=description, lifespan=self.start)
        

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self.server(scope, receive, send)

    async def start(self, _: FastAPI) -> AsyncGenerator[Any]:
        print("Do something")
        # TODO: Ajustar process manager para fazer processos em formato spawn
        # TODO: Ativar as tarefas
        # Chamar para cada tarefa:
        # 1. Criar um processo separado.
        # 1.1 No processo, criar o tópico se for autouse
        # 1.2 Criar a inscrição para a tarefa
        # 1.3 Se inscrever no tópico/inscrição passando o message handler.
        # No futuro podemos adicionar middlewares nesse handler.
        # No futuro podemos ter middlewares default no handler.
        # Precisamos considerar que essa função pode ser async ou não.  
        
        yield
        print("Do something after teardown")

    def add_consumer(self, consumer: TopicConsumer):
        if not isinstance(consumer, TopicConsumer):
            raise ValueError(f"The consumer must be {TopicConsumer.__name__} instance")
        
        for task_name, task in consumer.task_map.items():
            if task_name in self.all_tasks_map:
                new_subscription = task.subscription.name
                existing_subscription = self.all_tasks_map[task_name].subscription.name
                raise ValueError(f"Duplicated task name {task_name} for subscriptions {new_subscription} and {existing_subscription}")

            self.all_tasks_map[task_name] = task

    def activate_tasks(self, tasks_names: list[str]) -> None:
        if not isinstance(tasks_names, list):
            raise ValueError("You can only add the name of the tasks as list.")
        
        selected_tasks = set()
        for task_name in tasks_names:
            selected_tasks.add(task_name.casefold())

        print(f"We selected the tasks {selected_tasks}")
        for task_name in selected_tasks:
            if task_name in self.all_tasks_map:
                self.active_tasks.append(self.all_tasks_map[task_name])


app = StarConsumers()
consumer = TopicConsumer(project_id="abc", topic_name="topic")


@consumer.task(name="some_handler", subscription_name="some_subscription")
def some_handler():
    print("ola")


@consumer.task(name="some_other_handler", subscription_name="some_subscription2")
def some_other_handler():
    print("ola")


app.add_consumer(consumer)
app.activate_tasks(["some_other_handler"])


print(app.active_tasks)
print(consumer.task_map)
