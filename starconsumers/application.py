from typing import Any, AsyncGenerator
from weakref import ProxyType
from fastapi import FastAPI
from starconsumers.consumer import TopicConsumer
from starconsumers.datastructures import Task
from starconsumers.process import ProcessManager
from starlette.types import Scope, Receive, Send

class StarConsumers:

    def __init__(self, title: str = "StarConsumers", summary: str = None, description: str = ""):
        self.process_manager = ProcessManager()
        self.active_tasks: list[Task] = []
        self.all_tasks_map: dict[str, Task] = {}

        self.server = FastAPI(title=title, summary=summary, description=description, lifespan=self.start)
        

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self.server(scope, receive, send)

    async def start(self, _: FastAPI) -> AsyncGenerator[Any]:
        print("Starting the processes")
        self.process_manager.spawn(self.active_tasks)
        yield
        print("Terminating the processes")
        self.process_manager.terminate()
        print("Terminated the processes")

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

        if not selected_tasks:
            print("No task selected. We will run all existing tasks")
            self.active_tasks.extend(self.all_tasks_map.values())

        print(f"We selected the tasks {selected_tasks}")
        for task_name in selected_tasks:
            if task_name in self.all_tasks_map:
                self.active_tasks.append(self.all_tasks_map[task_name])

