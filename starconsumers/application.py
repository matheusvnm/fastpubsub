from typing import Any, AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware import Middleware
from starconsumers.consumer import TopicConsumer
from starconsumers.datastructures import MessageMiddleware, Task
from starconsumers.exceptions import StarConsumersException
from starconsumers.middlewares import APMLogContextMiddleware, APMTransactionMiddleware, AsyncContextMiddleware, BasicExceptionHandler, MessageSerializerMiddleware
from starconsumers.process import ProcessManager
from starconsumers.logger import logger

from starlette.types import Scope, Receive, Send


class StarConsumers:

    def __init__(self):
        self.tasks: dict[str, Task] = {}
        self.active_tasks: list[Task] = []
        self.middlewares: list[Middleware] = []

        self._process_manager = ProcessManager()
        self._asgi_app = FastAPI(title="StarConsumers", lifespan=self.start)
        self._asgi_app.add_api_route(path="/health", endpoint=self._health_route, methods=["GET"])
        
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self._asgi_app(scope, receive, send)

    async def start(self, _: FastAPI) -> AsyncGenerator[Any]:
        logger.info("Starting the processes")

        self._process_manager.spawn(self.active_tasks)
        yield
        logger.info("Terminating the processes")
        self._process_manager.terminate()
        logger.info("Terminated the processes")

    def add_consumer(self, consumer: TopicConsumer):
        if not isinstance(consumer, TopicConsumer):
            raise ValueError(f"The consumer must be {TopicConsumer.__name__} instance")
        
        for task_name, task in consumer.task_map.items():
            if task_name in self.tasks:
                new_subscription = task.subscription.name
                existing_subscription = self.tasks[task_name].subscription.name
                raise ValueError(f"Duplicated task name {task_name} for subscriptions {new_subscription} and {existing_subscription}")

            self.tasks[task_name] = task

    def add_middleware(self, middleware: type[MessageMiddleware], *args: list, **kwargs: dict):
        if not (isinstance(middleware, type) and issubclass(middleware, MessageMiddleware)):
            raise ValueError(f"The consumer must implement {MessageMiddleware.__name__} class")

        self.middlewares.append(Middleware(middleware, *args, **kwargs))

    def activate_tasks(self, tasks_names: list[str]) -> None:
        if not isinstance(tasks_names, list):
            raise ValueError("You can only add the name of the tasks as list.")
        
        selected_tasks = set()
        for task_name in tasks_names:
            selected_tasks.add(task_name.casefold())

        if not selected_tasks:
            self._activate_all_tasks()
            return

        self._activate_selected_tasks(selected_tasks)

    def _activate_all_tasks(self):
        logger.info("No task selected. We will run all existing tasks")
        for task in self.tasks.values():
            new_task = self._build_task_middleware_stack(task)
            self.active_tasks.append(new_task)

    def _activate_selected_tasks(self, selected_tasks: set):
        logger.info(f"We selected the tasks {selected_tasks}")
        for task_name in selected_tasks:
            if not task_name in self.tasks:
                logger.warning(f"The task {task_name} not found in tasklist")
                continue

            task = self.tasks[task_name]
            new_task = self._build_task_middleware_stack(task)
            self.active_tasks.append(new_task)
        
        if not self.active_tasks:
            raise StarConsumersException("No task found to execute. Please check their names and use the --tasks argument to call them")

    def _build_task_middleware_stack(self, task: Task):
        pre_user_middlewares = [Middleware(APMTransactionMiddleware),
                                Middleware(APMLogContextMiddleware),
                                Middleware(BasicExceptionHandler),
                                Middleware(MessageSerializerMiddleware)]

        pos_user_middlewares = [Middleware(AsyncContextMiddleware)]

        middlewares = (
            pre_user_middlewares
            + self.middlewares +
            pos_user_middlewares
        )

        handler = task.handler.next_call
        for cls, args, kwargs in reversed(middlewares):
            handler = cls(next_call=handler, *args, **kwargs)

        return Task(
            handler=handler,
            autocreate=task.autocreate,
            subscription=task.subscription
        )

    async def _health_route(self):
        return self._process_manager.probe_processes()
        
        