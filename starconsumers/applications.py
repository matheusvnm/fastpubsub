from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from starlette.types import Receive, Scope, Send

from starconsumers.consumers import TopicConsumer
from starconsumers.datastructures import (
    MessageMiddleware,
    MiddlewareContainer,
    MiddlewaresRegister,
    Task,
    WrappedTask,
)
from starconsumers.exceptions import StarConsumersException
from starconsumers.logger import logger
from starconsumers.middlewares import (
    MiddlewareChainBuilder,
)
from starconsumers.process import ProcessManager


class StarConsumers:
    def __init__(self) -> None:
        self._process_manager = ProcessManager()
        self._asgi_app = FastAPI(title="StarConsumers", lifespan=self.start)
        self._asgi_app.add_api_route(path="/health", endpoint=self._health_route, methods=["GET"])

        self._active_tasks: list[WrappedTask] = []
        self._tasks: dict[str, tuple[Task, list[MiddlewareContainer]]] = {}
        self._middlewares_register = MiddlewaresRegister()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        await self._asgi_app(scope, receive, send)

    @asynccontextmanager
    async def start(self, _: FastAPI) -> AsyncGenerator[Any]:
        logger.info("Starting the processes")
        self._process_manager.spawn(self._active_tasks)
        yield
        logger.info("Terminating the processes")
        self._process_manager.terminate()
        logger.info("Terminated the processes")

    def add_consumer(self, consumer: TopicConsumer) -> None:
        if not isinstance(consumer, TopicConsumer):
            raise ValueError(f"The consumer must be {TopicConsumer.__name__} instance")

        tasks = consumer.tasks_register.get()
        middlewares = consumer.middlewares_register.get()
        for name, task in tasks.items():
            if name in self._tasks:
                existing_task, _ = self._tasks[name]
                raise ValueError(
                    f"Duplicated task name '{name}' for subscriptions "
                    f"{existing_task.subscription.name} and {task.subscription.name}"
                )

            self._tasks[name] = (task, middlewares)

    def add_middleware(
        self, middleware: type[MessageMiddleware], *args: list[str], **kwargs: dict[str, str]
    ) -> None:
        self._middlewares_register.register(middleware, *args, **kwargs)

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

    def _activate_all_tasks(self) -> None:
        logger.info("No task selected. We will run all existing tasks")
        for task, middlewares in self._tasks.values():
            wrapped_task = self._wrap_task(task, middlewares)
            self._active_tasks.append(wrapped_task)

    def _activate_selected_tasks(self, selected_tasks: set[str]) -> None:
        logger.info(f"We selected the tasks {selected_tasks}")
        for task_name in selected_tasks:
            if task_name not in self._tasks:
                logger.warning(f"The task {task_name} not found in tasklist")
                continue

            task, middlewares = self._tasks[task_name]
            wrapped_task = self._wrap_task(task, middlewares)
            self._active_tasks.append(wrapped_task)

        if not self._active_tasks:
            raise StarConsumersException(
                "No task found to execute. "
                "Please check their names and use the --tasks argument to call them"
            )

    def _wrap_task(self, task: Task, middlewares: list[MiddlewareContainer]) -> WrappedTask:
        global_middlewares = self._middlewares_register.get()
        builder = MiddlewareChainBuilder(
            handler=task.handler,
            local_middlewares=middlewares,
            global_middlewares=global_middlewares,
        )
        handler = builder.build()
        return WrappedTask(handler=handler, subscription=task.subscription)

    async def _health_route(self) -> dict[str, str]:
        return self._process_manager.probe_processes()
