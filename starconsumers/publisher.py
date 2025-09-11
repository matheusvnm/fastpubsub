"""Publisher logic."""

from starconsumers.concurrency import ensure_async_callable
from starconsumers.middlewares import BasePublisherMiddleware, MessagePublishCommand


class Publisher:
    def __init__(
        self, project_id: str, topic_name: str, middlewares: list[type[BasePublisherMiddleware]]
    ):
        self.project_id = project_id
        self.topic_name = topic_name
        self.middlewares = middlewares
        self.middlewares: list[type[BasePublisherMiddleware]] = []

        if middlewares:
            for middleware in middlewares:
                self.add_middleware(middleware)

    async def publish(self, data: dict, ordering_key: str = "", attributes: dict = None) -> None:
        publisher = MessagePublishCommand(project_id=self.project_id, topic_name=self.topic_name)
        for middleware in self.middlewares:
            publisher = middleware(publisher)

        await publisher(data=data, ordering_key=ordering_key, attributes=attributes)

    def add_middleware(self, middleware: type[BasePublisherMiddleware]) -> None:
        if not (middleware and issubclass(middleware, BasePublisherMiddleware)):
            return

        if middleware in self.middlewares:
            return

        ensure_async_callable(middleware)
        self.middlewares.append(middleware)

    def set_project_id(self, project_id: str):
        self.project_id = project_id
