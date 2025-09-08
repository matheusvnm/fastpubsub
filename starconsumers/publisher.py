"""Publisher logic."""

from starconsumers.middlewares import BasePublisherMiddleware
from starconsumers.pubsub.clients import PubSubPublisherClient


class Publisher:
    def __init__(
        self, project_id: str, topic_name: str, middlewares: list[type[BasePublisherMiddleware]]
    ):
        self.project_id = project_id
        self.topic_name = topic_name
        self.middlewares = middlewares

    async def publish(self, data: dict, ordering_key: str = "", attributes: dict = None) -> None:
        # TODO: Build middlewares
        client = PubSubPublisherClient(project_id=self.project_id, topic_name=self.topic_name)
        client.publish(data=data, ordering_key=ordering_key, attributes=attributes)

    def add_middleware(self, middleware: type[BasePublisherMiddleware]) -> None:
        if not (middleware and issubclass(middleware, BasePublisherMiddleware)):
            return

        if middleware in self.middlewares:
            return

        self.middlewares.append(middleware)

    def set_project_id(self, project_id: str):
        self.project_id = project_id
