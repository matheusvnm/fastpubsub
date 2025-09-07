"""Publisher logic."""
from typing import Any

from starconsumers.middlewares import BasePublisherMiddleware


class Publisher:
    def __init__(self, 
                 project_id: str, 
                 topic_name: str,
                 middlewares: list[BasePublisherMiddleware]
                 ):
        self.project_id = project_id
        self.topic_name = topic_name
        self.middlewares = middlewares

    async def publish(self, data: dict, ordering_key: str = "", attributes: dict = None) -> None:
       # TODO: Build middlewares
       # 
       pass

    def add_middleware(self, middleware: BasePublisherMiddleware) -> None:
        if not (middleware and isinstance(middleware, BasePublisherMiddleware)):
            return
        
        self.middlewares.append(middleware)