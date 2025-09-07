"""Broker implementation."""

from re import S
from typing import Any, Callable, Dict, List, Optional, Union

from starconsumers.middlewares import BasePublisherMiddleware, BaseSubscriberMiddleware
from starconsumers.publisher import Publisher
from starconsumers.pubsub.publisher import PubSubPublisher
from starconsumers.registrator import Registrator
from starconsumers.subscriber import Subscriber


class BrokerRouter(Registrator):
    def __init__(
        self,
        prefix: str = "",
        middlewares: list[
            Union[BasePublisherMiddleware, BaseSubscriberMiddleware]
        ] = [],
    ):
        super().__init__()
        self.prefix = prefix
        self.middlewares = middlewares



class Broker(Registrator):
    def __init__(
        self,
        project_id: str,
        middlewares: list[
            Union[BasePublisherMiddleware, BaseSubscriberMiddleware]
        ] = [],
    ):
        super().__init__(middlewares=middlewares)
        self.project_id = project_id
        # TODO: Create a process manager


    def include_router(self, router: BrokerRouter) -> None:
        for alias, subscriber in router.subscribers.items():
            if alias in self.subscribers:
                raise ValueError(f"Subscriber with alias '{alias}' already exists.")
            
            for middleware in self.middlewares:
                subscriber.add_middleware(middleware)
                self.subscribers[alias] = subscriber

        for topic_name, publisher in router.publishers.items():
            for middleware in self.middlewares:
                publisher.add_middleware(middleware)
            self.publishers[topic_name] = publisher


    # Should return a ASGI app to attach in uvicorn (like FastAPI).
    async def start(self) -> None:
        """Start the broker."""
        for subscriber in self.subscribers.values():
            # TODO: Create all topics (Normal and DLT).
            # TODO: Create all subscriptions
            
            
            pass

    async def shutdown(self) -> None:
        """Shutdown the broker."""
        for subscriber in self.subscribers.values():
            # This is a placeholder for the subscription shutdown logic
            pass
