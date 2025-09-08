"""Broker implementation."""

import asyncio
from typing import Union

from starconsumers.exceptions import StarConsumersException
from starconsumers.middlewares import BasePublisherMiddleware, BaseSubscriberMiddleware
from starconsumers.pubsub.clients import PubSubSubscriberClient, PubSubPublisherClient
from starconsumers.registrator import Registrator
from starconsumers.router import BrokerRouter
from starconsumers.subscriber import Subscriber


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
        router.set_project_id(self.project_id)
        for middleware in self.middlewares:
            router.add_middleware(middleware)

        for alias in router.subscribers.keys():
            if alias in self.subscribers:
                raise ValueError(f"Subscriber with alias '{alias}' already exists.")

            self.subscribers[alias] = router.subscribers.pop(alias)


    # Should return a ASGI app to attach in uvicorn (like FastAPI).
    async def start(self, selected_subscribers: set[str] = set()) -> None:
        """Start the broker."""

        await self._drop_unused_subscribers(selected_subscribers)

        created_topics = set()
        created_subscriptions = set()
        for subscriber in self.subscribers.values():
            target_topic = subscriber.topic_name
            target_subscription = subscriber.subscription_name

            group: asyncio.TaskGroup
            with asyncio.TaskGroup() as group:
                if subscriber.lifecycle_policy.autocreate:
                    if target_topic not in created_topics:
                        group.create_task(self._create_topic(target_topic)) 
                        created_topics.add(target_topic)
                    
                    if subscriber.dead_letter_policy:
                        target_topic = subscriber.dead_letter_policy.topic_name
                        if target_topic not in created_topics:
                            group.create_task(self._create_topic(target_topic)) 
                            created_topics.add(target_topic)

                    if target_subscription not in created_subscriptions:
                        group.create_task(self._create_subscription(subscriber)) 
                        created_subscriptions.add(created_subscriptions)

                if subscriber.lifecycle_policy.autoupdate:
                    group.create_task(self._update_subscription(subscriber)) 

            #self.process_manager.spawn(subscriber)

    async def _drop_unused_subscribers(self, selected_subscribers: set[str]) -> None:
        if not selected_subscribers:
            # We will run all the tasks
            return 
        
        for selected_subscriber in selected_subscribers:
            if selected_subscriber not in self.subscribers:
                raise StarConsumersException(f"The {selected_subscriber} does not exists")
            
        for subscription_name in self.subscribers.keys():
            if subscription_name not in self.subscribers:
                del self.subscribers[subscription_name]

    async def _create_topic(self, topic_name: str) -> None:
       client = PubSubPublisherClient(self.project_id, topic_name=topic_name)
       client.create_topic()

    async def _create_subscription(self, subscriber: Subscriber) -> None:
        client = PubSubSubscriberClient()
        client.create_subscription(subscriber=subscriber)

    async def _update_subscription(self, subscriber: Subscriber) -> None:
        client = PubSubSubscriberClient()
        # TODO: Checar o que ocorre se uma inscrição não criada for atualizada
        client.update_subscription(subscriber=subscriber)


    async def shutdown(self) -> None:
        """Shutdown the broker."""
        for subscriber in self.subscribers.values():
            # This is a placeholder for the subscription shutdown logic
            # It should send a message to process manager to shutdown its subscribers
            pass
