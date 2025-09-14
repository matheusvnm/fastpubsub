"""Broker implementation."""

import os

from fastpubsub.clients.pub import PubSubPublisherClient
from fastpubsub.clients.sub import PubSubSubscriberClient
from fastpubsub.exceptions import StarConsumersException
from fastpubsub.logger import logger
from fastpubsub.middlewares import BasePublisherMiddleware, BaseSubscriberMiddleware
from fastpubsub.process import ProcessManager
from fastpubsub.routing.base_router import Router
from fastpubsub.routing.router import PubSubRouter
from fastpubsub.subscriber import Subscriber


class PubSubBroker(Router):
    def __init__(
        self,
        project_id: str,
        routers: list[PubSubRouter] = None,
        middlewares: list[type[BaseSubscriberMiddleware] | type[BasePublisherMiddleware]] = None,
    ):
        if not (project_id and isinstance(project_id, str) and len(project_id.strip()) > 0):
            raise StarConsumersException(f"The project id value ({project_id}) is invalid.")

        super().__init__(project_id=project_id, routers=routers, middlewares=middlewares)
        self.process_manager = ProcessManager()


    def include_router(self, router: PubSubRouter) -> None:
        super().include_router(router)

        router.set_project_id(self.project_id)
        for middleware in self.middlewares:
            router.include_middleware(middleware)

        for alias, subscriber in router.subscribers.items():
            if alias in self.subscribers:
                raise ValueError(f"Subscriber with alias '{alias}' already exists.")

            self.subscribers[alias] = subscriber

    async def start(self) -> None:
        subscribers = await self._filter_subscribers()

        created_topics = set()
        for subscriber in subscribers:
            target_topic = subscriber.topic_name
            if subscriber.lifecycle_policy.autocreate:
                if target_topic not in created_topics:
                    await self._create_topic(target_topic)
                    created_topics.add(target_topic)

                if subscriber.dead_letter_policy:
                    target_topic = subscriber.dead_letter_policy.topic_name
                    if target_topic not in created_topics:
                        await self._create_topic(target_topic, create_default_subscription=True)
                        created_topics.add(target_topic)

                await self._create_subscription(subscriber)

            if subscriber.lifecycle_policy.autoupdate:
                await self._update_subscription(subscriber)

            self.process_manager.spawn(subscriber)

    async def _filter_subscribers(self) -> list[Subscriber]:
        found_subscribers = []
        subscribers = dict(self.subscribers)

        router: PubSubRouter
        for router in self.routers:
            subscribers.update(router.get_subscribers())

        selected_subscribers = self._get_selected_subscribers()
        if not selected_subscribers:
            found_subscribers = list(subscribers.values())
            logger.debug(f"Running all the subscribers as {list(subscribers.keys())}")
            return found_subscribers

        for selected_subscriber in selected_subscribers:
            if selected_subscriber not in subscribers:
                logger.warning(f"The '{selected_subscriber}' subscriber alias not found")
                continue

            logger.debug(f"We have found the subscriber '{selected_subscriber}'")
            found_subscribers.append(subscribers[selected_subscriber])

        if not found_subscribers:
            raise StarConsumersException(
                f"No subscriber found for '{selected_subscriber}'. It should be one of {list(subscribers.keys())}"
            )

        return found_subscribers

    def _get_selected_subscribers(self) -> set[str]:
        selected_subscribers = set()
        subscribers_text = os.getenv("FASTPUBSUB_SUBSCRIBERS", "")
        if not subscribers_text:
            return selected_subscribers

        dirty_aliases = subscribers_text.split(",")
        for dirty_alias in dirty_aliases:
            clean_alias = dirty_alias.lower().strip()
            if clean_alias:
                selected_subscribers.add(clean_alias)

        return selected_subscribers

    async def _create_topic(
        self, topic_name: str, create_default_subscription: bool = False
    ) -> None:
        client = PubSubPublisherClient(project_id=self.project_id, topic_name=topic_name)
        client.create_topic(create_default_subscription)

    async def _create_subscription(self, subscriber: Subscriber) -> None:
        client = PubSubSubscriberClient()
        client.create_subscription(subscriber=subscriber)

    async def _update_subscription(self, subscriber: Subscriber) -> None:
        client = PubSubSubscriberClient()
        client.update_subscription(subscriber=subscriber)
        # TODO: Checar o que ocorre se uma inscrição não criada for atualizada

    async def shutdown(self) -> None:
        """Shutdown the broker."""
        for alias, subscriber in self.subscribers.items():
            logger.info(f"Stopping the the subscription '{alias}'")
            self.process_manager.terminate(subscriber)
