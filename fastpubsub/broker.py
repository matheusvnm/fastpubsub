"""Broker implementation."""

import os
from typing import Any

from fastpubsub.baserouter import BaseRouter
from fastpubsub.clients.pub import PubSubPublisherClient
from fastpubsub.clients.sub import PubSubSubscriberClient
from fastpubsub.concurrency.controller import ProcessController
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.logger import logger
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.subscriber import Subscriber
from fastpubsub.router import PubSubRouter


class BrokerConfigurator:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.created_topics = set()

    def create_topics(self, subscriber: Subscriber):
        target_topic = subscriber.topic_name
        self._new_topic(target_topic, create_default_subscription=False)

        if subscriber.dead_letter_policy:
            target_topic = subscriber.dead_letter_policy.topic_name
            self._new_topic(target_topic)

    def _new_topic(self, topic_name: str, create_default_subscription=True):
        if topic_name in self.created_topics:
            return

        client = PubSubPublisherClient(project_id=self.project_id, topic_name=topic_name)
        client.create_topic(create_default_subscription)
        self.created_topics.add(topic_name)

    def create_subscription(self, subscriber: Subscriber) -> None:
        client = PubSubSubscriberClient()
        client.create_subscription(subscriber=subscriber)

    def update_subscription(self, subscriber: Subscriber) -> None:
        client = PubSubSubscriberClient()
        client.update_subscription(subscriber=subscriber)


class PubSubBroker(BaseRouter):
    def __init__(
        self,
        project_id: str,
        routers: list[PubSubRouter] = None,
        middlewares: list[type[BaseMiddleware]] = None,
    ):
        if not (project_id and isinstance(project_id, str) and len(project_id.strip()) > 0):
            raise FastPubSubException(f"The project id value ({project_id}) is invalid.")

        self.process_manager = ProcessController()
        self.broker_configurator = BrokerConfigurator(project_id=project_id)
        super().__init__(project_id=project_id, routers=routers, middlewares=middlewares)

    def include_router(self, router: PubSubRouter) -> None:
        if not (router and isinstance(router, PubSubRouter)):
            raise FastPubSubException(f"Your routers must be of type {PubSubRouter.__name__}")

        for existing_router in self.routers:
            if existing_router.prefix == router.prefix:
                raise FastPubSubException(
                    f"The prefixes must be unique. The prefix={router.prefix} is duplicated."
                )

        router.propagate_project_id(self.project_id)
        for middleware in self.middlewares:
            router.include_middleware(middleware)

        for alias in router.subscribers.keys():
            if alias in self.subscribers:
                raise ValueError(f"Subscriber with alias '{alias}' already exists.")

        self.routers.append(router)

    async def start(self) -> None:
        subscribers = await self._filter_subscribers()

        for subscriber in subscribers:
            if subscriber.lifecycle_policy.autocreate:
                self.broker_configurator.create_topics(subscriber=subscriber)
                self.broker_configurator.create_subscription(subscriber=subscriber)

            if subscriber.lifecycle_policy.autoupdate:
                await self.broker_configurator.update_subscription(subscriber)

            self.process_manager.add_subscriber(subscriber)

        if not subscribers:
            logger.info("No subscriber detected, we will run as an API-only.")

        self.process_manager.start()

    def info(self) -> dict[str, Any]:
        return self.process_manager.get_info()

    def alive(self) -> bool:
        subscribers = self.process_manager.get_liveness()
        if not subscribers:
            logger.info("The subscribers are not active. May be they are deactivated?")
            return False

        alive = True
        for name, liveness in subscribers.items():
            if not liveness:
                logger.error(f"The {name} subscriber handler is not alive")
                alive = False

        return alive

    def ready(self) -> bool:
        subscribers = self.process_manager.get_readiness()
        if not subscribers:
            logger.info("The subscribers are not active. May be they are deactivated?")
            return False

        ready = True
        for name, readiness in subscribers.items():
            if not readiness:
                logger.error(f"The {name} subscriber handler is not ready")
                ready = False
                return ready

        return ready

    async def _filter_subscribers(self) -> list[Subscriber]:
        subscribers = self.get_subscribers()
        selected_subscribers = self._get_selected_subscribers()

        if not selected_subscribers:
            logger.debug(f"Running all the subscribers as {list(subscribers.keys())}")
            return list(subscribers.values())

        found_subscribers = []
        for selected_subscriber in selected_subscribers:
            if selected_subscriber not in subscribers:
                logger.warning(f"The '{selected_subscriber}' subscriber alias not found")
                continue

            logger.debug(f"We have found the subscriber '{selected_subscriber}'")
            found_subscribers.append(subscribers[selected_subscriber])

        return found_subscribers

    def _get_selected_subscribers(self) -> set[str]:
        selected_subscribers = set()
        # TODO: Add API Only feature
        subscribers_text = os.getenv("FASTPUBSUB_SUBSCRIBERS", "")
        if not subscribers_text:
            return selected_subscribers

        dirty_aliases = subscribers_text.split(",")
        for dirty_alias in dirty_aliases:
            clean_alias = dirty_alias.lower().strip()
            if clean_alias:
                selected_subscribers.add(clean_alias)

        return selected_subscribers

    async def shutdown(self) -> None:
        self.process_manager.terminate()
