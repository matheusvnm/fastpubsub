"""Broker implementation."""

import os
from typing import Any

from pydantic import BaseModel, validate_call

from fastpubsub.clients.pub import PubSubPublisherClient
from fastpubsub.clients.sub import PubSubSubscriberClient
from fastpubsub.concurrency.controller import ProcessController
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.logger import logger
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.pubsub.publisher import Publisher
from fastpubsub.pubsub.subscriber import Subscriber
from fastpubsub.router import PubSubRouter
from fastpubsub.types import SubscribedCallable


class SubscriptionBuilder:
    def __init__(self, subscriber: Subscriber):
        self.subscriber = subscriber
        self.created_topics: set[str] = set()

    def build(self) -> None:
        if self.subscriber.lifecycle_policy.autocreate:
            self._create_topics()
            self._create_subscription()

        if self.subscriber.lifecycle_policy.autoupdate:
            self._update_subscription()

    def _create_topics(self) -> None:
        target_topic = self.subscriber.topic_name
        self._new_topic(target_topic, create_default_subscription=False)

        if self.subscriber.dead_letter_policy:
            target_topic = self.subscriber.dead_letter_policy.topic_name
            self._new_topic(target_topic)

    def _new_topic(self, topic_name: str, create_default_subscription: bool = True) -> None:
        if topic_name in self.created_topics:
            return

        client = PubSubPublisherClient(project_id=self.subscriber.project_id, topic_name=topic_name)
        client.create_topic(create_default_subscription)
        self.created_topics.add(topic_name)

    def _create_subscription(self) -> None:
        client = PubSubSubscriberClient()
        client.create_subscription(subscriber=self.subscriber)

    def _update_subscription(self) -> None:
        client = PubSubSubscriberClient()
        client.update_subscription(subscriber=self.subscriber)


class PubSubBroker:
    def __init__(
        self,
        project_id: str,
        routers: tuple[PubSubRouter] | None = None,
        middlewares: tuple[type[BaseMiddleware]] | None = None,
    ):
        if not (project_id and isinstance(project_id, str) and len(project_id.strip()) > 0):
            raise FastPubSubException(f"The project id value ({project_id}) is invalid.")

        self.process_controller = ProcessController()
        self.router = PubSubRouter(routers=routers, middlewares=middlewares)
        self.router._propagate_project_id(project_id)

    @validate_call
    def subscriber(
        self,
        alias: str,
        *,
        topic_name: str,
        subscription_name: str,
        autocreate: bool = True,
        autoupdate: bool = False,
        filter_expression: str = "",
        dead_letter_topic: str = "",
        max_delivery_attempts: int = 5,
        ack_deadline_seconds: int = 60,
        enable_message_ordering: bool = False,
        enable_exactly_once_delivery: bool = False,
        min_backoff_delay_secs: int = 10,
        max_backoff_delay_secs: int = 600,
        max_messages: int = 1000,
        max_messages_bytes: int = 100 * 1024 * 1024,
        middlewares: tuple[type[BaseMiddleware]] | Any = None,
    ) -> SubscribedCallable:
        return self.router.subscriber(
            alias=alias,
            topic_name=topic_name,
            subscription_name=subscription_name,
            autocreate=autocreate,
            autoupdate=autoupdate,
            filter_expression=filter_expression,
            dead_letter_topic=dead_letter_topic,
            max_delivery_attempts=max_delivery_attempts,
            ack_deadline_seconds=ack_deadline_seconds,
            enable_message_ordering=enable_message_ordering,
            enable_exactly_once_delivery=enable_exactly_once_delivery,
            min_backoff_delay_secs=min_backoff_delay_secs,
            max_backoff_delay_secs=max_backoff_delay_secs,
            max_messages=max_messages,
            max_messages_bytes=max_messages_bytes,
            middlewares=middlewares,
        )

    @validate_call
    def publisher(self, topic_name: str) -> Publisher:
        return self.router.publisher(topic_name=topic_name)

    @validate_call
    async def publish(
        self,
        topic_name: str,
        data: BaseModel | dict[str, Any] | str | bytes,
        ordering_key: str | None = None,
        attributes: dict[str, str] | None = None,
        autocreate: bool = True,
    ) -> None:
        return await self.router.publish(
            topic_name=topic_name,
            data=data,
            ordering_key=ordering_key,
            attributes=attributes,
            autocreate=autocreate,
        )

    def include_router(self, router: PubSubRouter) -> None:
        return self.router.include_router(router)

    @validate_call
    def include_middleware(self, middleware: type[BaseMiddleware]) -> None:
        return self.router.include_middleware(middleware)

    async def start(self) -> None:
        subscribers = await self._filter_subscribers()
        if not subscribers:
            logger.error("No subscriber detected!")
            raise FastPubSubException(
                "You must select subscribers (using --subscribers flag) or run them all."
            )

        for subscriber in subscribers:
            subscription_builder = SubscriptionBuilder(subscriber=subscriber)
            subscription_builder.build()
            self.process_controller.add_subscriber(subscriber)

        self.process_controller.start()

    def info(self) -> dict[str, Any]:
        return self.process_controller.get_info()

    def alive(self) -> bool:
        subscribers = self.process_controller.get_liveness()
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
        subscribers = self.process_controller.get_readiness()
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
        subscribers = self.router._get_subscribers()
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
        selected_subscribers: set[str] = set()
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
        self.process_controller.terminate()
