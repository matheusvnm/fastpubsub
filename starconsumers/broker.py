"""Broker implementation."""

from starconsumers.exceptions import StarConsumersException
from starconsumers.logger import logger
from starconsumers.middlewares import BasePublisherMiddleware, BaseSubscriberMiddleware
from starconsumers.process import ProcessManager
from starconsumers.pubsub.clients import PubSubPublisherClient, PubSubSubscriberClient
from starconsumers.registrator import Registrator
from starconsumers.router import BrokerRouter
from starconsumers.subscriber import Subscriber


class Broker(Registrator):
    def __init__(
        self,
        project_id: str,
        middlewares: list[type[BaseSubscriberMiddleware] | type[BasePublisherMiddleware]] = None,
    ):
        super().__init__(middlewares=middlewares)
        self.project_id = project_id
        self.process_manager = ProcessManager()

    def include_router(self, router: BrokerRouter) -> None:
        router.set_project_id(self.project_id)
        for middleware in self.middlewares:
            router.add_middleware(middleware)

        for alias, subscriber in router.subscribers.items():
            if alias in self.subscribers:
                raise ValueError(f"Subscriber with alias '{alias}' already exists.")

            self.subscribers[alias] = subscriber

    async def start(self, selected_subscribers: set[str] = None) -> None:
        await self._drop_unused_subscribers(selected_subscribers)

        created_topics = set()
        for alias, subscriber in self.subscribers.items():
            logger.info(f"Starting the subscription '{alias}'")

            target_topic = subscriber.topic_name
            if subscriber.lifecycle_policy.autocreate:
                if target_topic not in created_topics:
                    await self._create_topic(target_topic)
                    created_topics.add(target_topic)

                if subscriber.dead_letter_policy:
                    target_topic = subscriber.dead_letter_policy.topic_name
                    if target_topic not in created_topics:
                        await self._create_topic(target_topic)
                        created_topics.add(target_topic)

                await self._create_subscription(subscriber)

            if subscriber.lifecycle_policy.autoupdate:
                await self._update_subscription(subscriber)

            self.process_manager.spawn(subscriber)

    async def _drop_unused_subscribers(self, selected_subscribers: set[str]) -> None:
        if not selected_subscribers:
            return

        for selected_subscriber in selected_subscribers:
            if selected_subscriber not in self.subscribers:
                raise StarConsumersException(f"The {selected_subscriber} does not exists")

        for subscription_name in self.subscribers.keys():
            if subscription_name not in self.subscribers:
                del self.subscribers[subscription_name]

    async def _create_topic(self, topic_name: str) -> None:
        logger.info(f"The topic '{topic_name}' will be created if does not exists.")
        client = PubSubPublisherClient(project_id=self.project_id, topic_name=topic_name)
        client.create_topic()

    async def _create_subscription(self, subscriber: Subscriber) -> None:
        logger.info(
            f"The subscription '{subscriber.subscription_name}' will be created if does not exists."
        )
        client = PubSubSubscriberClient()
        client.create_subscription(subscriber=subscriber)

    async def _update_subscription(self, subscriber: Subscriber) -> None:
        logger.info(f"The subscription '{subscriber.subscription_name}' will be updated if exists.")
        client = PubSubSubscriberClient()
        client.update_subscription(subscriber=subscriber)
        # TODO: Checar o que ocorre se uma inscrição não criada for atualizada

    async def shutdown(self) -> None:
        """Shutdown the broker."""
        for alias, subscriber in self.subscribers.items():
            logger.info(f"Stopping the the subscription '{alias}'")
            self.process_manager.terminate(subscriber)
