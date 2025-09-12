"""Broker implementation."""

from starconsumers.exceptions import StarConsumersException
from starconsumers.logger import logger
from starconsumers.middlewares import BasePublisherMiddleware, BaseSubscriberMiddleware
from starconsumers.process import ProcessManager
from starconsumers.pubsub.pub import PubSubPublisherClient
from starconsumers.pubsub.sub import PubSubSubscriberClient
from starconsumers.registrator import Registrator
from starconsumers.router import Router
from starconsumers.subscriber import Subscriber


class Broker(Registrator):
    def __init__(
        self,
        project_id: str,
        routers: list[Router] = None,
        middlewares: list[type[BaseSubscriberMiddleware] | type[BasePublisherMiddleware]] = None,
    ):
        super().__init__(middlewares=middlewares)
        self.project_id = project_id
        self.process_manager = ProcessManager()

        self.routers: list[Router] = []
        if routers:
            for router in routers:
                self.include_router(router=router)

    def include_router(self, router: Router) -> None:
        router.set_project_id(self.project_id)
        for middleware in self.middlewares:
            router.add_middleware(middleware)

        for alias, subscriber in router.subscribers.items():
            if alias in self.subscribers:
                raise ValueError(f"Subscriber with alias '{alias}' already exists.")

            self.subscribers[alias] = subscriber

        self.routers.append(router)

    async def start(self, selected_subscribers: set[str] = None) -> None:
        subscribers = await self._filter_subscribers(selected_subscribers)

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
                        await self._create_topic(target_topic)
                        created_topics.add(target_topic)

                await self._create_subscription(subscriber)

            if subscriber.lifecycle_policy.autoupdate:
                await self._update_subscription(subscriber)

            self.process_manager.spawn(subscriber)

    async def _filter_subscribers(self, selected_subscribers: set[str]) -> list[Subscriber]:
        found_subscribers = []

        subscribers = {**self.subscribers}
        for router in self.routers:
            subscribers.update(router.subscribers)

        if not selected_subscribers or not isinstance(selected_subscribers, set):
            found_subscribers = list(subscribers.values())
            return found_subscribers

        aliases = [name.casefold() for name in selected_subscribers]
        for alias in aliases:
            if alias not in subscribers:
                logger.error(f"The '{alias}' not found")
                continue

            found_subscribers.append(subscribers[alias])

        if not found_subscribers:
            raise StarConsumersException(f"No subscriber aliases found for '{aliases}'")
        
        return found_subscribers

    async def _create_topic(self, topic_name: str) -> None:
        client = PubSubPublisherClient(project_id=self.project_id, topic_name=topic_name)
        client.create_topic()

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
