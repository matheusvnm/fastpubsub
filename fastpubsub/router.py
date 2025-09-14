
from fastpubsub.middlewares import BasePublisherMiddleware, BaseSubscriberMiddleware
from fastpubsub.registrator import Registrator, RouterRegistrator
from fastpubsub.subscriber import Subscriber


class PubSubRouter(Registrator, RouterRegistrator):
    def __init__(
        self,
        prefix: str,
        *,
        routers: list["PubSubRouter"] = None,
        middlewares: list[type[BaseSubscriberMiddleware], type[BasePublisherMiddleware]] = None,
    ):
        super().__init__(middlewares=middlewares)
        super(Registrator, self).__init__(routers=routers)

        self.prefix = prefix  # TODO: Add prefix validation (only letter/numbers/underscore/slashed -- should not have special chars, point or space)

    def set_project_id(self, project_id: str) -> None:
        self.project_id = project_id

        router: PubSubRouter
        for router in self.routers:
            router.set_project_id(project_id)

        for publisher in self.publishers.values():
            publisher.set_project_id(self.project_id)

        for subscriber in self.subscribers.values():
            subscriber.set_project_id(self.project_id)

    def include_router(self, router: "PubSubRouter") -> None:
        super(Registrator, self).include_router(router)

        router.add_prefix(self.prefix)
        for middleware in self.middlewares:
            router.include_middleware(middleware)

    def add_prefix(self, new_prefix: str):
        self.prefix = f"{new_prefix}.{self.prefix}"

        subscribers_to_realias = dict(self.subscribers)

        self.subscribers.clear()
        for alias, subscriber in subscribers_to_realias.items():
            subscriber.add_prefix(new_prefix)

            new_prefixed_alias = f"{new_prefix}.{alias}"
            self.subscribers[new_prefixed_alias] = subscriber

    def get_subscribers(self) -> dict[str, Subscriber]:
        subscribers = dict()

        router: PubSubRouter
        for router in self.routers:
            subscribers.update(router.subscribers)

        subscribers.update(self.subscribers)
        return subscribers
