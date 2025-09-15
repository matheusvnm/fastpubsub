from fastpubsub.middlewares import BasePublisherMiddleware, BaseSubscriberMiddleware
from fastpubsub.routing.base_router import Router
from fastpubsub.subscriber import Subscriber
from fastpubsub.exceptions import StarConsumersException

class PubSubRouter(Router):
    def __init__(
        self,
        prefix: str,
        *,
        routers: list["PubSubRouter"] = None,
        middlewares: list[type[BaseSubscriberMiddleware], type[BasePublisherMiddleware]] = None,
    ):
        
        # TODO: Add prefix validation
        # It must start/end with letter/numbers
        # It can have periods, slashes and underscore in the middle.
        super().__init__(prefix=prefix, routers=routers, middlewares=middlewares)

    def propagate_project_id(self, project_id: str) -> None:
        self.project_id = project_id

        router: PubSubRouter
        for router in self.routers:
            router.add_prefix(self.prefix)
            router.propagate_project_id(project_id)

        for publisher in self.publishers.values():
            publisher.set_project_id(self.project_id)

        for subscriber in self.subscribers.values():
            subscriber.set_project_id(self.project_id)

    def include_router(self, router: "PubSubRouter") -> None:
        if not isinstance(router, PubSubRouter):
            raise StarConsumersException(
                f"Your routers must be of type {PubSubRouter.__name__}"
            )

        router.add_prefix(self.prefix)
        router.propagate_project_id(self.project_id)
        for middleware in self.middlewares:
            router.include_middleware(middleware)

        self.routers.append(router)

    def add_prefix(self, new_prefix: str):
        if new_prefix in self.prefix:
            return

        self.prefix = f"{new_prefix}.{self.prefix}"
        subscribers_to_realias = dict(self.subscribers)

        self.subscribers.clear()
        for alias, subscriber in subscribers_to_realias.items():
            subscriber.add_prefix(new_prefix)

            new_prefixed_alias = f"{new_prefix}.{alias}"
            self.subscribers[new_prefixed_alias] = subscriber