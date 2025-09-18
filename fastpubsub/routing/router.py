import re

from fastpubsub.exceptions import StarConsumersException
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.routing.base import BaseRouter

_PREFIX_REGEX = re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9_./]*[a-zA-Z0-9])?$")


class PubSubRouter(BaseRouter):
    def __init__(
        self,
        prefix: str,
        *,
        routers: list["PubSubRouter"] = None,
        middlewares: list[type[BaseMiddleware]] = None,
    ):
        if not isinstance(prefix, str) or not _PREFIX_REGEX.match(prefix):
            raise StarConsumersException(
                "Prefix must be a string that starts and ends with a letter or number, "
                "and can only contain periods, slashes, or underscores in the middle."
            )

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
        if not (router and isinstance(router, PubSubRouter)):
            raise StarConsumersException(f"Your routers must be of type {PubSubRouter.__name__}")

        for existing_router in self.routers:
            if existing_router.prefix == router.prefix:
                raise StarConsumersException(
                    f"The prefix={router.prefix} is duplicated, it must be unique."
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
