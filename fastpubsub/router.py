import re
from typing import cast

from fastpubsub.baserouter import BaseRouter
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares.base import BaseMiddleware

_PREFIX_REGEX = re.compile(r"^[a-zA-Z0-9]([a-zA-Z0-9_./]*[a-zA-Z0-9])?$")


class PubSubRouter(BaseRouter):
    def __init__(
        self,
        prefix: str = "",
        *,
        routers: tuple["PubSubRouter"] | None = None,
        middlewares: tuple[type[BaseMiddleware]] | None = None,
    ):
        # if not isinstance(prefix, str) or not _PREFIX_REGEX.match(prefix):
        #    raise FastPubSubException(
        #        "Prefix must be a string that starts and ends with a letter or number, "
        #        "and can only contain periods, slashes, or underscores in the middle."
        #    )
        super().__init__(prefix=prefix)

        self.project_id = ""

        if routers:
            if not isinstance(routers, tuple):
                raise FastPubSubException("Your routers should be passed as a tuple")

            for router in routers:
                self.include_router(router)

        if middlewares:
            if not isinstance(middlewares, tuple):
                raise FastPubSubException("Your routers should be passed as a tuple")

            for middleware in middlewares:
                self.include_middleware(middleware)

    def _propagate_project_id(self, project_id: str) -> None:
        self.project_id = project_id

        for router in self.routers:
            router = cast(PubSubRouter, router)
            router.add_prefix(self.prefix)
            router._propagate_project_id(project_id)

        for publisher in self.publishers.values():
            publisher.set_project_id(self.project_id)

        for subscriber in self.subscribers.values():
            subscriber.set_project_id(self.project_id)

    def include_router(self, router: "PubSubRouter") -> None:
        if not (router and isinstance(router, PubSubRouter)):
            raise FastPubSubException(f"Your routers must be of type {PubSubRouter.__name__}")

        router.add_prefix(self.prefix)
        for existing_router in self.routers:
            if existing_router.prefix == router.prefix:
                raise FastPubSubException(
                    f"The prefix={router.prefix} is duplicated, it must be unique."
                )

        router._propagate_project_id(self.project_id)
        for middleware in self.middlewares:
            router.include_middleware(middleware)

        self.routers.append(router)
