import re
from collections.abc import Sequence
from typing import cast

from fastpubsub.baserouter import BaseRouter
from fastpubsub.exceptions import FastPubSubException
from fastpubsub.middlewares.base import BaseMiddleware

_PREFIX_REGEX = re.compile(r"^[a-zA-Z0-9]+([_./][a-zA-Z0-9]+)*$")


class PubSubRouter(BaseRouter):
    def __init__(
        self,
        prefix: str = "",
        *,
        routers: Sequence["PubSubRouter"] | None = None,
        middlewares: Sequence[type[BaseMiddleware]] | None = None,
    ):
        if prefix and not _PREFIX_REGEX.match(prefix):
            raise FastPubSubException(
                "Prefix must be a string that starts and ends with a letter or number, "
                "and can only contain periods, slashes, or underscores in the middle."
            )
        super().__init__(prefix=prefix)

        self.project_id = ""

        if routers:
            if not isinstance(routers, Sequence):
                raise FastPubSubException("Your routers should be passed as a sequence")

            for router in routers:
                self.include_router(router)

        if middlewares:
            if not isinstance(middlewares, Sequence):
                raise FastPubSubException("Your routers should be passed as a sequence")

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

        if self == router:
            # V2: Create a algorithm to detect cycles on these routers.
            # For now, let us assume that the router is well configured
            # and this is the only error case.
            raise FastPubSubException(f"There is a cyclical reference on router {self.prefix}.")

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
