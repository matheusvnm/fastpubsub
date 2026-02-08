from typing import Any

from fastpubsub import BaseMiddleware, Message
from fastpubsub.logger import logger


# --8<-- [start:broker_middleware]
class BrokerMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        logger.info(
            "I'm the broker subscriber middleware! "
            "I will only be executed at broker handlers and its children"
        )
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        logger.info(
            "I'm the broker publish middleware! "
            "I will only be executed at broker publish and its children"
        )
        return await super().on_publish(data, ordering_key, attributes)


# --8<-- [end:broker_middleware]


# --8<-- [start:router_middleware]
class RouterMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        logger.info(
            "I'm the router subscriber middleware! "
            "I will only be executed at the router handlers and its children"
        )
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        logger.info(
            "I'm the router publish middleware! "
            "I will only be executed at the router publish and its children"
        )
        return await super().on_publish(data, ordering_key, attributes)


# --8<-- [end:router_middleware]


# --8<-- [start:subrouter_middleware]
class SubRouterMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        logger.info(
            "I'm the sub-router subscriber middleware! "
            "I will only be executed at the sub-child router handlers"
        )
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        logger.info(
            "I'm the sub-router publish middleware! "
            "I will only be executed at the sub-child router publishers"
        )
        return await super().on_publish(data, ordering_key, attributes)


# --8<-- [end:subrouter_middleware]


# --8<-- [start:subscriber_middleware]
class SubcriberMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        logger.info("I'm the subscriber middleware! I will only be executed at subscriber level")
        return await super().on_message(message)

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        pass


# --8<-- [end:subscriber_middleware]


# --8<-- [start:publisher_middleware]
class PublisherMiddleware(BaseMiddleware):
    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        logger.info("I'm the publisher middleware! I will only be executed at publisher level")
        return await super().on_publish(data, ordering_key, attributes)

    async def on_message(self, message: Message) -> Any:
        pass


# --8<-- [end:publisher_middleware]
