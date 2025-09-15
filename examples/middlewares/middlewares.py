from typing import Any
from fastpubsub.logger import logger
from fastpubsub.datastructures import Message
from fastpubsub.middlewares import BasePublisherMiddleware, BaseSubscriberMiddleware


class BrokerSubscriberMiddleware(BaseSubscriberMiddleware):

    async def __call__(self, message: Message):
        logger.info(f"I'm the broker subscriber middleware! I will only be executed at broker handlers")
        return await super().__call__(message)

class BrokerPublisherMiddleware(BasePublisherMiddleware):

    async def __call__(self, data: dict[str, Any], attributes: dict[str, str], ordering_key: str, autocreate: bool):
        logger.info(f"I'm the broker publisher middleware! I will only be executed at broker publishers")
        return await super().__call__(data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate)

class RouterSubscriberMiddleware(BaseSubscriberMiddleware):

    async def __call__(self, message: Message):
        logger.info(f"I'm the router subscriber middleware! I will only be executed at the router handlers and its children")
        return await super().__call__(message)

class RouterPublisherMiddleware(BasePublisherMiddleware):

    async def __call__(self, data: dict[str, Any], attributes: dict[str, str], ordering_key: str, autocreate: bool):
        logger.info(f"I'm the router publisher middleware! I will only be executed at the router publishers and its children")
        return await super().__call__(data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate)


class SubRouterSubscriberMiddleware(BaseSubscriberMiddleware):

    async def __call__(self, message: Message):
        logger.info(f"I'm the sub-router subscriber middleware! I will only be executed at the sub-child router handlers")
        return await super().__call__(message)


class SubRouterPublisherMiddleware(BasePublisherMiddleware):

    async def __call__(self, data: dict[str, Any], attributes: dict[str, str], ordering_key: str, autocreate: bool):
        logger.info(f"I'm the sub-router publisher middleware! I will only be executed at the sub-child router publishers")
        return await super().__call__(data=data, ordering_key=ordering_key, attributes=attributes, autocreate=autocreate)


class SubscriberLevelSubscriberMiddleware(BaseSubscriberMiddleware):

    async def __call__(self, message: Message):
        logger.info(f"This is a subscriber-level subscriber middleware with message: {message.data}")
        return await super().__call__(message)