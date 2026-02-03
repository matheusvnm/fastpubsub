"""Title: Error Handling in Middlewares

Demonstrates proper error handling and transformation in middlewares.
"""

import logging
from typing import Any

import httpx

from fastpubsub import BaseMiddleware, Message
from fastpubsub.exceptions import Drop, Retry

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    pass


class TemporaryError(Exception):
    pass


# --8<-- [start:error_handling_middleware]
class ErrorHandlingMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        try:
            return await super().on_message(message)

        except ValidationError as e:
            logger.warning(f"Dropping invalid message: {e}")
            raise Drop(f"Validation failed: {e}")

        except TemporaryError as e:
            logger.info(f"Retrying message due to: {e}")
            raise Retry(f"Temporary failure: {e}")

        except Exception as e:
            logger.exception(f"Unexpected error processing message {message.id}")
            raise
# --8<-- [end:error_handling_middleware]


# --8<-- [start:external_service_middleware]
class ExternalServiceMiddleware(BaseMiddleware):
    async def on_message(self, message: Message) -> Any:
        try:
            return await super().on_message(message)

        except httpx.ConnectError:
            raise Retry("External service unreachable")

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                raise Retry("Rate limited by external service")
            elif e.response.status_code >= 500:
                raise Retry(f"External service error: {e.response.status_code}")
            else:
                raise Drop(f"Client error: {e.response.status_code}")
# --8<-- [end:external_service_middleware]
