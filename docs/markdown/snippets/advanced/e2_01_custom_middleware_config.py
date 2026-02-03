"""Title: Custom Middleware with Configuration

Demonstrates middleware with constructor parameters.
"""

import asyncio
import time
from typing import Any

from fastpubsub import BaseMiddleware, Message, Middleware, PubSubBroker


# --8<-- [start:rate_limit_middleware]
class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, requests_per_second: int = 100):
        self.requests_per_second = requests_per_second
        self.tokens = requests_per_second
        self.last_update = time.monotonic()

    async def on_message(self, message: Message) -> Any:
        await self._acquire_token()
        return await super().on_message(message)

    async def _acquire_token(self):
        now = time.monotonic()
        elapsed = now - self.last_update
        self.tokens = min(
            self.requests_per_second, self.tokens + elapsed * self.requests_per_second
        )
        self.last_update = now

        if self.tokens < 1:
            await asyncio.sleep(1 / self.requests_per_second)
            self.tokens = 1

        self.tokens -= 1
# --8<-- [end:rate_limit_middleware]


# --8<-- [start:apply_configured_middleware]
broker = PubSubBroker(
    project_id="your-project-id",
    middlewares=[
        Middleware(RateLimitMiddleware, requests_per_second=50),
    ],
)
# --8<-- [end:apply_configured_middleware]
