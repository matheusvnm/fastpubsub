"""Title: Middleware with External Dependencies

Demonstrates middleware with lifecycle hooks for resource management.
"""

from typing import Any

import httpx

from fastpubsub import BaseMiddleware, Message


# --8<-- [start:webhook_middleware]
class WebhookMiddleware(BaseMiddleware):
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.client: httpx.AsyncClient | None = None

    async def on_startup(self):
        """Called when the application starts."""
        self.client = httpx.AsyncClient()

    async def on_shutdown(self):
        """Called when the application stops."""
        if self.client:
            await self.client.aclose()

    async def on_message(self, message: Message) -> Any:
        result = await super().on_message(message)

        await self.client.post(
            self.webhook_url, json={"message_id": message.id, "status": "processed"}
        )

        return result
# --8<-- [end:webhook_middleware]
