from typing import Any

from fastpubsub.clients.pubsub import PubSubClient
from fastpubsub.datastructures import Message
from fastpubsub.types import AsyncCallable


class HandleMessageCommand:
    def __init__(self, *, target: AsyncCallable):
        self.target = target

    async def on_message(self, message: Message) -> Any:
        # V2: Add message serialization via pydantic
        return await self.target(message)


class PublishMessageCommand:
    def __init__(self, *, project_id: str, topic_name: str, autocreate: bool = True):
        self.project_id = project_id
        self.topic_name = topic_name
        self.autocreate = autocreate

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        client = PubSubClient(project_id=self.project_id)
        if self.autocreate:
            await client.create_topic(self.topic_name)

        await client.publish(
            topic_name=self.topic_name, data=data, ordering_key=ordering_key, attributes=attributes
        )
