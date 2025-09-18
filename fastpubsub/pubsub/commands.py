from typing import Any

from fastpubsub.clients.pub import PubSubPublisherClient
from fastpubsub.datastructures import Message
from fastpubsub.types import AsyncCallable


class HandleMessageCommand:
    def __init__(self, *, target: AsyncCallable):
        self.target = target

    async def on_message(self, message: Message) -> Any:
        # TODO: Add the serialization logic for pydantic
        return await self.target(message)


class PublishMessageCommand:
    def __init__(self, *, project_id: str, topic_name: str):
        self.project_id = project_id
        self.topic_name = topic_name

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None, autocreate: bool
    ) -> Any:
        client = PubSubPublisherClient(project_id=self.project_id, topic_name=self.topic_name)
        if autocreate:
            client.create_topic()

        client.publish(data=data, ordering_key=ordering_key, attributes=attributes)
