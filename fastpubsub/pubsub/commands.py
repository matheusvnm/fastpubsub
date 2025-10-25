"""Internal commands for handling and publishing messages."""

from typing import Any

from fastpubsub.clients.pubsub import PubSubClient
from fastpubsub.datastructures import Message
from fastpubsub.types import AsyncCallable


class HandleMessageCommand:
    """A command for handling incoming messages."""

    def __init__(self, *, target: AsyncCallable):
        """Initializes the HandleMessageCommand.

        Args:
            target: The target callable to handle the message.
        """
        self.target = target

    async def on_message(self, message: Message) -> Any:
        """Handles a message.

        Args:
            message: The message to handle.

        Returns:
            The result of the target callable.
        """
        # V2: Add message serialization via pydantic
        return await self.target(message)


class PublishMessageCommand:
    """A command for publishing messages."""

    def __init__(self, *, project_id: str, topic_name: str, autocreate: bool = True):
        """Initializes the PublishMessageCommand.

        Args:
            project_id: The Google Cloud project ID.
            topic_name: The name of the topic.
            autocreate: Whether to automatically create the topic.
        """
        self.project_id = project_id
        self.topic_name = topic_name
        self.autocreate = autocreate

    async def on_publish(
        self, data: bytes, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        """Publishes a message.

        Args:
            data: The message data.
            ordering_key: The ordering key for the message.
            attributes: A dictionary of message attributes.
        """
        client = PubSubClient(project_id=self.project_id)
        if self.autocreate:
            await client.create_topic(self.topic_name)

        await client.publish(
            topic_name=self.topic_name, data=data, ordering_key=ordering_key, attributes=attributes
        )
