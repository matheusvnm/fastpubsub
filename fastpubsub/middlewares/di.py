"""Internal middlewares for handling and publishing messages."""

import inspect
from typing import Any, ClassVar

from fastpubsub.concurrency.utils import apply_async
from fastpubsub.datastructures import Message
from fastpubsub.middlewares.base import BaseMiddleware
from fastpubsub.transport import NotFound, PubSubClient
from fastpubsub.types import MessageHandler


class HandleMessageSerializerMiddleware(BaseMiddleware):
    """A special middleware for handling incoming messages serialization."""

    def __init__(
        self, next_call: BaseMiddleware | None, target: MessageHandler
    ):
        """Initializes the HandleMessageSerializerMiddleware.

        Args:
            next_call: The next call to the middleware chain.
            target: The target callable to handle the message. An async
                function runs on the event loop. A sync function runs in
                the thread pool.
        """
        super().__init__(next_call)
        self.target = target
        self.is_async_target = inspect.iscoroutinefunction(target)

    async def on_message(self, message: Message) -> Any:
        """Handles a message.

        Args:
            message: The message to handle.

        Returns:
            The result of the target callable.
        """
        if self.is_async_target:
            return await self.target(message)

        return await apply_async(self.target, message)


class PublishMessageSerializerMiddleware(BaseMiddleware):
    """A special middleware for publishing messages serialization."""

    _created_topics: ClassVar[set[tuple[str, str]]] = set()

    def __init__(
        self,
        next_call: BaseMiddleware | None,
        project_id: str,
        topic_name: str,
        autocreate: bool = True,
    ):
        """Initializes the PublishMessageSerializerMiddleware.

        Args:
            next_call: The next call to the middleware chain.
            project_id: The Google Cloud project ID.
            topic_name: The name of the topic.
            autocreate: Whether to automatically create the topic.
        """
        super().__init__(next_call)
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
        topic_key = (self.project_id, self.topic_name)
        if self.autocreate and topic_key not in self._created_topics:
            await client.create_topic(self.topic_name)
            self._created_topics.add(topic_key)

        try:
            return await client.publish(
                topic_name=self.topic_name,
                data=data,
                ordering_key=ordering_key,
                attributes=attributes,
            )
        except NotFound:
            self._created_topics.discard(topic_key)
            raise
