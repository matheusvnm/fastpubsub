"""Internal commands for handling and publishing messages."""

from typing import Any

from fastpubsub.clients.pubsub import PubSubClient
from fastpubsub.datastructures import PullMessage
from fastpubsub.di.handler import Handler
from fastpubsub.di.repository import context_repo
from fastpubsub.exceptions import Drop
from fastpubsub.middlewares import BaseMiddleware
from fastpubsub.serialization import EncodingError, Serializer


class _ConsumeMessageDecoderMiddleware(BaseMiddleware):
    """A middleware for handling incoming messages with serialization support."""

    def __init__(self, *, handler: Handler, serializer: Serializer):
        """Initializes the _ConsumeMessageDecoderMiddleware.

        Args:
            handler: The handler wrapping the target callable with DI.
            serializer: The serializer for decoding messages.
        """
        super().__init__(None)
        self.handler = handler
        self.serializer = serializer

    async def on_message(self, message: PullMessage) -> Any:
        """Handles a message with deserialization and DI.

        Args:
            message: The message to handle.

        Returns:
            The result of the target callable.
        """
        await super().on_message(message=message)

        call_kwargs: dict[str, Any] = {}
        content_type = message.attributes.get(self.serializer.CONTENT_TYPE_KEY, "")
        if not self.serializer.supports(content_type):
            raise Drop(
                f"We can not decode message {message.id} "
                f"it has unsupported format {content_type} "
                f"for serializer {self.serializer.__class__.__name__}."
            )

        decoded_data = self.serializer.decode(message.data)

        with context_repo.scope("message", message):
            context_repo.set_local("message.data", message.data)
            context_repo.set_local("message.attributes", message.attributes)
            context_repo.set_local("message.decoded_data", decoded_data)

            if len(self.handler.unannotated_param_names) == 1:
                param_name = next(iter(self.handler.unannotated_param_names))
                call_kwargs[param_name] = decoded_data
            elif isinstance(decoded_data, dict):
                for key in self.handler.unannotated_param_names:
                    if key in decoded_data:
                        call_kwargs[key] = decoded_data[key]

            return await self.handler.target(**call_kwargs)


class _PublishMessageEncoderMiddleware(BaseMiddleware):
    """A middleware for publishing messages with serialization support."""

    def __init__(
        self,
        *,
        project_id: str,
        topic_name: str,
        serializer: Serializer,
        autocreate: bool = True,
    ):
        """Initializes the _PublishMessageEncoderMiddleware.

        Args:
            project_id: The Google Cloud project ID.
            topic_name: The name of the topic.
            autocreate: Whether to automatically create the topic.
            serializer: The serializer for encoding messages.
        """
        super().__init__(None)

        self.project_id = project_id
        self.topic_name = topic_name
        self.serializer = serializer
        self.autocreate = autocreate

    async def on_publish(
        self, data: Any, ordering_key: str, attributes: dict[str, str] | None
    ) -> Any:
        """Publishes a message with automatic encoding.

        Args:
            data: The message data (any serializable type).
            ordering_key: The ordering key for the message.
            attributes: A dictionary of message attributes.
        """
        await super().on_publish(data=data, ordering_key=ordering_key, attributes=attributes)

        attrs = dict(attributes) if attributes is not None else {}

        content_type = attrs.get(self.serializer.CONTENT_TYPE_KEY, "")
        if content_type and not self.serializer.supports(content_type):
            raise EncodingError(
                f"We can not encode message type {type(data)} "
                f"it has unsupported format {content_type} "
                f"for serializer {self.serializer.__class__.__name__}."
            )

        # If not set a content type, we will let the encoder try to deal with the guessing.
        encoded_data, actual_content_type = self.serializer.encode(data)

        if self.serializer.CONTENT_TYPE_KEY not in attrs:
            attrs[self.serializer.CONTENT_TYPE_KEY] = actual_content_type

        client = PubSubClient(project_id=self.project_id)
        if self.autocreate:
            await client.create_topic(self.topic_name)

        await client.publish(
            topic_name=self.topic_name,
            data=encoded_data,
            ordering_key=ordering_key,
            attributes=attrs,
        )
