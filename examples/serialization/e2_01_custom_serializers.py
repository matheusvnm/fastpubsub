"""Example: Custom serializers.

This example demonstrates how to create and use custom serializers
for handling different message formats. A custom serializer uses
msgspec for fast JSON encoding/decoding as an alternative to the
default serializer.

A custom serializer must implement the Serializer protocol:
- encode(data) -> tuple[bytes, str]: Serialize data and return content-type
- decode(data: bytes) -> Any: Deserialize bytes back to Python objects
- supports(content_type: str) -> bool: Check if content-type is supported

Run with: fastpubsub run examples.serialization.e2_01_custom_serializers:app
"""

from typing import Any

import msgspec

from fastpubsub import FastPubSub, PubSubBroker
from fastpubsub.logger import logger
from fastpubsub.serialization import Serializer


class MsgSpecSerializer(Serializer):
    """A custom serializer using msgspec for fast JSON encoding."""

    CONTENT_TYPE = "application/json"

    def encode(self, data: Any) -> tuple[bytes, str]:
        """Encode data using msgspec."""
        return msgspec.json.encode(data), self.CONTENT_TYPE

    def decode(self, data: bytes) -> Any:
        """Decode data using msgspec."""
        return msgspec.json.decode(data)

    def supports(self, content_type: str) -> bool:
        """Check if content-type is supported."""
        return content_type.lower().startswith("application/json")


broker = PubSubBroker(
    project_id="fastpubsub-pubsub-local",
    serializer=MsgSpecSerializer(),
)
app = FastPubSub(broker)


@broker.subscriber(
    "custom-serializer-example",
    topic_name="custom-topic",
    subscription_name="custom-subscription",
)
async def handle_message(data: dict[str, Any]) -> None:
    """Handle message decoded with custom serializer."""
    logger.info(f"Received data: {data}")


@app.after_startup
async def test_publish() -> None:
    """Publish a test message using the custom serializer."""
    await broker.publish(
        "custom-topic",
        {"message": "Encoded with msgspec!", "value": 42},
    )
