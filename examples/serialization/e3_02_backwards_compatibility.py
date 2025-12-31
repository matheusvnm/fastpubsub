"""Example: Backwards compatibility with existing handlers.

This example demonstrates that existing handlers that use the
raw Message object continue to work unchanged.

FastPubSub automatically detects when a handler expects:
- A single parameter with no type hint -> raw Message
- A parameter typed as Message -> raw Message

This ensures smooth migration from existing code.
"""

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


# Style 1: Explicit Message type annotation (STILL WORKS)
@broker.subscriber(
    "explicit-message",
    topic_name="compat-topic",
    subscription_name="compat-sub-1",
)
async def handle_explicit_message(message: Message) -> None:
    """Handler with explicit Message type annotation.

    This is the traditional way of handling messages.
    The full Message object is passed with all metadata.
    """
    logger.info(f"[Explicit] Message ID: {message.id}")
    logger.info(f"[Explicit] Topic: {message.topic_name}")
    logger.info(f"[Explicit] Data: {message.data!r}")
    logger.info(f"[Explicit] Attributes: {message.attributes}")


# Style 2: Single untyped parameter (STILL WORKS)
@broker.subscriber(
    "untyped-param",
    topic_name="compat-topic",
    subscription_name="compat-sub-2",
)
async def handle_untyped(msg) -> None:  # type: ignore[no-untyped-def]  # noqa: ANN001
    """Handler with single untyped parameter.

    For backwards compatibility, a single untyped parameter
    receives the raw Message object.
    """
    logger.info(f"[Untyped] Received: {type(msg).__name__}")
    logger.info(f"[Untyped] Data: {msg.data!r}")  # noqa: S608


# Style 3: New DI-style handler (NEW FEATURE)
@broker.subscriber(
    "di-style",
    topic_name="compat-topic",
    subscription_name="compat-sub-3",
)
async def handle_di_style(user_id: str, action: str) -> None:
    """Handler with auto-unwrapping.

    Parameters are automatically extracted from the JSON message body.
    """
    logger.info(f"[DI-Style] User {user_id} performed {action}")


@app.after_startup
async def test_publish() -> None:
    # All three handlers will receive this message
    # Each processes it according to their signature
    await broker.publish(
        "compat-topic",
        {
            "user_id": "user-999",
            "action": "login",
            "timestamp": "2024-01-15T10:30:00Z",
        },
    )
