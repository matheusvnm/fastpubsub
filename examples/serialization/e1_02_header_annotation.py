"""Example: Using Header annotation to extract message attributes.

This example demonstrates how to use the Header() annotation to
extract values from message attributes (metadata).

Headers are useful for:
- Trace IDs for distributed tracing
- Content versioning
- Source identification
- Custom routing metadata
"""

from typing import Annotated

from fastpubsub import FastPubSub, Header, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "header-example",
    topic_name="traced-events",
    subscription_name="traced-events-sub",
)
async def handle_traced_event(
    payload: dict[str, str],
    event_type: str,
    trace_id: Annotated[str, Header("x-trace-id")],
    source: Annotated[str, Header("x-source", default="unknown")],
) -> None:
    """Handle event with tracing metadata.

    The trace_id and source come from message attributes,
    while event_type and payload come from the message body.
    """
    logger.info(f"[{trace_id}] Event from {source}: {event_type}")
    logger.info(f"Payload: {payload}")


@broker.subscriber(
    "header-example-2",
    topic_name="traced-events",
    subscription_name="traced-events-sub-2",
)
async def handle_traced_event_without_annotation(
    payload: dict[str, str],
    event_type: str,
    trace_id: str = Header("x-trace-id"),
    source: str = Header("x-source", default="unknown"),
) -> None:
    """Handle event with tracing metadata without annotations.

    The trace_id and source come from message attributes,
    while event_type and payload come from the message body.
    """
    logger.info(f"[{trace_id}] Event from {source}: {event_type}")
    logger.info(f"Payload: {payload}")


@app.after_startup
async def test_publish() -> None:
    await broker.publish(
        "traced-events",
        data={
            "event_type": "user.created",
            "payload": {"user_id": "123", "email": "user@example.com"},
        },
        attributes={
            "x-trace-id": "trace-abc-123-def-456",
            "x-source": "user-service",
        },
    )
