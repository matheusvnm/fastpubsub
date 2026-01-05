"""Example: Using Depends for dependency injection.

This example demonstrates how to use fast_depends Depends()
for injecting dependencies like database connections, services,
or other shared resources.

Dependencies can be:
- Simple factories that return values
- Async factories for async resources
- Nested (dependencies can have their own dependencies)
"""

from typing import Annotated

from fastpubsub import Depends, FastPubSub, Header, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


# Simulated services/dependencies
class DatabaseSession:
    """Simulated database session."""

    def __init__(self, trace_id: str):
        self.trace_id = trace_id

    async def save_event(self, event_type: str, data: dict[str, str]) -> str:
        """Save event to database."""
        logger.info(f"[{self.trace_id}] Saving {event_type} to database")
        return f"event-{id(data)}"


class NotificationService:
    """Simulated notification service."""

    def __init__(self, db: DatabaseSession):
        self.db = db

    async def notify(self, user_id: str, message: str) -> None:
        """Send notification to user."""
        logger.info(f"[{self.db.trace_id}] Notifying user {user_id}: {message}")


# Dependency factories
def get_trace_id(trace_id: Annotated[str, Header("x-trace-id", default="no-trace")]) -> str:
    """Get trace ID from header."""
    return trace_id


def get_database(trace_id: Annotated[str, Depends(get_trace_id)]) -> DatabaseSession:
    """Get database session with trace ID."""
    return DatabaseSession(trace_id)


def get_notification_service(
    db: Annotated[DatabaseSession, Depends(get_database)],
) -> NotificationService:
    """Get notification service with database dependency."""
    return NotificationService(db)


@broker.subscriber(
    "depends-example",
    topic_name="user-events",
    subscription_name="user-events-sub",
)
async def handle_user_event(
    # Injected dependencies
    db: Annotated[DatabaseSession, Depends(get_database)],
    notifications: Annotated[NotificationService, Depends(get_notification_service)],
    # Auto-unwrapped from message body
    user_id: str,
    event_type: str,
    data: dict[str, str],
) -> None:
    """Handle user event with injected dependencies.

    The database session and notification service are automatically
    injected, with the trace ID propagated through the dependency chain.
    """
    # Save to database
    event_id = await db.save_event(event_type, data)
    logger.info(f"Saved event {event_id}")

    # Send notification
    await notifications.notify(user_id, f"Event {event_type} processed")


@app.after_startup
async def test_publish() -> None:
    await broker.publish(
        "user-events",
        data={
            "user_id": "user-456",
            "event_type": "profile.updated",
            "data": {"field": "email", "new_value": "new@example.com"},
        },
        attributes={
            "x-trace-id": "trace-xyz-789",
        },
    )
