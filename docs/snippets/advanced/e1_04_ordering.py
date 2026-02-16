from fastpubsub import FastPubSub, Message, PubSubBroker

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


async def update_user_state(user_id: str, data: bytes) -> None:
    """Update user state."""
    pass


async def process_event(data: bytes) -> None:
    """Process event."""
    pass


# --8<-- [start:ordered_subscriber]
@broker.subscriber(
    alias="user-events-ordered",
    topic_name="user-events",
    subscription_name="user-events-ordered-subscription",
    enable_message_ordering=True,
    autocreate=True,
)
async def process_user_events(message: Message):
    user_id = message.attributes.get("user_id", "unknown-user")
    await update_user_state(user_id, message.data)


# --8<-- [end:ordered_subscriber]


# --8<-- [start:ordered_publisher]
# Create an ordered publisher
ordered_publisher = broker.publisher("user-events")


@app.post("/user-action")
async def user_action():
    # Publish messages with ordering keys
    await ordered_publisher.publish(
        data={"action": "login", "user_id": "user-123"},
        ordering_key="user-123",
        attributes={"user_id": "user-123"},
    )

    await ordered_publisher.publish(
        data={"action": "update_profile", "user_id": "user-123"},
        ordering_key="user-123",  # Guaranteed to be processed after the login message
        attributes={"user_id": "user-123"},
    )


# --8<-- [end:ordered_publisher]


# --8<-- [start:ordered_with_dlt]
@broker.subscriber(
    alias="ordered-processor",
    topic_name="ordered-events",
    subscription_name="events-ordered-subscription",
    enable_message_ordering=True,
    dead_letter_topic="events-dlq",
    max_delivery_attempts=5,
    autocreate=True,
)
async def process_ordered(message: Message):
    await process_event(message.data)


# --8<-- [end:ordered_with_dlt]


# Simulated stores for use cases
class SessionStore:
    async def append_event(self, user_id: str, event: bytes) -> None:
        pass


class StateMachine:
    async def transition(self, order_id: str, transition: str) -> None:
        pass


class InventoryDB:
    async def update_quantity(self, sku: str, delta: int) -> None:
        pass


session_store = SessionStore()
state_machine = StateMachine()
inventory_db = InventoryDB()


# --8<-- [start:usecase_sessions]
@broker.subscriber(
    alias="session-tracker",
    topic_name="session-events",
    subscription_name="session-events-subscription",
    enable_message_ordering=True,
)
async def track_session(message: Message):
    user_id = message.attributes.get("user_id", "unknown-user")
    event = message.data

    # Events arrive in order: login → page_view → purchase → logout
    await session_store.append_event(user_id, event)


# --8<-- [end:usecase_sessions]


# --8<-- [start:usecase_state_machine]
@broker.subscriber(
    alias="order-state",
    topic_name="order-events",
    subscription_name="order-state-subscription",
    enable_message_ordering=True,
)
async def process_order_state(message: Message):
    import json

    order_id = message.attributes.get("order_id", "unknown-order")
    transition = json.loads(message.data)["transition"]

    # Transitions arrive in order: created → paid → shipped → delivered
    await state_machine.transition(order_id, transition)


# --8<-- [end:usecase_state_machine]


# --8<-- [start:usecase_inventory]
@broker.subscriber(
    alias="inventory-updater",
    topic_name="inventory-events",
    subscription_name="inventory-subscription",
    enable_message_ordering=True,
)
async def update_inventory(message: Message):
    import json

    sku = message.attributes.get("sku", "unknown-sku")
    delta = json.loads(message.data)["quantity_change"]

    # +10, -5, +3 applied in correct order
    await inventory_db.update_quantity(sku, delta)


# --8<-- [end:usecase_inventory]
