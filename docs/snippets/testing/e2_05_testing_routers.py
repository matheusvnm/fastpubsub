import pytest

from fastpubsub import Message, PubSubBroker, PubSubRouter
from fastpubsub.testing import PubSubTestClient

# --8<-- [start:router_setup]
broker = PubSubBroker(project_id="test-project")
users_router = PubSubRouter(prefix="users")

processed_events: list[str] = []


@users_router.subscriber(
    alias="created",
    topic_name="user-created",
    subscription_name="user-created-subscription",
)
async def handle_user_created(message: Message):
    processed_events.append("created")


broker.include_router(users_router)
# --8<-- [end:router_setup]


# --8<-- [start:router_test]
@pytest.mark.asyncio
async def test_router_subscriber():
    processed_events.clear()

    async with PubSubTestClient(broker) as client:
        await client.publish(b"test", topic="user-created")

        assert "created" in processed_events


# --8<-- [end:router_test]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
