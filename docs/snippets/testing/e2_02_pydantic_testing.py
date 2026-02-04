import pytest
from pydantic import BaseModel

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.testing import PubSubTestClient


# --8<-- [start:pydantic_model]
class User(BaseModel):
    name: str
    email: str
    age: int


# --8<-- [end:pydantic_model]


# --8<-- [start:pydantic_setup]
broker = PubSubBroker(project_id="test-project")
app = FastPubSub(broker)

processed_users: list[User] = []


@broker.subscriber(
    alias="user-processor",
    topic_name="user-created",
    subscription_name="user-created-subscription",
)
async def process_new_user(message: Message):
    user = User.model_validate_json(message.data)
    processed_users.append(user)


# --8<-- [end:pydantic_setup]


# --8<-- [start:pydantic_test]
@pytest.mark.asyncio
async def test_process_new_user():
    processed_users.clear()

    async with PubSubTestClient(broker) as client:
        test_user = User(name="Alice", email="alice@example.com", age=30)

        # Publish using Pydantic model (auto-serialized)
        await client.publish(test_user, topic="user-created")

        # Verify the user was processed
        assert len(processed_users) == 1
        assert processed_users[0].name == "Alice"


# --8<-- [end:pydantic_test]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

