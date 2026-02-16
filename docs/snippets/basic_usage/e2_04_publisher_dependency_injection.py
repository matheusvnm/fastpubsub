from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from fastpubsub import FastPubSub, Message, Publisher, PubSubBroker
from fastpubsub.logger import logger


# --8<-- [start:publisher_di_usecase]
@dataclass
class MyAwesomeUseCase:
    publisher: Publisher

    async def execute(self, data: dict) -> Any:
        # Business logic here...
        # Then publish the event
        return await self.publisher.publish(data=data)


# --8<-- [end:publisher_di_usecase]
# --8<-- [start:publisher_di_model]
class User(BaseModel):
    name: str
    age: int


# --8<-- [end:publisher_di_model]

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)

# --8<-- [start:publisher_di_setup]
# Create a dedicated publisher for user events
user_publisher = broker.publisher("new-users-topic")
# --8<-- [end:publisher_di_setup]


@broker.subscriber(
    "user-events-handler",
    topic_name="new-users-topic",
    subscription_name="new-users-subscription",
)
async def handle_user_event(message: Message) -> None:
    logger.info(f"Received user event: {message.data.decode()}")


# --8<-- [start:publisher_di_endpoint]
@app.post("/new-user")
async def receive_new_user(user: User) -> dict[str, str]:
    logger.info(f"Received a new user: {user.name}")

    # Inject the dedicated publisher into the use case
    # Easy to mock in tests
    use_case = MyAwesomeUseCase(publisher=user_publisher)
    await use_case.execute(user.model_dump())

    return {"message": "Use case executed successfully"}


# --8<-- [end:publisher_di_endpoint]
