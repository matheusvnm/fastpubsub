from pydantic import BaseModel, Field

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger


# --8<-- [start:pydantic_model]
class Address(BaseModel):
    street: str = Field(..., examples=["5th Avenue"])
    number: str = Field(..., examples=["1548"])


# --8<-- [end:pydantic_model]


# --8<-- [start:broker_app]
broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)
# --8<-- [end:broker_app]


# --8<-- [start:rest_endpoint]
@app.post("/addresses/")
async def create_address(address: Address):
    logger.info(f"Address received: {address}")
    await broker.publish(topic_name="address-events", data=address)
    return {"message": "Address published"}


# --8<-- [end:rest_endpoint]


# --8<-- [start:subscriber]
@broker.subscriber(
    alias="address-handler",
    topic_name="address-events",
    subscription_name="address-events-subscription",
)
async def handle_message(message: Message):
    logger.info(f"The message {message.id} arrived.")
    address = Address.model_validate_json(message.data)
    logger.info(f"Address: {address}")
    return {"status": "ok"}


# --8<-- [end:subscriber]
