import json

import httpx

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.exceptions import Retry

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


# --8<-- [start:retry_handler]
@broker.subscriber(
    alias="order-handler",
    topic_name="orders",
    subscription_name="orders-subscription",
)
async def handle_order(message: Message):
    order_id = json.loads(message.data)["order_id"]
    try:
        async with httpx.AsyncClient() as client:
            await client.post(f"https://downstream.service/process/{order_id}")
    except httpx.TimeoutException as e:
        # Service is slow, retry later
        raise Retry("Downstream service timed out.") from e


# --8<-- [end:retry_handler]
