import base64
from fastpubsub import FastPubSub, PubSubBroker, PushMessage
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


# --8<-- [start:push_subscriber]
@app.post("/push-handler")
async def handle_push_message(data: PushMessage):
    logger.info(f"Received push message: {data.message}")
    # Returning 2xx acknowledges the message
    return {"status": "ok",
            "processed_data": data.message.data,
            "processed_attributes": data.message.attributes,}


# --8<-- [end:push_subscriber]
