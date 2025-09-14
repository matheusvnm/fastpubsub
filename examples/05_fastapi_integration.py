



from contextlib import asynccontextmanager

import uvicorn
from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import Message
from fastpubsub.logger import logger

@asynccontextmanager
async def lifespan(app: FastPubSub):
    logger.info("Hi! I'm lifespan from FastConsumers RestAPI")
    yield
    logger.info("Hi! I'm lifespan from FastConsumers RestAPI finishing")

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")

@broker.subscriber("broker-subscriber", topic_name="topic_a", subscription_name="subscription_a",)
async def broker_handle(message: Message):
    logger.info(f"This handler has only the broker middleware")

app = FastPubSub(broker, lifespan=lifespan)

@app.after_startup
async def after_started():
    logger.info("The next published message will have one middleware")
    await broker.publish(topic_name="topic_a", data={"some_message": "messageA"})


@app.get("/")
async def home():
    await broker.publisher(topic_name="topic_a")
    return {"hello": "world"}
