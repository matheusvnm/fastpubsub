



from contextlib import asynccontextmanager

import uvicorn
from starconsumers.applications import FastConsumers
from starconsumers.broker import Broker
from starconsumers.datastructures import Message
from starconsumers.logger import logger

@asynccontextmanager
async def lifespan(app: FastConsumers):
    logger.info("Hi! I'm lifespan from FastConsumers RestAPI")
    yield
    logger.info("Hi! I'm lifespan from FastConsumers RestAPI finishing")

broker = Broker(project_id="starconsumers-pubsub-local")

@broker.subscriber("broker-subscriber", topic_name="topic_a", subscription_name="subscription_a",)
async def broker_handle(message: Message):
    logger.info(f"This handler has only the broker middleware")

app = FastConsumers(broker)

@app.after_startup
async def after_started():
    logger.info("The next published message will have one middleware")
    await broker.publish(topic_name="topic_a", data={"some_message": "messageA"})


app = FastConsumers(broker, lifespan=lifespan)


@app.get("/")
def home():
    return {"hello": "world"}


if __name__ == "__main__":
    uvicorn.run("examples.05_fastapi_integration:app", workers=2, log_level="warning")


        