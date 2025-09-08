



import asyncio
from typing import Any
from starconsumers.applications import StarConsumers
from starconsumers.broker import Broker
from starconsumers.logger import logger
from starconsumers.router import BrokerRouter


broker = Broker(project_id="jeitto-backend-local")

@broker.subscriber("mysub", topic_name="topic1", subscription_name="subscription")
def handler(message: Any):
    logger.info(f"Hi we received {message.data}")
    with open("abc.txt", "w") as fs:
        fs.write(message.data.decode())


router = BrokerRouter(prefix="finance")

@router.subscriber("mysub", topic_name="topic2", subscription_name="subscription2", autoupdate=True)
async def handler2(message: Any):
    logger.info(f"Hi we received router {message.data}")

broker.include_router(router)

app = StarConsumers(broker=broker)



@app.on_startup
def start():
    logger.info("This is a startup hook")

@app.on_shutdown
def shutdown():
    logger.info("This is a shutdown hook")

@app.after_startup
async def after_started():
    logger.info("This is a after startup hook")
    await broker.publish(topic_name="topic1", data={"some_message": "messageA"})

@app.after_shutdown
def after_shutdown():
    logger.info("This is a after shutdown hook")


if __name__ == "__main__":
    asyncio.run(app.run())