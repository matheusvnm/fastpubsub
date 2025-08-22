from starconsumers.applications import StarConsumers
from starconsumers.consumers import TopicConsumer
from starconsumers.datastructures import MessageMiddleware, TopicMessage
from starconsumers.logger import logger

class LogSomethingMiddleware(MessageMiddleware):

    def __call__(self, *args, **kwargs):
        logger.info(f"I'm a MIDDLEWARE!")
        return super().__call__(*args, **kwargs)


app = StarConsumers()
consumer = TopicConsumer(project_id="starconsumers-pubsub-local", topic_name="topic")


@consumer.task(name="some_handler", subscription_name="some_subscription")
async def some_handler(message: TopicMessage):
    logger.info(f"Some async message received for some_handler")



@consumer.task(name="other_handler", subscription_name="some_subscription2")
def other_handler(message: TopicMessage):
    logger.info(f"Some sync message received for other_handler")


app.add_consumer(consumer)
app.add_middleware(LogSomethingMiddleware)