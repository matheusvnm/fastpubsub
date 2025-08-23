from starconsumers.applications import StarConsumers
from starconsumers.consumers import TopicConsumer
from starconsumers.datastructures import MessageMiddleware, TopicMessage
from starconsumers.logger import logger

class LogGlobalMiddleware(MessageMiddleware):

    def __call__(self, *args, **kwargs):
        logger.info(f"I'm a Global Middleware!")
        return super().__call__(*args, **kwargs)

class LogLocalMiddleware(MessageMiddleware):

    def __call__(self, *args, **kwargs):
        logger.info(f"I'm a Local Middleware!")
        return super().__call__(*args, **kwargs)


consumer_a = TopicConsumer(project_id="starconsumers-pubsub-local", topic_name="topic")
@consumer_a.task(name="some_handler", subscription_name="some_subscription", dead_letter_topic="some_dlt")
async def some_handler(message: TopicMessage):
    logger.info(f"Some async message received for some_handler")


consumerb = TopicConsumer(project_id="starconsumers-pubsub-local", topic_name="topic2")
@consumerb.task(name="other_handler", subscription_name="some_subscription2")
def other_handler(message: TopicMessage):
    logger.info(f"Some sync message received for other_handler")



app = StarConsumers()

app.add_consumer(consumer_a)
app.add_consumer(consumerb)
app.add_middleware(LogGlobalMiddleware)

consumer_a.add_middleware(LogLocalMiddleware)
