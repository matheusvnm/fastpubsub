
from starconsumers import observability
from starconsumers.application import StarConsumers
from starconsumers.consumer import TopicConsumer
from starconsumers.datastructures import MessageMiddleware, TopicMessage


class LogSomethingMiddleware(MessageMiddleware):

    def __call__(self, *args, **kwargs):
        print(f"I'm a MIDDLEWARE!")
        return super().__call__(*args, **kwargs)


app = StarConsumers()
consumer = TopicConsumer(project_id="starconsumers-pubsub-local", topic_name="topic")


@consumer.task(name="some_handler", subscription_name="some_subscription")
async def some_handler(message: TopicMessage):
    apm = observability.get_apm_provider()
    print(f"Some async message received for some_handler {apm.get_trace_id()}")



@consumer.task(name="other_handler", subscription_name="some_subscription2")
def other_handler(message: TopicMessage):
    apm = observability.get_apm_provider()
    print(f"Some async message received for other_handler {apm.get_trace_id()}")


app.add_consumer(consumer)
app.add_middleware(LogSomethingMiddleware)