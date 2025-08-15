
from starconsumers.application import StarConsumers
from starconsumers.consumer import TopicConsumer
from starconsumers.observability import apm


app = StarConsumers()
consumer = TopicConsumer(project_id="starconsumers-pubsub-local", topic_name="topic")


@consumer.task(name="some_handler", subscription_name="some_subscription")
def some_handler(message):
    print(f"ola2 {message} {apm.get_trace_id()}")


@consumer.task(name="some_other_handler", subscription_name="some_subscription2")
def some_other_handler(message):
    print(f"ola2 {message} {apm.get_trace_id()}")


app.add_consumer(consumer)