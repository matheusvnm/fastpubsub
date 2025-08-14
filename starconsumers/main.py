
from starconsumers.application import StarConsumers
from starconsumers.consumer import TopicConsumer


app = StarConsumers()
consumer = TopicConsumer(project_id="abc", topic_name="topic")


@consumer.task(name="some_handler", subscription_name="some_subscription")
def some_handler():
    print("ola")


@consumer.task(name="some_other_handler", subscription_name="some_subscription2")
def some_other_handler():
    print("ola")


app.add_consumer(consumer)
app.activate_tasks(["some_other_handler"])


print(app.active_tasks)
print(consumer.task_map)
