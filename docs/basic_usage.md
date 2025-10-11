# Basic Usage

**FastPubSub** is designed to be intuitive and easy to use. The core of the framework is the `PubSubBroker`, which is used to create subscribers and publishers. The `FastPubSub` class is a `FastAPI` application that integrates with the broker, providing a simple and elegant way to build message-driven applications.

## Subscriptions Basics

To create a subscriber, you can use the `@broker.subscriber` decorator. The decorator takes the following arguments:

- `alias`: A unique name for the subscriber, used by the CLI for subscription selection.
- `topic_name`: The name of the topic to subscribe to.
- `subscription_name`: The name of the subscription.

Here's an example of a simple subscriber:

```python
from fastpubsub import FastPubSub, PubSubBroker, Message

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

@broker.subscriber(
    alias="my-subscriber",
    topic_name="my-topic",
    subscription_name="my-subscription",
)
async def handle_message(message: Message):
    print(message)
```

## Publishers Basics

**FastPubSub** provides two ways to publish messages:

1.  **Via the broker or router:** You can use the `broker.publish` or `router.publish` method to publish messages to a specific topic. This is the most straightforward way to publish messages.
2.  **Via a publisher instance:** You can create a `Publisher` instance for a specific topic and use it to publish messages. This is useful if you need to publish messages from different places in your application.

Here's an example of how to publish a message using the `broker.publish` and instancianting a `Publisher` method:

```python
from fastpubsub import PubSubBroker

broker = PubSubBroker(project_id="your-project-id")

async def main():
    await broker.publish("my-topic", "Hello, world!")
    publisher = broker.publisher("my-topic")
    await publisher.publish("foobar")
```


## The FastPubSub CLI

The application can be started using the built-in **FastPubSub** CLI command. It is embedded in the library and is a core part of the system, providing a simple and efficient way to run your applications.

To run the service, use the **FastPubSub CLI** command and pass the module (in this case, the file where the app implementation is located) and the app symbol to the command.

```bash
fastpubsub run basic:app
```

## Multiple Publishers/Subscriptions

You can define multiple subscribers and publishers in the same application. **FastPubSub** will automatically run all of them concurrently.

```python
from fastpubsub import FastPubSub, PubSubBroker, Message

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

@broker.subscriber(
    alias="subscriber-1",
    topic_name="topic-1",
    subscription_name="subscription-1",
)
async def handle_message_1(message: Message):
    print(f"Received message from topic-1: {message}")

@broker.subscriber(
    alias="subscriber-2",
    topic_name="topic-2",
    subscription_name="subscription-2",
)
async def handle_message_2(message: Message):
    print(f"Received message from topic-2: {message}")

@app.after_startup
async def test_publish() -> None:
    await broker.publish("topic-1", "Hello from topic-1!")
    await broker.publish("topic-2", "Hello from topic-2!")
```

## Acknowledging Messages

**FastPubSub** considers that a messages that are completed without exceptions must be acknowledged as a succesful execution.
However, it also provides two exceptions that can be used to control the message acknowledgment process:

- `Drop`: If you raise this exception in a subscriber, the message will be acknowledged and will not be redelivered.
- `Retry`: If you raise this exception in a subscriber, the message will be nacknowledged and will be redelivered.

Any other exception raises by the message handler will result in the message being nacked to retried later.

Here's an example of how to use the `Drop` and `Retry` exceptions:

```python
from fastpubsub import FastPubSub, PubSubBroker, Message
from fastpubsub.exceptions import Drop, Retry

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

@broker.subscriber(
    alias="my-subscriber",
    topic_name="my-topic",
    subscription_name="my-subscription",
)
async def handle_message(message: Message):
    if "error" in message.data.decode():
        # This will trigger a nack and the message will be redelivered.
        raise Retry()
    elif "ignore" in message.data.decode():
        # This will trigger a ack and the message will be droped.
        raise Drop()
    elif "exception" in message.data.decode():
        # This will trigger a nack and the message will be delivered
        raise ValueError()
    else:
        print(message)
        # This will result in a ack as it was successfully processed
```




## The FastPubSub Hooks

**FastPubSub** supports `on_startup`, `on_shutdown`, `after_startup`, and `after_shutdown` events. These can be used to add custom logic to the application lifecycle.

## Usage

Here's an example of how to use the lifecycle events:

```python
from fastpubsub import FastPubSub, PubSubBroker

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

@app.on_startup
async def startup():
    print("Application is starting up")

@app.on_shutdown
async def shutdown():
    print("Application is shutting down")

@app.after_startup
async def after_startup():
    print("Application has started up")

@app.after_shutdown
async def after_shutdown():
    print("Application has shut down")
```

The order of execution is as follows:

1.  `on_startup`
2.  `after_startup`
3.  `on_shutdown`
4.  `after_shutdown`




# The Concurrency Models

**FastPubSub** is built on top of `anyio`, which allows it to run on top of either `asyncio` or `trio`.
This means that you can use `async` and `await` to write your subscribers and publishers.

All subscriber handlers must be `async` functions. If you try to use a synchronous function as a handler, **FastPubSub** will raise a `TypeError`.
We choose this design principle due to allow maximum concurrency without needing to create subprocesses/threads to improve the scalability.
All the principal functions and utilities of **FastPubSub** are based on async function.

### How to Scale Consumers

So if the consumers must follow async/await pattern, how can we scale our application? It's simple!
**FastPubSub** allows you to scale application right from the command line by running you application in multiple instances. Just set the --workers option to scale your application:


```python
import os
from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)


@broker.subscriber(
    "test-alias",
    topic_name="test-topic",
    subscription_name="test-basic-subscription",
)
async def handle(message: Message) -> None:
    logger.info(f"We received a message")


@app.after_startup
async def test_publish() -> None:
    await broker.publish("test-topic", {"hello": "world"})
```


```shell
fastpubsub run main:app --workers 2
```


```shell
2025-10-11 19:52:43,475 | INFO     | 103788:138628326336320 | runner:run:55 | Starting FastPubSub processes
2025-10-11 19:52:43,969 | INFO     | 103791:132853977544512 | pubsub:publish:305 | Message published for topic projects/fastpubsub-pubsub-local/topics/test-topic with id 11
2025-10-11 19:52:43,975 | INFO     | 103790:140537774901056 | pubsub:publish:305 | Message published for topic projects/fastpubsub-pubsub-local/topics/test-topic with id 12
2025-10-11 19:52:44,006 | INFO     | 103791:132853977544512 | e1_01_basic_subscriber:handle:14 | We received a message | name=handle message_id=11 topic_name=test-topic
2025-10-11 19:52:44,007 | INFO     | 103791:132853977544512 | e1_01_basic_subscriber:handle:14 | We received a message | name=handle message_id=12 topic_name=test-topic
2025-10-11 19:52:44,014 | INFO     | 103791:132853977544512 | tasks:_consume:130 | Message successfully processed. | name=handle message_id=12 topic_name=test-topic
2025-10-11 19:52:44,014 | INFO     | 103791:132853977544512 | tasks:_consume:130 | Message successfully processed. | name=handle message_id=12 topic_name=test-topic
2025-10-11 19:52:53,538 | INFO     | 103788:138628326336320 | runner:run:65 | Terminating FastPubSub processes
```
