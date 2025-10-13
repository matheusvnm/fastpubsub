# Middlewares

**FastPubSub**'s middlewares are a powerful feature that allows you to add custom logic to your subscribers and publishers. They can be used to modify incoming messages, outgoing messages, or to add custom logic to the message handling process, allowing you to implement cross-cutting concerns such as logging, authentication, and error handling, without cluttering your business logic.

## Creating a middleware

To create a middleware, you need to create a class that inherits from `BaseMiddleware` and implements the `on_message` or `on_publish` methods.
Here's an example of a simple middleware that logs the incoming and outgoing messages:

```python
from fastpubsub import BaseMiddleware
from fastpubsub import Message

class LoggingMiddleware(BaseMiddleware):
    async def on_message(self, message: Message):
        print(f"Received message: {message}")
        await super().on_message(message)

    async def on_publish(self, data: bytes, ordering_key: str, attributes: dict[str, str] | None):
        print(f"Publishing message: {data}")
        await super().on_publish(data, ordering_key, attributes)
```

## Using a middleware

To use a middleware, you can pass it to the `PubSubBroker` or `PubSubRouter` constructor, to the `subscriber` decorator or when `publisher` instancianting a publisher .

Here's an example of how to use a middleware with the `PubSubBroker`:

```python
from fastpubsub import PubSubBroker
from my_app.middlewares import LoggingMiddleware

broker = PubSubBroker(
    project_id="your-project-id",
    middlewares=[LoggingMiddleware],
)
```

Here's an example of how to use a middleware with the `subscriber` decorator:

```python
from fastpubsub import PubSubBroker
from my_app.middlewares import LoggingMiddleware

broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

@broker.subscriber(
    alias="my-subscriber",
    topic_name="my-topic",
    subscription_name="my-subscription",
    middlewares=[LoggingMiddleware],
)
async def handle_message(message):
    print(message)

```



## Middleware Hierarchy

Middlewares are executed in the order they are defined.
The first middleware defined will be the first to be executed for incoming messages and the last to be executed for outgoing messages.

## Broker vs. Router vs. Subscriber/Publisher

Middlewares can be defined at three different levels:

- **Broker:** Middlewares defined at the broker level will be applied to all subscribers and publishers.
- **Router:** Middlewares defined at the router level will be applied to all subscribers and publishers in that router and its children.
- **Subscriber/Publisher:** Middlewares defined at the subscriber or publisher level will only be applied to that specific subscriber or publisher.

The order of execution is as follows:

1.  Broker middlewares
2.  Router middlewares
3.  Subscriber/Publisher middlewares

Here's an example of how to use middlewares at different levels:

```python
from fastpubsub import FastPubSub, PubSubBroker, PubSubRouter
from application.middlewares import (
    BrokerMiddleware,
    RouterMiddleware,
    SubscriberMiddleware,
)

router = PubSubRouter(middlewares=[RouterMiddleware])

@router.subscriber(
    alias="my-subscriber",
    topic_name="my-topic",
    subscription_name="my-subscription",
    middlewares=[SubscriberMiddleware],
)
async def handle_message(message):
    print(message)

broker = PubSubBroker(
    project_id="your-project-id",
    routers=[router],
    middlewares=[BrokerMiddleware],
)
app = FastPubSub(broker)

```

In this example, the order of execution for incoming messages will be:

1.  `BrokerMiddleware`
2.  `RouterMiddleware`
3.  `SubscriberMiddleware`
