# FastStream

<b>A high performance FastAPI-based message consumer framework for PubSub</b>


---

<div align="center">
[![Package version](https://img.shields.io/pypi/v/pubsub?label=PyPI)](https://pypi.org/project/fastpubsub)
[![Supported Python versions](https://img.shields.io/pypi/pyversions/pubsub.svg)](https://pypi.org/project/fastpubsub)\
[![License](https://img.shields.io/github/license/matheusvnm/fastpubsub.svg)](https://github.com/matheusvnm/fastpubsub/blob/main/LICENSE)
</div>


## Features

- **Easy to use:** **FastPubSub** is designed to be intuitive and easy to use, even for beginners.
- **Asynchronous:** **FastPubSub** is built on top of `anyio`, which allows it to run on top of either `asyncio` or `trio`.
- **Fast:** **FastPubSub** is fast. It's built on top of `uvloop` and `pydantic` for performance.
- **Typed:** **FastPubSub** is fully typed, which means you can get great editor support.
- **Lifecycle events:** **FastPubSub** supports `on_startup`, `on_shutdown`, `after_startup`, and `after_shutdown` events.
- **Middleware:** **FastPubSub** supports middlewares, which can be used to add custom logic to your subscribers and publishers.
- **Routers:** **FastPubSub** supports routers, which can be used to organize your subscribers and publishers.
- **CLI:** **FastPubSub** comes with a CLI that can be used to run your applications.
- **Observability:** **FastPubSub** has built-in support for observability, with integrations for `New Relic` and other on plan such as `OpenTelemetry` and `Datadog`.

## Installing

```bash
pip install fastpubsub
```

## Writing app code

**FastPubSub** brokers provide convenient function decorators (`@broker.subscriber`) and methods (`broker.publisher`) to allow you to delegate the actual process of:

- Creating Pub/Sub subscriptions to receive and process data from topics.
- Publishing data to other topics downstream in your message processing pipeline.

These decorators make it easy to specify the processing logic for your consumers and producers, allowing you to focus on the core business logic of your application without worrying about the underlying integration.

Here is an example Python app using **FastPubSub** that consumes data from an incoming data stream and outputs the data to another one:


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
   await broker.publish(topic_name="out", data="Hi!")

```

With **FastPubSub**, you can effortlessly run multiple subscribers at once, allowing you to process data from different topics in concurrently. You can also select which subscribers to run, giving you fine-grained control over your application.

Also, **Pydantic**’s [`BaseModel`](https://docs.pydantic.dev/usage/models/) class allows you to define messages using a declarative syntax for sending messages downstream, making it easy to specify the fields and types of your messages.


```python
from pydantic import BaseModel, Field
from fastpubsub import FastPubSub, PubSubBroker, Message

class Address(BaseModel):
    street: str = Field(..., examples=["5th Avenue"])
    number: str = Field(..., examples=["1548"])


broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)

@broker.subscriber(
    alias="my-subscriber",
    topic_name="my-topic",
    subscription_name="my-subscription",
)
async def handle_message(message: Message):
   address = Address(street="Av. Flores", number="213")
   await broker.publish(topic_name="out", data=address)

```

## Running the application

The application can be started using built-in **FastPubSub** CLI command.
It is embedded in the library and its a core part of the system.

To run the service, use the **FastPubSub CLI** command and pass the module (in this case, the file where the app implementation is located) and the app symbol to the command.



```bash
fastpubsub run basic:app
```

After running the command, you should see the following output:


``` shell
2025-10-11 19:32:13,245 | INFO     | 95921:124619031033664 | applications:_start_hooks:134 | Starting FastPubSub processes
2025-10-11 19:32:13,475 | INFO     | 95921:124619031033664 | applications:_start_hooks:143 | The FastPubSub processes started
```

Also, **FastPubSub** provides you with a great hot reload feature to improve your Development Experience

``` shell
fastpubsub run basic:app --reload
```

And multiprocessing horizontal scaling feature as well:

``` shell
fastpubsub run basic:app --workers 3
```

You can learn more about **CLI** features [here](docs/cli.md)


## Advanced Features

**FastPubSub** is packed with advanced features that make it easy to build robust and scalable applications. Here are some of the features you can learn more about in our documentation:

- [Basic Usage](./docs/basic_usage.md)
- [Routers](./docs/routers.md)
- [Middlewares](./docs/middlewares.md)
- [Observability](./docs/observability.md)
- [FastAPI Integration](./docs/fastapi_integration.md)
- [Concurrency Model](./docs/concurrency_model.md)

## How to contact us
Please stay in touch by:
- Sending a email at sandro-matheus@hotmail.com
- Sending a message on my [linkedin](https://www.linkedin.com/in/matheusvnm/).
