# FastPubSub

*A high performance FastAPI-based message consumer framework for PubSub.*

[//]: # (Aqui devem ir algumas tags)


## Features


FastPubSub ia modern, high-performance framework for building modern applications that process event messages on Google PubSub. It combines the standard PubSub Python SDK with FastAPI, Pydantic and Uvicorn to provide a easy-to-use development experience.

The key features are:

- **Fast:** FastPubSub is (unironically) fast. It's built on top of [**FastAPI**](https://fastapi.tiangolo.com/), [**uvicorn**](https://uvicorn.dev/) and [**Google PubSub Python SDK**](https://github.com/googleapis/python-pubsub) for maximum performance.
- **Intuitive**: It is designed to be intuitive and easy to use, even for beginners.
- **Typed**: Providing a great editor support and less time reading docs.
- **Robust**: Get production-ready code with sensible default values  avoiding shooting yourself in the foot.
- **Asynchronous:** It is built on top of anyio, which allows it to run on top of either asyncio or trio.
- **Batteries Included**: Providing its own CLI and other widely used tools such as [**pydantic**](https://docs.pydantic.dev/) for data validation, observability integrations and log contextualization.



## Quick Start

### Installation

FastPubSub works on Linux, macOS, Windows and most Unix-style operating systems. You can install it with pip as usual:

```shell
pip install fastpubsub
```

### Writing your first application

**FastPubSub** brokers provide convenient function decorators (`@broker.subscriber`) and methods (`broker.publisher`) to allow you to delegate the actual process of:

- Creating Pub/Sub subscriptions to receive and process data from topics.
- Publishing data to other topics downstream in your message processing pipeline.

These decorators make it easy to specify the processing logic for your consumers and producers, allowing you to focus on the core business logic of your application without worrying about the underlying integration.

Also, **Pydantic**’s [`BaseModel`](https://docs.pydantic.dev/usage/models/) class allows you to define messages using a declarative syntax for sending messages downstream, making it easy to specify the fields and types of your messages.

Here is an example Python app using **FastPubSub** that consumes data from an incoming data stream and outputs two messages to another one:


```python
# basic.py

from pydantic import BaseModel, Field
from fastpubsub import FastPubSub, PubSubBroker, Message
from fastpubsub.logger import logger

class Address(BaseModel):
    street: str = Field(..., examples=["5th Avenue"])
    number: str = Field(..., examples=["1548"])


broker = PubSubBroker(project_id="some-project-id")
app = FastPubSub(broker)

@broker.subscriber(
    alias="my_handler",
    topic_name="in_topic",
    subscription_name="sub_name",
)
async def handle_message(message: Message):
   logger.info(f"The message {message.id} is processed.")
   await broker.publish(topic_name="out_topic", data="Hi!")

   address = Address(street="Av. Flores", number="213")
   await broker.publish(topic_name="out_topic", data=address)
```



### Running the application

Before running the command make sure to set one of the variables (mutually exclusive):

1. **Running PubSub on Cloud**: The environment variable  `GOOGLE_APPLICATION_CREDENTIALS` with the path of the service-account on your system.
2. **Running PubSub Emulator**: The environment variable `PUBSUB_EMULATOR_HOST` with host:port of your local PubSub emulator.


---

After that, the application can be started using built-in **FastPubSub** CLI command. It is embedded in the library and its a core part of the system.

To run the service, use the **FastPubSub** embedded CLI. Just execute the command ``run`` and pass the module (in this case, the file where the app implementation is located) and the app symbol to the command.

```bash
fastpubsub run basic:app
```

After running the command, you should see the following output:


``` shell
2025-10-13 15:23:59,550 | INFO     | 97527:133552019097408 | runner:run:55 | FastPubSub app starting...
2025-10-13 15:23:59,696 | INFO     | 97527:133552019097408 | tasks:start:74 | The handle_message handler is waiting for messages.
```

Also, **FastPubSub** provides you with a great hot reload feature to improve your Development Experience

``` shell
fastpubsub run basic:app --reload
```

And multiprocessing horizontal scaling feature as well:

``` shell
fastpubsub run basic:app --workers 3
```

You can learn more about **CLI** features [here](docs/learn/tutorial/07.cli.md).


## Further Documentation

1. [Features](docs/features/00.index.md)
2. [Getting Started](docs/getting-started/00.index.md)
3. [Learn](docs/learn/00.index.md)
    1. [Introduction to Google PubSub](docs/learn/01.intro-pubsub.md)
    2. [Introduction to Async/Await](docs/learn/02.intro-async-await.md)
    3. [Introduction to Virtual Environments](docs/learn/03.intro-venv.md)
    4. [Tutorial: User Guide](docs/learn/tutorial/00.index.md)
        1. [Subscription Basics](docs/learn/tutorial/01.subscription.md)
        2. [Publishing Basics](docs/learn/tutorial/02.publishing.md)
        3. [Lifespan and Hooks](docs/learn/tutorial/03.lifespan.md)
        4. [Acknowledgement](docs/learn/tutorial/04.acknowledgement.md)
        5. [Routers (and Hierarchy)](docs/learn/tutorial/05.routers.md)
        6. [Middlewares (and Hierarchy)](docs/learn/tutorial/06.middlewares.md)
        7. [Command line Interface (CLI)](docs/learn/tutorial/07.cli.md)
        8. [Integrations](docs/learn/tutorial/integrations/00.index.md)
            1. [FastAPI](docs/learn/tutorial/integrations/01.fastapi.md)
            2. [Observability](docs/learn/tutorial/integrations/02.observability.md)
            3. [Logging](docs/learn/tutorial/integrations/03.logger.md)
            4. [Application Probes](docs/learn/tutorial/integrations/04.probes.md)
    5. [Deployment Guide](docs/learn/deployment/00.index.md)
        1. [On Virtual Machines](docs/learn/deployment/01.vm-guide.md)
        2. [On Kubernetes](docs/learn/deployment/02.k8-guide.md)


## Contact

Please stay in touch by:

Sending a email at sandro-matheus@hotmail.com.

Sending a message on my [linkedin](www.linkedin.com/in/matheusvnm).


## License
This project is licensed under the terms of the Apache 2.0 license.
