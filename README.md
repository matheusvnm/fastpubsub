# FastPubSub

*A high performance FastAPI-based message consumer framework for PubSub.*

[//]: # (Aqui devem ir algumas tags)


## Features


FastPubSub is a modern, high-performance framework for building modern applications that process event messages on Google PubSub. It combines the standard PubSub Python SDK with FastAPI, Pydantic and Uvicorn to provide an easy-to-use development experience.

The key features are:

- **Fast:** FastPubSub is (unironically) fast. It's built on top of [**FastAPI**](https://fastapi.tiangolo.com/), [**uvicorn**](https://uvicorn.dev/) and [**Google PubSub Python SDK**](https://github.com/googleapis/python-pubsub) for maximum performance.
- **Intuitive**: It is designed to be intuitive and easy to use, even for beginners.
- **Typed**: Provides a great editor support and less time reading docs.
- **Robust**: Get production-ready code with sensible default values helping you avoid common pitfalls.
- **Asynchronous:** It is built on top of anyio, which allows it to run on top of either asyncio or trio.
- **Batteries Included**: Provides its own CLI and other widely used tools such as [**pydantic**](https://docs.pydantic.dev/) for data validation, observability integrations and log contextualization.



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
2. **Running PubSub Emulator**: The environment variable `PUBSUB_EMULATOR_HOST` with `host:port` of your local PubSub emulator.


---

After that, the application can be started using built-in **FastPubSub** CLI which is a core part of the framework.

To run the service, use the **FastPubSub** embedded CLI. Just execute the command ``run`` and pass the module (in this case, the file where the app implementation is located) and the app symbol to the command.

```bash
fastpubsub run basic:app
```

After running the command, you should see the following output:


``` shell
2025-10-13 15:23:59,550 | INFO     | 97527:133552019097408 | runner:run:55 | FastPubSub app starting...
2025-10-13 15:23:59,696 | INFO     | 97527:133552019097408 | tasks:start:74 | The handle_message handler is waiting for messages.
```

Also, **FastPubSub** provides you with a great hot reload feature to improve your development experience

``` shell
fastpubsub run basic:app --reload
```

And multiprocessing horizontal scaling feature as well:

``` shell
fastpubsub run basic:app --workers 3
```

You can learn more about CLI's features [here](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/07.cli.md).


## Further Documentation

1. [Features](https://github.com/matheusvnm/fastpubsub/tree/dev/docsfeatures/00.index.md)
2. [Getting Started](https://github.com/matheusvnm/fastpubsub/tree/dev/docsgetting-started/00.index.md)
3. [Learn](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/00.index.md)
    1. [Introduction to Google PubSub](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/01.intro-pubsub.md)
    2. [Introduction to Async/Await](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/02.intro-async-await.md)
    3. [Tutorial: User Guide](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/00.index.md)
        1. [Subscribing Basics](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/01.subscription.md)
        2. [Publishing Basics](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/02.publishing.md)
        3. [Lifespan and Hooks](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/03.lifespan.md)
        4. [Acknowledgement](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/04.acknowledgement.md)
        5. [Routers (and Hierarchy)](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/05.routers.md)
        6. [Middlewares (and Hierarchy)](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/06.middlewares.md)
        7. [Command line Interface (CLI)](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/07.cli.md)
        8. [Integrations](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/integrations/00.index.md)
            1. [FastAPI](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/integrations/01.fastapi.md)
            2. [Observability](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/integrations/02.observability.md)
            3. [Logging](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/integrations/03.logger.md)
            4. [Application Probes](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/tutorial/integrations/04.probes.md)
    5. [Deployment Guide](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/deployment/00.index.md)
        1. [On Virtual Machines](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/deployment/01.vm-guide.md)
        2. [On Kubernetes](https://github.com/matheusvnm/fastpubsub/tree/dev/docslearn/deployment/02.k8-guide.md)


## Contact

Feel free to get in touch by:

Sending a email at sandro-matheus@hotmail.com.

Sending a message on my [linkedin](www.linkedin.com/in/matheusvnm).


## License
This project is licensed under the terms of the Apache 2.0 license.
