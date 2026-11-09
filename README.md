<p align="center">
  <a href="https://docs.fastpubsub.dev"><img src="https://docs.fastpubsub.dev/assets/images/logo.png" alt="FastPubSub"></a>
</p>

<p align="center">
    <em>A high-performance, asyncio-native framework for Google Cloud Pub/Sub applications.</em>
</p>

<p align="center">
<a href="https://github.com/matheusvnm/fastpubsub/actions/workflows/pr_tests.yaml?query=branch%3Adev" target="_blank">
    <img src="https://github.com/matheusvnm/fastpubsub/actions/workflows/pr_tests.yaml/badge.svg?query=branch%3Adev" alt="Quality Checks">
</a>
<a href="https://pepy.tech/projects/fastpubsub">
    <img src="https://static.pepy.tech/personalized-badge/fastpubsub?period=monthly&units=INTERNATIONAL_SYSTEM&left_color=GREY&right_color=BRIGHTGREEN&left_text=Downloads" alt="PyPI Downloads">
</a>
<a href="https://pypi.org/project/fastpubsub" target="_blank">
    <img src="https://img.shields.io/pypi/v/fastpubsub?color=%2334D058&label=pypi%20package" alt="PyPI">
</a>
<a href="https://pypi.org/project/fastpubsub" target="_blank">
    <img src="https://img.shields.io/pypi/pyversions/fastpubsub.svg?color=%2334D058" alt="Supported Python versions">
</a>
</p>


---


**Documentation**: <a href="https://docs.fastpubsub.dev" target="_blank">https://docs.fastpubsub.dev</a>

**Source Code**: <a href="https://github.com/matheusvnm/fastpubsub" target="_blank">https://github.com/matheusvnm/fastpubsub</a>

---

## Features


FastPubSub is a high-performance framework for building applications that process message events on Google Pub/Sub. A compiled core, written in Rust, runs all Pub/Sub network I/O. FastAPI, Pydantic and Uvicorn give it an easy-to-use development experience in Python.

The key features are:

- **Fast:** A compiled core runs the streams, the batches and the acknowledgements outside the Python interpreter. It is written in Rust, on Google's official [Rust client for Pub/Sub](https://github.com/googleapis/google-cloud-rust). Your handlers stay plain Python.
- **Intuitive**: It is designed to be intuitive and easy to use, even for beginners.
- **Typed**: Provides great editor support and less time reading docs.
- **Robust**: Get production-ready code with sensible default values helping you avoid common pitfalls.
- **Asynchronous:** It is built on top of asyncio, which allows it to run on fully asynchronous code.
- **Batteries Included**: Provides its own CLI and other widely used tools such as [**pydantic**](https://docs.pydantic.dev/) for data validation and log contextualization.

## Performance

The Rust core is the reason that FastPubSub is fast. It receives, acknowledges and publishes messages on its own threads, without the Python GIL. It gives the received messages to the asyncio event loop in batches, so Python does only the work of your handlers.

We compared FastPubSub with the Google Python client (`google-cloud-pubsub` with a non-blocking callback) on real Pub/Sub. The runs used a 4-vCPU VM in the same region, 2 CPUs for each run and 100 messages in flight. Each value is the median of 5 runs:

| Scenario | Metric | Google Python client | FastPubSub |
|---|---|---|---|
| Echo (receive and publish) | CPU per message | 276 µs | 87 µs |
| Echo (receive and publish) | Messages per second | 1,032 | 1,005 |
| Handler waits 100 ms | Messages per second | 396 | 1,511 |
| Handler waits 100 ms | CPU per message | 666 µs | 94 µs |
| Handler uses 1 ms of CPU and waits 100 ms | Messages per second | 393 | 815 |
| All scenarios | Peak memory | 100 to 106 MB | 62 to 69 MB |

The more time a handler spends waiting, the larger the gain. When the handler does almost nothing, as in echo, the network sets the limit and the throughput is about the same. The CPU and the memory are lower in all scenarios.

## Quick Start

### Installation

FastPubSub has ready wheels for Linux, macOS (arm64) and Windows, for CPython 3.12 and later and for free-threaded Python. You can install it with `pip` as usual:

```shell
pip install fastpubsub
```

### Writing your first application

**FastPubSub** broker connection provides convenient function decorators (`@broker.subscriber`) and methods (`broker.publisher`) to allow you to delegate the actual process of:

- Creating Pub/Sub subscriptions to receive and process data from topics.
- Publishing data to other topics downstream in your message processing pipeline.

These decorators make it easy to specify the processing logic for your producers and consumers, allowing you to focus on the core business logic of your application without worrying about the underlying integration.

Also, **Pydantic**’s [`BaseModel`](https://docs.pydantic.dev/usage/models/) class allows you to define messages using a declarative syntax for sending messages downstream, making it easy to specify the fields and types of your messages.

Here is an example Python app using **FastPubSub** that consumes data from an incoming data stream and outputs one message to another one:


```python
from pydantic import BaseModel, Field

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger


class Address(BaseModel):
    street: str = Field(..., examples=["5th Avenue"])
    number: str = Field(..., examples=["1548"])

broker = PubSubBroker(project_id="fastpubsub-pubsub-local")
app = FastPubSub(broker)

@broker.subscriber(
    alias="my_handler",
    topic_name="in_topic",
    subscription_name="sub_name",
)
async def handle_message(message: Message):
    logger.info(f"The message {message.id} is processed.")
    await broker.publish(topic_name="out_topic", data="Hi!")

@app.after_startup
async def publish_initial_message() -> None:
    address = Address(street="Av. Flores", number="213")
    await broker.publish(topic_name="in_topic", data=address)
```


### Running the application

Before running the command make sure to set one of the variables (mutually exclusive):

1. **Running Pub/Sub on Cloud**: The environment variable  `GOOGLE_APPLICATION_CREDENTIALS` with the path of the service-account on your system.
2. **Running Pub/Sub Emulator**: The environment variable `PUBSUB_EMULATOR_HOST` with `host:port` of your local Pub/Sub emulator.


---

After that, the application can be started using the built-in **FastPubSub** CLI which is a core part of the framework. Just execute the command ``run`` and pass the module and the app symbol to the command.

```bash
fastpubsub run basic:app
```

After running the command, you should see the following output:


```shell
2026-02-04 21:00:07,469 | INFO     | 88674:8702897216 | runner:run:76 | FastPubSub app starting...
2026-02-04 21:00:07,999 | INFO     | 88674:8702897216 | tasks:start:80 | The handle_message handler is waiting for messages.
2026-02-04 21:00:08,028 | INFO     | 88674:8702897216 | pubsub:publish:248 | Message published for topic projects/fastpubsub-pubsub-local/topics/in_topic with id 17
2026-02-04 21:00:08,094 | INFO     | 88674:8702897216 | e1_01_basic_subscriber:handle_message:25 | The message 1 is processed. | message_id=17 | topic_name=in_topic | subscriber_name=handle_message
2026-02-04 21:00:08,125 | INFO     | 88674:8702897216 | pubsub:publish:248 | Message published for topic projects/fastpubsub-pubsub-local/topics/out_topic with id 18 | message_id=17 | topic_name=in_topic | subscriber_name=handle_message
2026-02-04 21:00:08,125 | INFO     | 88674:8702897216 | tasks:_consume:107 | The message successfully processed. | message_id=17 | topic_name=in_topic | subscriber_name=handle_message
```

Also, **FastPubSub** provides you with a great hot reload feature to improve your development experience

``` shell
fastpubsub run basic:app --reload
```

And multiprocessing horizontal scaling feature as well:

``` shell
fastpubsub run basic:app --workers 3
```

We just scratched the surface, but you already get the idea of how the basic structure works. Everything built into FastPubSub is meant to improve the development experience and provide great productivity with high-performance guarantees.

## Contact

Feel free to get in touch by:

Sending a email at sandro-matheus@hotmail.com.

Sending a message on my [linkedin](https://www.linkedin.com/in/matheusvnm).


## License
This project is licensed under the terms of the Apache 2.0 license.

</div>
