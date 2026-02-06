<div class="home" markdown>

# FastPubSub

<p align="center">
  <a href="/"><img src="assets/images/logo.png" alt="FastPubSub"></a>
</p>

<p align="center">
    <em>A high-performance, asyncio-native framework for Google Cloud Pub/Sub applications.</em>
</p>


---


**Documentation**: https://fastpubsub.github.io

**Source Code**: <a href="https://github.com/matheusvnm/fastpubsub" target="_blank">https://github.com/matheusvnm/fastpubsub</a>

---

## Features


FastPubSub is a high-performance framework for building applications that streaming message events on Google Pub/Sub. It combines the Google Pub/Sub Python SDK with FastAPI, Pydantic, and Uvicorn to provide an easy-to-use development experience.

The key features are:

- **Fast:** Built on top of [**FastAPI**](https://fastapi.tiangolo.com/), [**uvicorn**](https://uvicorn.dev/) and the [**Google Pub/Sub Python SDK**](https://github.com/googleapis/python-pubsub) for high throughput.
- **Intuitive**: It is designed to be intuitive and easy to use, even for beginners.
- **Typed**: Provides great editor support and less time reading docs.
- **Robust**: Get production-ready code with sensible default values helping you avoid common pitfalls.
- **Asynchronous:** It is built on top of asyncio, which allows it to run on fully asynchronous code.
- **Batteries Included**: Provides its own CLI and other widely used tools such as [**pydantic**](https://docs.pydantic.dev/) for data validation and log contextualization.



## Quick Start

### Installation

FastPubSub works on Linux, macOS, Windows and most Unix-style operating systems. You can install it with `pip` as usual:

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
--8<-- "basic_usage/e1_01_basic_subscriber.py"
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
