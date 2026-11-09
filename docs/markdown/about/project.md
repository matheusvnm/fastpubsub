---
icon: lucide/info
---

# About FastPubSub

FastPubSub is a modern, high-performance framework for building applications that process event messages on Google Cloud Pub/Sub.

## Project Goals

FastPubSub was created to simplify the development of Pub/Sub consumers by:

- Handling the complexities of asynchronous message processing
- Providing an intuitive, decorator-based API for defining subscribers and publishers
- Integrating seamlessly with FastAPI for hybrid REST + Pub/Sub applications
- Offering production-ready features out of the box

## Design Philosophy

- **Developer Experience First**: Simple, intuitive APIs that feel natural to Python developers
- **Performance**: A compiled Rust core for the Pub/Sub I/O, and asyncio for concurrent handlers
- **Production Ready**: Built-in health checks, structured logging, and graceful shutdown
- **Flexible**: Support for dead-letter topics, filtering, ordering, and exactly-once delivery

## Why FastPubSub Is Fast

FastPubSub has two parts:

- **`fastpubsub`** is the Python package that you use: the broker, the routers, the subscribers, the middlewares, the CLI and the FastAPI integration.
- **`fastpubsub-core`** is a compiled module, written in Rust with [PyO3](https://pyo3.rs). It uses Google's official [Rust client for Pub/Sub](https://github.com/googleapis/google-cloud-rust). pip installs it with `fastpubsub`.

The core does all the Pub/Sub work:

- It keeps the streaming pull open, and applies the flow control.
- It extends the ack deadlines of the messages that your handlers still process.
- It sends the acks and the nacks.
- It collects the published messages in batches and sends them.

The core runs this work on its own threads, without the Python GIL. It gives the received messages and the publish results to the asyncio event loop in batches. It takes the GIL one time for each batch. So the Python interpreter runs only your handlers and the FastPubSub middlewares.

A Python client library does the same Pub/Sub work in Python. It decodes each message, manages the leases and runs the callbacks in Python threads, and each of these steps needs the GIL. In our benchmarks on real Pub/Sub, FastPubSub used 1.7 to 7 times less CPU per message than the Google Python client. It also used 32% to 41% less memory. The gain is smallest when the handler itself uses much CPU. With handlers that wait for I/O, FastPubSub processed 2 to 4 times more messages per second. See the [Performance](../index.md#performance) table.

## Links

- **GitHub**: [github.com/matheusvnm/fastpubsub](https://github.com/matheusvnm/fastpubsub)
- **PyPI**: [pypi.org/project/fastpubsub](https://pypi.org/project/fastpubsub/)
- **Issues**: [GitHub Issues](https://github.com/matheusvnm/fastpubsub/issues)

## License

FastPubSub is open source software licensed under the [Apache 2.0 License](https://github.com/matheusvnm/fastpubsub/blob/master/LICENSE).
