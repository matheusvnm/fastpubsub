# Observability and Logging

**FastPubSub** is a "batteries included" framework, and that includes observability. We believe that you should only have to worry about your business logic, and not about the boilerplate of setting up logging and tracing. That's why **FastPubSub** comes with a built-in logger and support for APM providers like `New Relic`, with more to come.

## APM Provider Integrations

To enable APM, you can use the `--apm-provider` flag:

```bash
fastpubsub run my_app:app --apm-provider newrelic
```

You can also set the `FASTPUBSUB_APM_PROVIDER` environment variable:

```bash
export FASTPUBSUB_APM_PROVIDER=newrelic
fastpubsub run my_app:app
```

You will also need to install the `newrelic` package:

```bash
pip install fastpubsub[newrelic]
```

> **Note**
> You still need to configure the `newrelic` package by setting the appropriate environment variables, such as `NEW_RELIC_DISTRIBUTED_TRACING_ENABLED`, `NEW_RELIC_LICENSE_KEY`, and `NEW_RELIC_APP_NAME`.

### Distributed Tracing

**FastPubSub** automatically propagates the distributed trace context for you. This means that if you have a distributed tracing system set up, **FastPubSub** will automatically create spans for your subscribers and publishers.

## The FastPubSub logger

**FastPubSub** comes with a built-in logger that can be used to log messages. The logger is a standard Python logger, so you can use it in the same way you would use any other logger.

### Usage

Here's an example of how to use the logger:

```python
from fastpubsub.logger import logger

logger.info("This is an info message")
logger.warning("This is a warning message")
logger.error("This is an error message")
```

### Serialization

The logger can be configured to serialize logs in JSON format. This can be useful if you want to send your logs to a log aggregation service.

To enable serialization, you can use the `--log-serialize` flag:

```bash
fastpubsub run my_app:app --log-serialize
```

You can also set the `FASTPUBSUB_ENABLE_LOG_SERIALIZE` environment variable:

```bash
export FASTPUBSUB_ENABLE_LOG_SERIALIZE=1
fastpubsub run my_app:app
```

### Contextualization

The logger supports contextualization, which allows you to add extra data to your logs. This can be useful for tracing requests or for adding extra information to your logs.

Here's an example of how to use contextualization:

```python
from fastpubsub.logger import logger

with logger.contextualize(trace_id="12345"):
    logger.info("This log will have the trace_id.")
```
