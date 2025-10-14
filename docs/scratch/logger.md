# Logger

**FastPubSub** comes with a built-in logger that can be used to log messages.
The logger is a standard Python logger, so you can use it in the same way you would use any other logger.

## Usage

Here's an example of how to use the logger:

```python
from fastpubsub.logger import logger

logger.info("This is an info message")
logger.warning("This is a warning message")
logger.error("This is an error message")
```

## Serialization

[*] focus on cli commands instead of the env variables

The logger can be configured to serialize logs in JSON format. This can be useful if you want to send your logs to a log aggregation service.

To enable serialization, you can set the `FASTPUBSUB_ENABLE_LOG_SERIALIZE` environment variable to `1`:

```bash
FASTPUBSUB_ENABLE_LOG_SERIALIZE=1 fastpubsub run my_app:app
```

## Contextualization

The logger supports contextualization, which allows you to add extra data to your logs. This can be useful for tracing requests or for adding extra information to your logs.
By default the logger adds the name of the handler, the name of the topic, the message id. If the observability module is enabled them the trace_id/span_is is also added.

Here's an example of how to use contextualization:

```python
from fastpubsub.logger import logger

with logger.contextualize(trace_id="12345"):
    logger.info("This log will have the trace_id.")
