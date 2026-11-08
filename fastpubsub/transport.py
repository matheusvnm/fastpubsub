"""Select the Pub/Sub transport.

``FASTPUBSUB_TRANSPORT=python`` (the default) uses the Google Python
client. ``FASTPUBSUB_TRANSPORT=rust`` uses the Rust core
(``fastpubsub_core``) and does not import the Google Python client. The
value is read once, when this module is imported.
"""

import os

TRANSPORT_VARIABLE = "FASTPUBSUB_TRANSPORT"


def is_rust_transport() -> bool:
    """Check if the Rust transport is selected.

    Returns:
        True when ``FASTPUBSUB_TRANSPORT`` is ``rust``.
    """
    return os.getenv(TRANSPORT_VARIABLE, "python").strip().lower() == "rust"


if not is_rust_transport():
    from google.api_core.exceptions import NotFound

    from fastpubsub.clients.factory import PubSubClientFactory
    from fastpubsub.clients.pubsub import PubSubClient
    from fastpubsub.concurrency.tasks import PubSubStreamingPullTask
else:
    from fastpubsub.rust.client import (  # type: ignore[assignment]
        NotFound,
        PubSubClient,
        PubSubClientFactory,
    )
    from fastpubsub.rust.tasks import (  # type: ignore[assignment]
        PubSubStreamingPullTask,
    )

__all__ = [
    "NotFound",
    "PubSubClient",
    "PubSubClientFactory",
    "PubSubStreamingPullTask",
    "is_rust_transport",
]
