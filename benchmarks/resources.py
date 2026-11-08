"""Topic and subscription setup for one benchmark run.

The Rust transport uses the admin client of the Rust core. The Python
transport uses the Google Python client. The free-threaded builds of
Python cannot install the Google Python client.
"""

import os
import uuid
from contextlib import suppress
from dataclasses import dataclass

from benchmarks.workload import ACK_DEADLINE_SECONDS

PROJECT_ID = "fastpubsub-benchmark"


@dataclass(frozen=True)
class RunResources:
    """The names of the topic and the subscription of one run."""

    topic_name: str
    subscription_name: str

    @property
    def topic_path(self) -> str:
        """The full topic path."""
        return f"projects/{PROJECT_ID}/topics/{self.topic_name}"

    @property
    def subscription_path(self) -> str:
        """The full subscription path."""
        return f"projects/{PROJECT_ID}/subscriptions/{self.subscription_name}"


def is_rust_transport() -> bool:
    """Check if the worker runs the Rust transport."""
    return os.getenv("FASTPUBSUB_TRANSPORT", "python") == "rust"


def read_endpoint() -> str | None:
    """Read the emulator endpoint for the Rust admin client."""
    emulator_host = os.getenv("PUBSUB_EMULATOR_HOST")
    return f"http://{emulator_host}" if emulator_host else None


def create_run_resources(case_name: str) -> RunResources:
    """Create a new topic and a new subscription for one run.

    All cases use the same subscription settings.

    Args:
        case_name: The benchmark case name.

    Returns:
        The names of the created resources.
    """
    suffix = uuid.uuid4().hex[:8]
    resources = RunResources(
        topic_name=f"bench-{case_name}-{suffix}",
        subscription_name=f"bench-{case_name}-{suffix}-subscription",
    )

    if is_rust_transport():
        from fastpubsub_core import AdminClient

        admin_client = AdminClient(read_endpoint())
        admin_client.create_topic(resources.topic_path)
        admin_client.create_subscription(
            resources.subscription_path,
            resources.topic_path,
            {"ack_deadline_seconds": ACK_DEADLINE_SECONDS},
        )
        return resources

    from google.cloud.pubsub import PublisherClient, SubscriberClient

    publisher_client = PublisherClient()
    subscriber_client = SubscriberClient()
    try:
        publisher_client.create_topic(name=resources.topic_path)
        subscriber_client.create_subscription(
            name=resources.subscription_path,
            topic=resources.topic_path,
            ack_deadline_seconds=ACK_DEADLINE_SECONDS,
        )
    finally:
        subscriber_client.close()
        publisher_client.transport.close()

    return resources


def delete_run_resources(resources: RunResources) -> None:
    """Delete the topic and the subscription of one run.

    Args:
        resources: The resources to delete.
    """
    if is_rust_transport():
        from fastpubsub_core import AdminClient, NotFoundError

        admin_client = AdminClient(read_endpoint())
        with suppress(NotFoundError):
            admin_client.delete_subscription(resources.subscription_path)
        with suppress(NotFoundError):
            admin_client.delete_topic(resources.topic_path)
        return

    from google.api_core.exceptions import NotFound
    from google.cloud.pubsub import PublisherClient, SubscriberClient

    publisher_client = PublisherClient()
    subscriber_client = SubscriberClient()
    try:
        with suppress(NotFound):
            subscriber_client.delete_subscription(
                subscription=resources.subscription_path
            )
        with suppress(NotFound):
            publisher_client.delete_topic(topic=resources.topic_path)
    finally:
        subscriber_client.close()
        publisher_client.transport.close()
