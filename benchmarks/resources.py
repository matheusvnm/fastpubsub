"""Topic and subscription setup for one benchmark run."""

import uuid
from contextlib import suppress
from dataclasses import dataclass

from google.api_core.exceptions import NotFound
from google.cloud.pubsub import PublisherClient, SubscriberClient

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
        topic_path: str = PublisherClient.topic_path(
            PROJECT_ID, self.topic_name
        )
        return topic_path

    @property
    def subscription_path(self) -> str:
        """The full subscription path."""
        subscription_path: str = SubscriberClient.subscription_path(
            PROJECT_ID, self.subscription_name
        )
        return subscription_path


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
