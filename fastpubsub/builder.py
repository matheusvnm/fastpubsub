"""Builds and configures Pub/Sub subscriptions."""

from fastpubsub.pubsub.subscriber import Subscriber
from fastpubsub.transport import PubSubClient


class PubSubSubscriptionBuilder:
    """A builder for creating and updating Pub/Sub subscriptions."""

    def __init__(self, project_id: str) -> None:
        """Initializes the PubSubSubscriptionBuilder.

        Args:
            project_id: The Google Cloud project ID.
        """
        self.client = PubSubClient(project_id=project_id)
        self.created_topics: set[str] = set()

    async def build(self, subscriber: Subscriber) -> None:
        """Builds a subscription for the given subscriber.

        Args:
            subscriber: The subscriber to build the subscription for.
        """
        self.subscriber = subscriber
        if self.subscriber.lifecycle_policy.autocreate:
            await self._create_topics()
            await self._create_subscription()

        if self.subscriber.lifecycle_policy.autoupdate:
            await self._update_subscription()

    async def _create_topics(self) -> None:
        """Creates the topic of the subscriber and its dead-letter topic.

        The dead-letter topic gets a default subscription, so the messages
        that it receives stay available.
        """
        target_topic = self.subscriber.topic_name
        await self._new_topic(
            topic_name=target_topic, create_default_subscription=False
        )

        if self.subscriber.dead_letter_policy:
            target_topic = self.subscriber.dead_letter_policy.topic_name
            await self._new_topic(
                topic_name=target_topic, create_default_subscription=True
            )

    async def _new_topic(
        self, topic_name: str, create_default_subscription: bool = True
    ) -> None:
        """Creates one topic, one time for each builder.

        Args:
            topic_name: The name of the topic.
            create_default_subscription: Also create a subscription with the
                same name as the topic.
        """
        if topic_name in self.created_topics:
            return

        await self.client.create_topic(
            topic_name=topic_name,
            create_default_subscription=create_default_subscription,
        )
        self.created_topics.add(topic_name)

    async def _create_subscription(self) -> None:
        """Creates the subscription with the policies of the subscriber."""
        await self.client.create_subscription(
            topic_name=self.subscriber.topic_name,
            subscription_name=self.subscriber.subscription_name,
            retry_policy=self.subscriber.retry_policy,
            delivery_policy=self.subscriber.delivery_policy,
            dead_letter_policy=self.subscriber.dead_letter_policy,
        )

    async def _update_subscription(self) -> None:
        """Updates the subscription with the policies of the subscriber."""
        await self.client.update_subscription(
            topic_name=self.subscriber.topic_name,
            subscription_name=self.subscriber.subscription_name,
            retry_policy=self.subscriber.retry_policy,
            delivery_policy=self.subscriber.delivery_policy,
            dead_letter_policy=self.subscriber.dead_letter_policy,
        )
