from fastpubsub.clients.pub import PubSubPublisherClient
from fastpubsub.clients.sub import PubSubSubscriberClient
from fastpubsub.pubsub.subscriber import Subscriber


class PubSubSubscriptionBuilder:
    def __init__(self):
        self.created_topics: set[str] = set()

    def build(self, subscriber: Subscriber) -> None:
        self.subscriber = subscriber
        if self.subscriber.lifecycle_policy.autocreate:
            self._create_topics()
            self._create_subscription()

        if self.subscriber.lifecycle_policy.autoupdate:
            self._update_subscription()

    def _create_topics(self) -> None:
        target_topic = self.subscriber.topic_name
        self._new_topic(target_topic, create_default_subscription=False)

        if self.subscriber.dead_letter_policy:
            target_topic = self.subscriber.dead_letter_policy.topic_name
            self._new_topic(target_topic)

    def _new_topic(self, topic_name: str, create_default_subscription: bool = True) -> None:
        if topic_name in self.created_topics:
            return

        client = PubSubPublisherClient(project_id=self.subscriber.project_id, topic_name=topic_name)
        client.create_topic(create_default_subscription)
        self.created_topics.add(topic_name)

    def _create_subscription(self) -> None:
        client = PubSubSubscriberClient()
        client.create_subscription(subscriber=self.subscriber)

    def _update_subscription(self) -> None:
        client = PubSubSubscriberClient()
        client.update_subscription(subscriber=self.subscriber)
