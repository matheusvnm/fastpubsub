import json
from concurrent.futures import Future
from contextlib import suppress
from typing import Any

from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub_v1 import PublisherClient, SubscriberClient
from google.cloud.pubsub_v1.types import PublisherOptions

from starconsumers import observability
from starconsumers.logger import logger
from starconsumers.pubsub.utils import ensure_pubsub_credentials


class PubSubPublisherClient:
    def __init__(self, project_id: str, topic_name: str) -> None:
        self.topic = PublisherClient.topic_path(project=project_id, topic=topic_name)
        self.default_subscription = SubscriberClient.subscription_path(
            project=project_id, subscription=topic_name
        )
        ensure_pubsub_credentials()

    def create_topic(self) -> None:
        """
        Creates the configured Pub/Sub topic.
        """
        publisher_client = PublisherClient()
        with suppress(AlreadyExists):
            logger.debug(f"Creating topic '{self.topic}'.")
            publisher_client.create_topic(name=self.topic)
            logger.debug(f"Created topic '{self.topic}' sucessfully.")

            with SubscriberClient() as subscriber_client:
                logger.debug(f"Creating default subscription for '{self.topic}'.")
                subscriber_client.create_subscription(
                    name=self.default_subscription, topic=self.topic
                )
                logger.debug(
                    f"Creating default subscription created successfully for '{self.topic}'."
                )

    def publish(
        self, *, data: dict[str, Any], attributes: dict[str, str] = {}, ordering_key: str = ""
    ) -> None:
        """
        Publishes some data on a configured Pub/Sub topic.
        It considers that the topic is already created
        """
        apm = observability.get_apm_provider()
        attributes = {} if attributes is None else attributes
        headers = apm.get_distributed_trace_context()
        headers.update(attributes)

        ordered = True if ordering_key else False
        publisher_options = PublisherOptions(enable_message_ordering=ordered)
        client = PublisherClient(publisher_options=publisher_options)

        try:
            encoded_data = json.dumps(data).encode()
            future: Future[str] = client.publish(
                topic=self.topic, data=encoded_data, ordering_key=ordering_key, **headers
            )
            message_id = future.result()
            logger.info(f"Message published for topic {self.topic} with id {message_id}")
            logger.debug(f"We sent {data} with metadata {attributes}")
        except Exception:
            logger.exception("Publisher failure", stacklevel=5)
            raise
