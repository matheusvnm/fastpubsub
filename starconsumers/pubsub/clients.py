from datetime import timedelta


from google.api_core.exceptions import AlreadyExists, GoogleAPICallError
from google.cloud.pubsub_v1 import SubscriberClient, PublisherClient
from google.cloud.pubsub_v1.types import FlowControl, PublisherOptions
from google.protobuf.field_mask_pb2 import FieldMask
from google.pubsub_v1.types import DeadLetterPolicy, RetryPolicy, Subscription

from starconsumers.datastructures import MessageControlFlowPolicy, TopicSubscription
from starconsumers.logger import logger
from starconsumers.pubsub.utils import is_emulator
from starconsumers.subscriber import Subscriber


import json
from concurrent.futures import Future
from typing import Any

from starconsumers import observability
from starconsumers.logger import logger



class PubSubSubscriberClient:
    def _create_subscription_request(self, subscriber: Subscriber) -> Subscription:
        name = SubscriberClient.subscription_path(subscriber.project_id, subscriber.name)
        topic = SubscriberClient.topic_path(subscriber.project_id, subscriber.topic_name)

        dlt_policy = None
        if subscriber.dead_letter_policy:
            dlt_topic = SubscriberClient.topic_path(
                subscriber.project_id,
                subscriber.dead_letter_policy.topic_name,
            )
            dlt_policy = DeadLetterPolicy(
                dead_letter_topic=dlt_topic,
                max_delivery_attempts=subscriber.dead_letter_policy.max_delivery_attempts,
            )

        min_backoff_delay = timedelta(seconds=subscriber.retry_policy.min_backoff_delay_secs)
        max_backoff_delay = timedelta(seconds=subscriber.retry_policy.max_backoff_delay_secs)
        retry_policy = RetryPolicy(
            minimum_backoff=min_backoff_delay, maximum_backoff=max_backoff_delay
        )

        return Subscription(
            name=name,
            topic=topic,
            retry_policy=retry_policy,
            dead_letter_policy=dlt_policy,
            filter=subscriber.delivery_policy.filter_expression,
            ack_deadline_seconds=subscriber.delivery_policy.ack_deadline_seconds,
            enable_message_ordering=subscriber.delivery_policy.enable_message_ordering,
            enable_exactly_once_delivery=subscriber.delivery_policy.enable_exactly_once_delivery,
        )

    def create_subscription(self, subscriber: Subscriber) -> bool:
        """
        Creates the Pub/Sub subscription if it doesn't exist.
        Handles AlreadyExists errors gracefully.
        """
        subscription_request = self._create_subscription_request(subscriber=subscriber)

        with SubscriberClient() as client:
            try:
                logger.info(f"Attempting to create subscription: {subscription_request.name}")
                client.create_subscription(request=subscription_request)
                logger.info(f"Successfully created subscription: {subscription_request.name}")
                return True
            except AlreadyExists:
                logger.info(
                    f"Subscription '{subscription_request.name}' already exists. Skipping creation."
                )
                return False
            except GoogleAPICallError:
                logger.exception(
                    f"Failed to create subscription '{subscription_request.name}'", stacklevel=5
                )
                raise
            except Exception:
                logger.exception(
                    "An unexpected error occurred during subscription creation", stacklevel=5
                )
                raise

    def update_subscription(self, subscriber: Subscriber) -> None:
        subscription_request = self._create_subscription_request(subscriber=subscriber)
        update_fields = [
            "ack_deadline_seconds",
            "dead_letter_policy",
            "retry_policy",
            "enable_exactly_once_delivery",
        ]

        if not is_emulator():
            update_fields.append("filter")

        update_mask = FieldMask(paths=update_fields)
        with SubscriberClient() as client:
            try:
                logger.info(f"Attempting to update the subscription: {subscription_request.name}")
                response = client.update_subscription(
                    subscription=subscription_request, update_mask=update_mask
                )
                logger.info(f"Successfully updated the subscription: {subscription_request.name}")
                logger.debug(
                    f"The subscription is now set with the following configuration: {response}"
                )
            except GoogleAPICallError:
                logger.exception(
                    f"Failed to update subscription '{subscription_request.name}'", stacklevel=5
                )
                raise
            except Exception:
                logger.exception(
                    "An unexpected error occurred during subscription update", stacklevel=5
                )
                raise

    def subscribe(
        self,
        subscriber: Subscriber
    ) -> None:
        """
        Starts listening for messages on the configured Pub/Sub subscription.
        This method is blocking and will run indefinitely.
        """
        subscription_path = SubscriberClient.subscription_path(project_id, subscription_name)

        with SubscriberClient() as client:
            logger.info(f"Listening for messages on {subscription_path}")
            streaming_pull_future = client.subscribe(
                subscription_path,
                await_callbacks_on_shutdown=True,
                callback=callback,
                flow_control=FlowControl(
                    max_messages=control_flow_policy.max_messages,
                    max_bytes=control_flow_policy.max_bytes,
                ),
            )
            try:
                streaming_pull_future.result()
            except KeyboardInterrupt:
                logger.info("Subscriber stopped by user")
            except Exception:
                logger.exception("Subscription stream terminated unexpectedly", stacklevel=5)
            finally:
                logger.info("Sending cancel streaming pull command.")
                streaming_pull_future.cancel()
                logger.info("Subscriber has shut down.")




class PubSubPublisherClient:
    def __init__(self, project_id: str, topic_name: str) -> None:
        self.topic = PublisherClient.topic_path(project=project_id, topic=topic_name)

    def create_topic(self) -> None:
        """
        Creates the configured Pub/Sub topic.
        """
        client = PublisherClient()
        try:
            client.create_topic(
                name=self.topic,
            )
            # TODO: Add a default subscription with the same name of the topic
            logger.info("Created topic sucessfully.")
        except AlreadyExists:
            logger.info("The topic already exists.")

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
