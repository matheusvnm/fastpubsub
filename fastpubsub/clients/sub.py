from contextlib import suppress
from datetime import timedelta
import os

from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.types import FlowControl
from google.protobuf.field_mask_pb2 import FieldMask
from google.pubsub_v1.types import DeadLetterPolicy, RetryPolicy, Subscription

from fastpubsub.logger import logger
from fastpubsub.clients.handlers import CallbackHandler
from fastpubsub.subscriber import Subscriber


class PubSubSubscriberClient:
    def __init__(self):
        self.is_emulator = True if os.getenv("PUBSUB_EMULATOR_HOST") else False

    def _create_subscription_request(self, subscriber: Subscriber) -> Subscription:
        name = SubscriberClient.subscription_path(
            subscriber.project_id, subscriber.subscription_name
        )
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

        with suppress(AlreadyExists):
            with SubscriberClient() as client:
                logger.debug(f"Attempting to create subscription: {subscription_request.name}")
                client.create_subscription(request=subscription_request)
                logger.debug(f"Successfully created subscription: {subscription_request.name}")
                return True

    def update_subscription(self, subscriber: Subscriber) -> None:
        subscription_request = self._create_subscription_request(subscriber=subscriber)
        update_fields = [
            "ack_deadline_seconds",
            "dead_letter_policy",
            "retry_policy",
            "enable_exactly_once_delivery",
        ]

        if not self.is_emulator:
            update_fields.append("filter")

        update_mask = FieldMask(paths=update_fields)
        with SubscriberClient() as client:
            logger.debug(f"Attempting to update the subscription: {subscription_request.name}")
            response = client.update_subscription(
                subscription=subscription_request, update_mask=update_mask
            )
            logger.debug(f"Successfully updated the subscription: {subscription_request.name}")
            logger.debug(f"The subscription is now following the configuration: {response}")

    def subscribe(self, subscriber: Subscriber) -> None:
        """
        Starts listening for messages on the configured Pub/Sub subscription.
        This method is blocking and will run indefinitely.
        """
        subscription_path = SubscriberClient.subscription_path(
            subscriber.project_id, subscriber.subscription_name
        )
        callback_handler = CallbackHandler(subscriber)

        with SubscriberClient() as client:
            logger.debug(f"Listening for messages on {subscription_path}")
            streaming_pull_future = client.subscribe(
                subscription_path,
                await_callbacks_on_shutdown=True,
                callback=callback_handler.handle,
                flow_control=FlowControl(
                    max_messages=subscriber.control_flow_policy.max_messages,
                    max_bytes=subscriber.control_flow_policy.max_bytes,
                ),
            )
            try:
                streaming_pull_future.result()
            except KeyboardInterrupt:
                logger.debug(f"Subscriber '{subscriber.subscription_name}' stopped by user")
            except Exception:
                logger.exception(
                    f"Subscription stream terminated unexpectedly for '{subscriber.subscription_name}'",
                    stacklevel=5,
                )
            finally:
                logger.debug(
                    f"Sending cancel streaming pull command for '{subscriber.subscription_name}'."
                )
                streaming_pull_future.cancel()
                logger.debug(f"Subscriber '{subscriber.subscription_name}' has shutdown.")
