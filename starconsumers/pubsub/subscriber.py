from collections.abc import Callable
from datetime import timedelta
from typing import Any

from google.api_core.exceptions import AlreadyExists, GoogleAPICallError
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage
from google.cloud.pubsub_v1.types import FlowControl
from google.protobuf.field_mask_pb2 import FieldMask
from google.pubsub_v1.types import DeadLetterPolicy, RetryPolicy, Subscription

from starconsumers.datastructures import MessageControlFlowPolicy, TopicSubscription
from starconsumers.logger import logger
from starconsumers.pubsub.utils import is_emulator


class PubSubSubscriber:
    def _create_subscription_request(self, subscription: TopicSubscription) -> Subscription:
        name = SubscriberClient.subscription_path(subscription.project_id, subscription.name)
        topic = SubscriberClient.topic_path(subscription.project_id, subscription.topic_name)

        dlt_policy = None
        if subscription.dead_letter_policy:
            dlt_topic = SubscriberClient.topic_path(
                subscription.project_id,
                subscription.dead_letter_policy.topic_name,
            )
            dlt_policy = DeadLetterPolicy(
                dead_letter_topic=dlt_topic,
                max_delivery_attempts=subscription.dead_letter_policy.max_delivery_attempts,
            )

        min_backoff_delay = timedelta(seconds=subscription.retry_policy.min_backoff_delay_secs)
        max_backoff_delay = timedelta(seconds=subscription.retry_policy.max_backoff_delay_secs)
        retry_policy = RetryPolicy(
            minimum_backoff=min_backoff_delay, maximum_backoff=max_backoff_delay
        )

        return Subscription(
            name=name,
            topic=topic,
            retry_policy=retry_policy,
            dead_letter_policy=dlt_policy,
            filter=subscription.delivery_policy.filter_expression,
            ack_deadline_seconds=subscription.delivery_policy.ack_deadline_seconds,
            enable_message_ordering=subscription.delivery_policy.enable_message_ordering,
            enable_exactly_once_delivery=subscription.delivery_policy.enable_exactly_once_delivery,
        )

    def create_subscription(self, subscription: TopicSubscription) -> bool:
        """
        Creates the Pub/Sub subscription if it doesn't exist.
        Handles AlreadyExists errors gracefully.
        """
        subscription_request = self._create_subscription_request(subscription=subscription)

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

    def update_subscription(self, subscription: TopicSubscription) -> None:
        subscription_request = self._create_subscription_request(subscription=subscription)
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
        project_id: str,
        subscription_name: str,
        control_flow_policy: MessageControlFlowPolicy,
        callback: Callable[[PubSubMessage], Any],
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
