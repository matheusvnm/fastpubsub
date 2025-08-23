from collections.abc import Callable
from datetime import timedelta
from gc import enable
from typing import Any

from google.api_core.exceptions import AlreadyExists, GoogleAPICallError
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.types import FlowControl
from google.cloud.pubsub_v1.subscriber.message import Message as PubSubMessage
from google.pubsub_v1.types import DeadLetterPolicy, RetryPolicy, Subscription

from starconsumers.datastructures import MessageControlFlowPolicy, TopicSubscription
from starconsumers.logger import logger


class PubSubSubscriber:
    def create_subscription(self, subscription: TopicSubscription) -> None:
        """
        Creates the Pub/Sub subscription if it doesn't exist.
        Handles AlreadyExists errors gracefully.
        """

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
        retry_policy = RetryPolicy(minimum_backoff=min_backoff_delay,
                                   maximum_backoff=max_backoff_delay)

        subscription_request = Subscription(
            name=name,
            topic=topic,
            retry_policy=retry_policy,
            dead_letter_policy=dlt_policy,
            filter=subscription.delivery_policy.filter_expression,
            ack_deadline_seconds=subscription.delivery_policy.ack_deadline_seconds,
            enable_message_ordering=subscription.delivery_policy.enable_message_ordering,
            enable_exactly_once_delivery=subscription.delivery_policy.enable_exactly_once_delivery,
        )

        with SubscriberClient() as client:
            try:
                logger.info(f"Attempting to create subscription: {subscription_request.name}")
                client.create_subscription(request=subscription_request)
                logger.info(f"Successfully created subscription: {subscription_request.name}")
            except AlreadyExists:
                logger.info(
                    f"Subscription '{subscription_request.name}' already exists. Skipping creation."
                )
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

    def subscribe(
        self, project_id: str, subscription_name: str, control_flow_policy: MessageControlFlowPolicy, callback: Callable[[PubSubMessage], Any]
    ) -> None:
        """
        Starts listening for messages on the configured Pub/Sub subscription.
        This method is blocking and will run indefinitely.
        """
        subscription_path = SubscriberClient.subscription_path(project_id, subscription_name)

        with SubscriberClient() as client:
            logger.info(f"Listening for messages on {subscription_path}")
            streaming_pull_future = client.subscribe(
                subscription_path, await_callbacks_on_shutdown=True, callback=callback,
                flow_control=FlowControl(max_messages=control_flow_policy.max_messages, 
                                         max_bytes=control_flow_policy.max_bytes)
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
