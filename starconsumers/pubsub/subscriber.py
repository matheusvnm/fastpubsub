from dataclasses import dataclass
from starconsumers.datastructures import MessageMiddleware, TopicSubscription
from starconsumers.types import DecoratedCallable
from google.api_core.exceptions import AlreadyExists, GoogleAPICallError
from google.cloud.pubsub_v1 import SubscriberClient
from google.pubsub_v1.types import DeadLetterPolicy, RetryPolicy, Subscription




class PubSubSubscriber:

    def create_subscription(self, subscription: TopicSubscription):
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
                max_delivery_attempts=subscription.dead_letter_policy.delivery_attempts,
            )

        subscription = Subscription(
                name=name,
                topic=topic,
                retry_policy=RetryPolicy(),
                dead_letter_policy=dlt_policy,
                filter=subscription.filter_expression,
                ack_deadline_seconds=subscription.ack_deadline_seconds,
                enable_message_ordering=subscription.enable_message_ordering,
                enable_exactly_once_delivery=subscription.enable_exactly_once_delivery,
            )


        try:
            print(f"Attempting to create subscription: {subscription.name}")
            client = SubscriberClient()
            client.create_subscription(request=subscription)
            print(f"Successfully created subscription: {subscription.name}")
        except AlreadyExists:
            print(f"Subscription '{subscription.name}' already exists. Skipping creation.")
        except GoogleAPICallError as e:
            print(f"Failed to create subscription '{subscription.name}': {e}")
            raise
        except Exception as e:
            print(f"An unexpected error occurred during subscription creation: {e}")
            raise


    def subscribe(self, project_id: str, subscription_name: str, callback: MessageMiddleware):
        """
        Starts listening for messages on the configured Pub/Sub subscription.
        This method is blocking and will run indefinitely.
        """
        subscription_path = SubscriberClient.subscription_path(project_id, subscription_name)

        client = SubscriberClient()
        with client:
            print(f"Listening for messages on {subscription_path}")
            streaming_pull_future = client.subscribe(subscription_path, callback=callback)
            try:
                streaming_pull_future.result()
            except KeyboardInterrupt:
                print("Subscriber stopped by user")
            except Exception as e:
                print(f"Subscription stream terminated unexpectedly: {e}")
            finally:
                print("Sending cancel streaming pull command.")
                streaming_pull_future.cancel()
                print("Waiting the cancel command to finish.")
                streaming_pull_future.result()
                print("Subscriber has shut down.")
