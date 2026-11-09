//! The administration of topics and subscriptions.

use fastpubsub_messaging::Result;
use google_cloud_pubsub::client::{SubscriptionAdmin, TopicAdmin};
use google_cloud_pubsub::model::{DeadLetterPolicy, RetryPolicy, Subscription};

use crate::error::from_rpc;
use crate::{Connection, SubscriptionPath, TopicPath};

/// The delays before Pub/Sub sends a nacked message again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryBackoff {
    /// The minimum delay, in seconds.
    pub minimum_seconds: i64,
    /// The maximum delay, in seconds.
    pub maximum_seconds: i64,
}

/// The topic that receives the messages that fail too many times.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeadLetter {
    /// The dead-letter topic.
    pub topic: TopicPath,
    /// The number of delivery attempts before Pub/Sub sends a message to
    /// the dead-letter topic.
    pub max_delivery_attempts: i32,
}

/// The settings of a subscription.
///
/// A value of the default settings uses the default values of the Pub/Sub
/// service.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SubscriptionSettings {
    /// The filter expression for the message attributes. An empty text
    /// receives all messages.
    pub filter: String,
    /// The time that a subscriber has to ack a message, in seconds. A value
    /// of 0 uses the default of the service.
    pub ack_deadline_seconds: i32,
    /// Deliver the messages with the same ordering key in the publish order.
    pub enable_message_ordering: bool,
    /// Deliver each message one time only.
    pub enable_exactly_once_delivery: bool,
    /// The retry delays, or `None` to retry at once.
    pub retry_backoff: Option<RetryBackoff>,
    /// The dead-letter policy, or `None` for no dead-letter topic.
    pub dead_letter: Option<DeadLetter>,
}

impl SubscriptionSettings {
    /// Build the Pub/Sub subscription model for a subscription on a topic.
    fn to_model(&self, subscription: &SubscriptionPath, topic: &TopicPath) -> Subscription {
        let mut model = Subscription::new()
            .set_name(subscription.as_str())
            .set_topic(topic.as_str())
            .set_filter(self.filter.as_str())
            .set_ack_deadline_seconds(self.ack_deadline_seconds)
            .set_enable_message_ordering(self.enable_message_ordering)
            .set_enable_exactly_once_delivery(self.enable_exactly_once_delivery);
        if let Some(backoff) = self.retry_backoff {
            model = model.set_retry_policy(
                RetryPolicy::new()
                    .set_minimum_backoff(wkt::Duration::clamp(backoff.minimum_seconds, 0))
                    .set_maximum_backoff(wkt::Duration::clamp(backoff.maximum_seconds, 0)),
            );
        }
        if let Some(dead_letter) = &self.dead_letter {
            model = model.set_dead_letter_policy(
                DeadLetterPolicy::new()
                    .set_dead_letter_topic(dead_letter.topic.as_str())
                    .set_max_delivery_attempts(dead_letter.max_delivery_attempts),
            );
        }
        model
    }
}

/// Create, update and remove topics and subscriptions.
#[derive(Debug)]
pub struct Administration {
    /// The topic administration client.
    topics: TopicAdmin,
    /// The subscription administration client.
    subscriptions: SubscriptionAdmin,
}

impl Administration {
    /// Build the administration clients.
    ///
    /// # Errors
    ///
    /// Returns an error if a client cannot get the credentials or cannot
    /// make the transport.
    pub async fn connect(connection: &Connection) -> Result<Self> {
        Ok(Self {
            topics: connection.topic_admin().await?,
            subscriptions: connection.subscription_admin().await?,
        })
    }

    /// Create a topic.
    ///
    /// # Errors
    ///
    /// Returns an error of the kind `AlreadyExists` if the topic exists, or
    /// of the kind `Other` for each other failure.
    pub async fn create_topic(&self, topic: &TopicPath) -> Result<()> {
        self.topics
            .create_topic()
            .set_name(topic.as_str())
            .send()
            .await
            .map(|_| ())
            .map_err(|error| from_rpc(&error))
    }

    /// Remove a topic.
    ///
    /// # Errors
    ///
    /// Returns an error of the kind `NotFound` if the topic does not exist,
    /// or of the kind `Other` for each other failure.
    pub async fn delete_topic(&self, topic: &TopicPath) -> Result<()> {
        self.topics
            .delete_topic()
            .set_topic(topic.as_str())
            .send()
            .await
            .map_err(|error| from_rpc(&error))
    }

    /// Create a subscription on a topic.
    ///
    /// # Errors
    ///
    /// Returns an error of the kind `AlreadyExists` if the subscription
    /// exists, `NotFound` if the topic does not exist, or `Other` for each
    /// other failure.
    pub async fn create_subscription(
        &self,
        subscription: &SubscriptionPath,
        topic: &TopicPath,
        settings: &SubscriptionSettings,
    ) -> Result<()> {
        self.subscriptions
            .create_subscription()
            .with_request(settings.to_model(subscription, topic))
            .send()
            .await
            .map(|_| ())
            .map_err(|error| from_rpc(&error))
    }

    /// Change the settings of a subscription.
    ///
    /// `update_fields` holds the names of the Pub/Sub fields to change, for
    /// example `ack_deadline_seconds` or `retry_policy`.
    ///
    /// # Errors
    ///
    /// Returns an error of the kind `NotFound` if the subscription does not
    /// exist, or `Other` for each other failure.
    pub async fn update_subscription(
        &self,
        subscription: &SubscriptionPath,
        topic: &TopicPath,
        settings: &SubscriptionSettings,
        update_fields: Vec<String>,
    ) -> Result<()> {
        self.subscriptions
            .update_subscription()
            .set_subscription(settings.to_model(subscription, topic))
            .set_update_mask(wkt::FieldMask::default().set_paths(update_fields))
            .send()
            .await
            .map(|_| ())
            .map_err(|error| from_rpc(&error))
    }

    /// Remove a subscription.
    ///
    /// # Errors
    ///
    /// Returns an error of the kind `NotFound` if the subscription does not
    /// exist, or `Other` for each other failure.
    pub async fn delete_subscription(&self, subscription: &SubscriptionPath) -> Result<()> {
        self.subscriptions
            .delete_subscription()
            .set_subscription(subscription.as_str())
            .send()
            .await
            .map_err(|error| from_rpc(&error))
    }
}

#[cfg(test)]
mod tests {
    use super::{DeadLetter, RetryBackoff, SubscriptionSettings};
    use crate::{SubscriptionPath, TopicPath};

    fn paths() -> (SubscriptionPath, TopicPath) {
        (
            "projects/demo/subscriptions/orders".parse().unwrap(),
            "projects/demo/topics/orders".parse().unwrap(),
        )
    }

    #[test]
    fn default_settings_have_no_retry_and_no_dead_letter_policy() {
        let (subscription, topic) = paths();

        let model = SubscriptionSettings::default().to_model(&subscription, &topic);

        assert_eq!(model.name, "projects/demo/subscriptions/orders");
        assert_eq!(model.topic, "projects/demo/topics/orders");
        assert!(model.retry_policy.is_none());
        assert!(model.dead_letter_policy.is_none());
    }

    #[test]
    fn settings_map_to_the_subscription_fields() {
        let (subscription, topic) = paths();
        let settings = SubscriptionSettings {
            filter: "attributes.type = \"order\"".to_owned(),
            ack_deadline_seconds: 60,
            enable_message_ordering: true,
            enable_exactly_once_delivery: true,
            retry_backoff: Some(RetryBackoff {
                minimum_seconds: 10,
                maximum_seconds: 600,
            }),
            dead_letter: Some(DeadLetter {
                topic: "projects/demo/topics/orders-dead-letter".parse().unwrap(),
                max_delivery_attempts: 5,
            }),
        };

        let model = settings.to_model(&subscription, &topic);

        assert_eq!(model.filter, "attributes.type = \"order\"");
        assert_eq!(model.ack_deadline_seconds, 60);
        assert!(model.enable_message_ordering);
        assert!(model.enable_exactly_once_delivery);
        let retry_policy = model.retry_policy.unwrap();
        assert_eq!(retry_policy.minimum_backoff.unwrap().seconds(), 10);
        assert_eq!(retry_policy.maximum_backoff.unwrap().seconds(), 600);
        let dead_letter_policy = model.dead_letter_policy.unwrap();
        assert_eq!(
            dead_letter_policy.dead_letter_topic,
            "projects/demo/topics/orders-dead-letter"
        );
        assert_eq!(dead_letter_policy.max_delivery_attempts, 5);
    }
}
