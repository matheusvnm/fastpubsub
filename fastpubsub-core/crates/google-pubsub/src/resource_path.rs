//! Validated paths of topics and subscriptions.

use std::fmt;
use std::str::FromStr;

use fastpubsub_messaging::{Error, ErrorKind};

/// The full path of a topic, in the format `projects/<project>/topics/<name>`.
///
/// The path is checked one time, when it is parsed. The other functions of
/// the adapter accept only this type.
///
/// # Examples
///
/// ```
/// use fastpubsub_google_pubsub::TopicPath;
///
/// let topic: TopicPath = "projects/demo/topics/orders".parse().unwrap();
/// assert_eq!(topic.as_str(), "projects/demo/topics/orders");
/// assert!("projects/demo/subscriptions/orders".parse::<TopicPath>().is_err());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPath(String);

/// The full path of a subscription, in the format
/// `projects/<project>/subscriptions/<name>`.
///
/// The path is checked one time, when it is parsed. The other functions of
/// the adapter accept only this type.
///
/// # Examples
///
/// ```
/// use fastpubsub_google_pubsub::SubscriptionPath;
///
/// let subscription: SubscriptionPath = "projects/demo/subscriptions/orders".parse().unwrap();
/// assert_eq!(subscription.as_str(), "projects/demo/subscriptions/orders");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionPath(String);

/// Check that `text` has the format `projects/<project>/<collection>/<name>`.
///
/// # Errors
///
/// Returns an error of the kind [`ErrorKind::InvalidArgument`] when the
/// format is wrong or when the project or the name is empty.
fn check_path(text: &str, collection: &str) -> Result<(), Error> {
    let mut segments = text.split('/');
    let is_valid = matches!(
        (
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
            segments.next(),
        ),
        (Some("projects"), Some(project), Some(found), Some(name), None)
            if !project.is_empty() && found == collection && !name.is_empty()
    );
    if is_valid {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::InvalidArgument,
            format!(
                "the path \"{text}\" must have the format projects/<project>/{collection}/<name>"
            ),
        ))
    }
}

impl TopicPath {
    /// Get the path as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for TopicPath {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        check_path(text, "topics")?;
        Ok(Self(text.to_owned()))
    }
}

impl fmt::Display for TopicPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl SubscriptionPath {
    /// Get the path as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for SubscriptionPath {
    type Err = Error;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        check_path(text, "subscriptions")?;
        Ok(Self(text.to_owned()))
    }
}

impl fmt::Display for SubscriptionPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use fastpubsub_messaging::ErrorKind;

    use super::{SubscriptionPath, TopicPath};

    #[test]
    fn parses_a_valid_topic_path() {
        let topic: TopicPath = "projects/demo/topics/orders".parse().unwrap();

        assert_eq!(topic.to_string(), "projects/demo/topics/orders");
    }

    #[test]
    fn parses_a_valid_subscription_path() {
        let subscription: SubscriptionPath =
            "projects/demo/subscriptions/orders.v1".parse().unwrap();

        assert_eq!(
            subscription.as_str(),
            "projects/demo/subscriptions/orders.v1"
        );
    }

    #[test]
    fn rejects_a_path_of_the_other_collection() {
        let error = "projects/demo/subscriptions/orders"
            .parse::<TopicPath>()
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }

    #[test]
    fn rejects_paths_with_missing_empty_or_extra_segments() {
        for text in [
            "",
            "orders",
            "projects/demo/topics",
            "projects//topics/orders",
            "projects/demo/topics/",
            "projects/demo/topics/orders/extra",
            "folders/demo/topics/orders",
        ] {
            assert!(text.parse::<TopicPath>().is_err(), "{text} was accepted");
        }
    }
}
