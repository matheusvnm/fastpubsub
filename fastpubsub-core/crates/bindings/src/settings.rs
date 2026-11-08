//! The conversion of the Python subscription settings.

use fastpubsub_google_pubsub::{DeadLetter, RetryBackoff, SubscriptionSettings};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::error::IntoPythonResult;

/// Read one optional value from the settings dictionary.
///
/// A missing key or a `None` value gives `T::default()`.
///
/// # Errors
///
/// Returns a Python `TypeError` if the value has a different type, or an
/// `OverflowError` if a number does not fit in `T`.
fn read_setting<T>(settings: &Bound<'_, PyDict>, key: &str) -> PyResult<T>
where
    T: Default + for<'a, 'py> FromPyObject<'a, 'py, Error = PyErr>,
{
    match settings.get_item(key)? {
        Some(value) if !value.is_none() => value.extract(),
        _ => Ok(T::default()),
    }
}

/// Convert the Python `SubscriptionSettings` dictionary to the adapter
/// settings.
///
/// The retry policy is set only when `max_backoff_seconds` is more than 0.
/// The dead-letter policy is set only when `dead_letter_topic` is not
/// empty.
///
/// # Errors
///
/// Returns a Python error if a value has a wrong type, if a number does
/// not fit in its Pub/Sub field, or if the dead-letter topic path has a
/// wrong format.
pub(crate) fn read_subscription_settings(
    settings: &Bound<'_, PyDict>,
) -> PyResult<SubscriptionSettings> {
    let max_backoff_seconds: i64 = read_setting(settings, "max_backoff_seconds")?;
    let retry_backoff = if max_backoff_seconds > 0 {
        Some(RetryBackoff {
            minimum_seconds: read_setting(settings, "min_backoff_seconds")?,
            maximum_seconds: max_backoff_seconds,
        })
    } else {
        None
    };
    let dead_letter_topic: String = read_setting(settings, "dead_letter_topic")?;
    let dead_letter = if dead_letter_topic.is_empty() {
        None
    } else {
        Some(DeadLetter {
            topic: dead_letter_topic.parse().into_python_result()?,
            max_delivery_attempts: read_setting(settings, "max_delivery_attempts")?,
        })
    };
    Ok(SubscriptionSettings {
        filter: read_setting(settings, "filter")?,
        ack_deadline_seconds: read_setting(settings, "ack_deadline_seconds")?,
        enable_message_ordering: read_setting(settings, "enable_message_ordering")?,
        enable_exactly_once_delivery: read_setting(settings, "enable_exactly_once_delivery")?,
        retry_backoff,
        dead_letter,
    })
}
