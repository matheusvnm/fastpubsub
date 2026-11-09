//! The Python class `AdminClient`.

use fastpubsub_google_pubsub::{Administration, Connection, SubscriptionPath, TopicPath};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::error::IntoPythonResult;
use crate::runtime::block_on;
use crate::settings::read_subscription_settings;

/// Create, update and remove topics and subscriptions.
///
/// Each method blocks until Pub/Sub answers, but it does not hold the GIL.
#[pyclass(frozen)]
pub(crate) struct AdminClient {
    /// The administration clients of the adapter.
    administration: Administration,
}

#[pymethods]
impl AdminClient {
    /// Connect to Pub/Sub.
    ///
    /// `endpoint` is the URL of the Pub/Sub emulator, or `None` for the
    /// Pub/Sub service with the default credentials.
    ///
    /// Raises `CoreError` if the client cannot connect or cannot get the
    /// credentials.
    #[new]
    #[pyo3(signature = (endpoint=None))]
    fn new(python: Python<'_>, endpoint: Option<String>) -> PyResult<Self> {
        let connection = Connection::new(endpoint);
        let administration =
            block_on(python, Administration::connect(&connection))?.into_python_result()?;
        Ok(Self { administration })
    }

    /// Create a topic.
    ///
    /// Raises `ValueError` if the path has a wrong format,
    /// `AlreadyExistsError` if the topic exists, or `CoreError` for a
    /// different error.
    fn create_topic(&self, python: Python<'_>, topic_path: &str) -> PyResult<()> {
        let topic: TopicPath = topic_path.parse().into_python_result()?;
        block_on(python, self.administration.create_topic(&topic))?.into_python_result()
    }

    /// Remove a topic.
    ///
    /// Raises `ValueError` if the path has a wrong format, `NotFoundError`
    /// if the topic does not exist, or `CoreError` for a different error.
    fn delete_topic(&self, python: Python<'_>, topic_path: &str) -> PyResult<()> {
        let topic: TopicPath = topic_path.parse().into_python_result()?;
        block_on(python, self.administration.delete_topic(&topic))?.into_python_result()
    }

    /// Create a subscription on a topic with the given settings.
    ///
    /// Raises `ValueError` if a path has a wrong format,
    /// `AlreadyExistsError` if the subscription exists, `NotFoundError` if
    /// the topic does not exist, or `CoreError` for a different error.
    fn create_subscription(
        &self,
        python: Python<'_>,
        subscription_path: &str,
        topic_path: &str,
        settings: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let subscription: SubscriptionPath = subscription_path.parse().into_python_result()?;
        let topic: TopicPath = topic_path.parse().into_python_result()?;
        let settings = read_subscription_settings(settings)?;
        block_on(
            python,
            self.administration
                .create_subscription(&subscription, &topic, &settings),
        )?
        .into_python_result()
    }

    /// Change the settings of a subscription.
    ///
    /// `update_fields` holds the names of the Pub/Sub fields to change, for
    /// example `ack_deadline_seconds` or `retry_policy`.
    ///
    /// Raises `ValueError` if a path has a wrong format, `NotFoundError` if
    /// the subscription does not exist, or `CoreError` for a different
    /// error.
    fn update_subscription(
        &self,
        python: Python<'_>,
        subscription_path: &str,
        topic_path: &str,
        settings: &Bound<'_, PyDict>,
        update_fields: Vec<String>,
    ) -> PyResult<()> {
        let subscription: SubscriptionPath = subscription_path.parse().into_python_result()?;
        let topic: TopicPath = topic_path.parse().into_python_result()?;
        let settings = read_subscription_settings(settings)?;
        block_on(
            python,
            self.administration.update_subscription(
                &subscription,
                &topic,
                &settings,
                update_fields,
            ),
        )?
        .into_python_result()
    }

    /// Remove a subscription.
    ///
    /// Raises `ValueError` if the path has a wrong format, `NotFoundError`
    /// if the subscription does not exist, or `CoreError` for a different
    /// error.
    fn delete_subscription(&self, python: Python<'_>, subscription_path: &str) -> PyResult<()> {
        let subscription: SubscriptionPath = subscription_path.parse().into_python_result()?;
        block_on(
            python,
            self.administration.delete_subscription(&subscription),
        )?
        .into_python_result()
    }
}
