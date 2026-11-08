//! The Python class `Subscription`.

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use fastpubsub_google_pubsub::{Connection, StreamingPull, SubscriptionPath};
use fastpubsub_messaging::FlowControl;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::bridge::{Bridge, BridgeItem};
use crate::error::IntoPythonResult;
use crate::runtime::block_on;

/// One streaming pull on a Pub/Sub subscription.
///
/// The stream starts in the constructor and runs on the tokio runtime. It
/// sends each received message to `dispatch` through the `Bridge`, as a
/// `(message_id, data, attributes, delivery_attempt, ack_handle)` tuple.
/// The handler must call `ack_handle.ack()` or `ack_handle.nack()` for each
/// message.
#[pyclass(frozen)]
pub(crate) struct Subscription {
    /// The stream of the adapter.
    stream: StreamingPull,
}

#[pymethods]
impl Subscription {
    /// Connect to Pub/Sub and start the stream.
    ///
    /// `subscription_path` is the full path, in the format
    /// `projects/<project>/subscriptions/<name>`. `max_messages` is the
    /// maximum number of handlers that the client runs at the same time.
    /// The server gets twice this limit, so the next messages are ready
    /// when a handler stops. `bridge` is the bridge of the event loop that
    /// runs `dispatch`. `dispatch` receives each batch of messages.
    /// `endpoint` is the URL of the Pub/Sub emulator, or `None` for the
    /// Pub/Sub service with the default credentials.
    ///
    /// Raises `ValueError` if the path has a wrong format or if
    /// `max_messages` is 0. Raises `CoreError` if the client cannot connect
    /// or cannot get the credentials.
    #[new]
    #[pyo3(signature = (subscription_path, max_messages, bridge, dispatch, endpoint=None))]
    fn new(
        python: Python<'_>,
        subscription_path: &str,
        max_messages: u32,
        bridge: &Bound<'_, Bridge>,
        dispatch: Py<PyAny>,
        endpoint: Option<String>,
    ) -> PyResult<Self> {
        let subscription: SubscriptionPath = subscription_path.parse().into_python_result()?;
        let max_messages = NonZeroU32::new(max_messages)
            .ok_or_else(|| PyValueError::new_err("max_messages must be at least 1"))?;
        let sender = bridge.get().sender();
        let dispatch = Arc::new(dispatch);
        let connection = Connection::new(endpoint);
        let stream = block_on(
            python,
            StreamingPull::start(
                &connection,
                &subscription,
                FlowControl::new(max_messages),
                move |message| {
                    sender
                        .send(BridgeItem::Message(Arc::clone(&dispatch), message))
                        .is_ok()
                },
            ),
        )?
        .into_python_result()?;
        Ok(Self { stream })
    }

    /// Return true until the stream stops because of an error or a
    /// shutdown.
    fn is_running(&self) -> bool {
        self.stream.is_running()
    }

    /// Stop the stream and wait for the pending acks.
    ///
    /// `timeout_seconds` is the maximum time to wait. A negative or
    /// non-finite value does not wait. The call blocks, but it does not
    /// hold the GIL. Returns true if the stream stopped before the timeout.
    ///
    /// Raises `CoreError` if the tokio runtime did not start.
    fn shutdown(&self, python: Python<'_>, timeout_seconds: f64) -> PyResult<bool> {
        let timeout = Duration::try_from_secs_f64(timeout_seconds).unwrap_or(Duration::ZERO);
        block_on(python, self.stream.shutdown(timeout))
    }
}
