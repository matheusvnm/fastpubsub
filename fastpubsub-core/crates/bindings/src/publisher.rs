//! The Python class `Publisher`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use fastpubsub_google_pubsub::{Connection, Publisher as TopicPublisher, TopicPath};
use fastpubsub_messaging::{OutgoingMessage, Result};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use pyo3::prelude::*;
use tokio::sync::mpsc;

use crate::bridge::{Bridge, BridgeItem};
use crate::error::{CoreError, IntoPythonResult};
use crate::runtime::{block_on, runtime};

/// A publish that waits for its result.
type PendingPublish = Pin<Box<dyn Future<Output = Result<String>> + Send>>;

/// A pending publish and the Python future that waits for its result.
type PublishRequest = (PendingPublish, Py<PyAny>);

/// Wait for the publish results and send them to the bridge.
///
/// The task receives each new publish from `requests`. It waits for all
/// pending publishes at the same time, and sends each result to the bridge
/// as soon as it is ready. When the publisher is gone, the task waits for
/// the last results and stops. When the bridge is gone, the task stops at
/// once.
async fn forward_publish_outcomes(
    mut requests: mpsc::UnboundedReceiver<PublishRequest>,
    bridge: mpsc::UnboundedSender<BridgeItem>,
    resolve_batch: Arc<Py<PyAny>>,
) {
    let mut pending = FuturesUnordered::new();
    loop {
        tokio::select! {
            request = requests.recv() => match request {
                Some((publish, future)) => {
                    pending.push(async move { (future, publish.await) });
                }
                None => break,
            },
            Some((future, result)) = pending.next(), if !pending.is_empty() => {
                if bridge.send(BridgeItem::Publish(Arc::clone(&resolve_batch), future, result)).is_err() {
                    return;
                }
            }
        }
    }
    while let Some((future, result)) = pending.next().await {
        if bridge
            .send(BridgeItem::Publish(
                Arc::clone(&resolve_batch),
                future,
                result,
            ))
            .is_err()
        {
            return;
        }
    }
}

/// Publish messages to one Pub/Sub topic.
///
/// The client collects the messages in batches before it sends them. The
/// result of each publish arrives at `resolve_batch` through the `Bridge`,
/// as a `(future, is_success, value)` tuple. `value` is the message ID for
/// a success, or an exception for an error.
#[pyclass(frozen)]
pub(crate) struct Publisher {
    /// The publisher of the adapter.
    inner: TopicPublisher,
    /// The channel to the task that runs [`forward_publish_outcomes`].
    requests: mpsc::UnboundedSender<PublishRequest>,
}

#[pymethods]
impl Publisher {
    /// Connect to Pub/Sub.
    ///
    /// `topic_path` is the full path, in the format
    /// `projects/<project>/topics/<name>`. `bridge` is the bridge of the
    /// event loop that runs `resolve_batch`. `resolve_batch` receives each
    /// batch of publish results. `endpoint` is the URL of the Pub/Sub
    /// emulator, or `None` for the Pub/Sub service with the default
    /// credentials.
    ///
    /// Raises `ValueError` if the path has a wrong format. Raises
    /// `CoreError` if the client cannot connect or cannot get the
    /// credentials.
    #[new]
    #[pyo3(signature = (topic_path, bridge, resolve_batch, endpoint=None))]
    fn new(
        python: Python<'_>,
        topic_path: &str,
        bridge: &Bound<'_, Bridge>,
        resolve_batch: Py<PyAny>,
        endpoint: Option<String>,
    ) -> PyResult<Self> {
        let topic: TopicPath = topic_path.parse().into_python_result()?;
        let connection = Connection::new(endpoint);
        let inner =
            block_on(python, TopicPublisher::connect(&connection, &topic))?.into_python_result()?;
        let (requests, request_receiver) = mpsc::unbounded_channel();
        runtime()?.spawn(forward_publish_outcomes(
            request_receiver,
            bridge.get().sender(),
            Arc::new(resolve_batch),
        ));
        Ok(Self { inner, requests })
    }

    /// Add one message to the next batch.
    ///
    /// The call does not block. The message ID or the error arrives later
    /// in `future`, through `resolve_batch`. `ordering_key` is `None` or an
    /// empty string to publish without a key.
    ///
    /// Raises `CoreError` if the publisher is closed or if the tokio
    /// runtime did not start.
    #[pyo3(signature = (data, attributes, future, ordering_key=None))]
    fn publish(
        &self,
        data: Vec<u8>,
        attributes: HashMap<String, String>,
        future: Py<PyAny>,
        ordering_key: Option<String>,
    ) -> PyResult<()> {
        let message = OutgoingMessage {
            data: data.into(),
            attributes,
            ordering_key: ordering_key.filter(|key| !key.is_empty()),
        };
        let _guard = runtime()?.enter();
        let publish: PendingPublish = Box::pin(self.inner.publish(message));
        self.requests
            .send((publish, future))
            .map_err(|_| CoreError::new_err("the publisher is closed"))
    }

    /// Send all pending batches and wait for the results.
    ///
    /// The call blocks, but it does not hold the GIL.
    ///
    /// Raises `CoreError` if the tokio runtime did not start.
    fn flush(&self, python: Python<'_>) -> PyResult<()> {
        block_on(python, self.inner.flush())
    }
}
