//! The compiled Pub/Sub transport of `FastPubSub`.
//!
//! This crate builds the Python extension module `fastpubsub_core`. The
//! `fastpubsub` package is the only expected user. The names can change in
//! each release of `fastpubsub-core`.
//!
//! The module has these parts:
//!
//! - A shared tokio runtime with two worker threads. It runs all network
//!   I/O: the streaming pulls, the publish batches and the admin calls.
//! - `Bridge`: one OS thread for each asyncio event loop. It collects the
//!   received messages and the publish results, and sends them to the event
//!   loop in batches. It holds the GIL one time for each batch.
//! - `Subscription`: one streaming pull on a subscription.
//! - `Publisher`: a batching publisher for one topic.
//! - `AdminClient`: topic and subscription administration.
//! - `AckHandle`: the acknowledgement of one received message.
//!
//! The classes are frozen and the module declares `gil_used = false`, so
//! the module works on free-threaded Python.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentials;
use google_cloud_gax::error::rpc::Code;
use google_cloud_pubsub::client::{
    Publisher as PubSubPublisher, Subscriber as PubSubSubscriber, SubscriptionAdmin, TopicAdmin,
};
use google_cloud_pubsub::error::PublishError;
use google_cloud_pubsub::model::{
    DeadLetterPolicy, Message as PubSubMessage, RetryPolicy, Subscription as SubscriptionModel,
};
use google_cloud_pubsub::publisher::PublishFuture;
use google_cloud_pubsub::subscriber::ShutdownToken;
use google_cloud_pubsub::subscriber::handler::Handler;
use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

create_exception!(
    fastpubsub_core,
    CoreError,
    PyRuntimeError,
    "An error of the Pub/Sub transport."
);
create_exception!(
    fastpubsub_core,
    NotFoundError,
    CoreError,
    "The topic or the subscription does not exist."
);
create_exception!(
    fastpubsub_core,
    AlreadyExistsError,
    CoreError,
    "The topic or the subscription exists already."
);

/// The number of tokio worker threads in the shared runtime.
const WORKER_THREADS: usize = 2;

/// The shared tokio runtime of the process.
///
/// The runtime starts at the first use. It holds the error if it cannot
/// start, so each caller gets a Python exception and not a panic.
static RUNTIME: LazyLock<std::io::Result<Runtime>> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS)
        .thread_name("fastpubsub-core")
        .enable_all()
        .build()
});

/// Get the shared tokio runtime.
///
/// # Errors
///
/// Returns a [`CoreError`] if the runtime did not start.
fn runtime() -> PyResult<&'static Runtime> {
    RUNTIME
        .as_ref()
        .map_err(|error| CoreError::new_err(format!("the tokio runtime did not start: {error}")))
}

/// Convert a Pub/Sub client error to a Python exception.
///
/// The gRPC status code selects the exception class: `NOT_FOUND` gives a
/// [`NotFoundError`], `ALREADY_EXISTS` gives an [`AlreadyExistsError`], and
/// each other code gives a [`CoreError`].
fn to_python_error(error: &google_cloud_pubsub::Error) -> PyErr {
    let text = error.to_string();
    match error.status().map(|status| status.code) {
        Some(Code::NotFound) => NotFoundError::new_err(text),
        Some(Code::AlreadyExists) => AlreadyExistsError::new_err(text),
        _ => CoreError::new_err(text),
    }
}

/// Convert a publish error to a Python exception.
///
/// An RPC error goes through [`to_python_error`]. Each other publish error
/// gives a [`CoreError`].
fn publish_error_to_python(error: &PublishError) -> PyErr {
    match error {
        PublishError::Rpc(inner) => to_python_error(inner),
        other => CoreError::new_err(other.to_string()),
    }
}

/// Apply the emulator endpoint to a client builder.
///
/// When `endpoint` has a value, the function calls `configure` with the
/// builder and the endpoint. The caller sets the endpoint and anonymous
/// credentials there. When `endpoint` is `None`, the builder does not
/// change and the client uses the Pub/Sub service with the default
/// credentials.
fn anonymous_endpoint<B>(
    builder: B,
    endpoint: Option<String>,
    configure: impl FnOnce(B, String) -> B,
) -> B {
    match endpoint {
        Some(endpoint) => configure(builder, endpoint),
        None => builder,
    }
}

/// Convert a client build error to a Python exception.
fn build_error_to_python(error: &impl std::fmt::Display) -> PyErr {
    CoreError::new_err(error.to_string())
}

/// The acknowledgement of one received message.
///
/// The first call to `ack` or `nack` has an effect. The next calls do
/// nothing. The methods do not block and do not need the GIL.
#[pyclass(frozen)]
struct AckHandle {
    /// The handler of the Pub/Sub client. It is `None` after the first ack
    /// or nack.
    handler: Mutex<Option<Handler>>,
}

impl AckHandle {
    /// Take the handler out of the handle.
    ///
    /// Returns `None` if a different call took it before. A poisoned lock
    /// does not stop the call, because the handler stays valid.
    fn take_handler(&self) -> Option<Handler> {
        self.handler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

#[pymethods]
impl AckHandle {
    /// Acknowledge the message, so Pub/Sub does not send it again.
    fn ack(&self) {
        if let Some(handler) = self.take_handler() {
            handler.ack();
        }
    }

    /// Reject the message, so Pub/Sub sends it again soon.
    fn nack(&self) {
        if let Some(handler) = self.take_handler() {
            handler.nack();
        }
    }
}

/// A message from the streaming pull, with the handler that acks it.
type ReceivedMessage = (PubSubMessage, Handler);

/// A Python future and the result of the publish that it waits for.
type PublishOutcome = (Py<PyAny>, Result<String, PublishError>);

/// One item that the bridge thread sends to the event loop.
///
/// Each variant holds the Python callback that receives the item. The
/// bridge groups the items of one batch by callback.
enum BridgeItem {
    /// A received message for the `dispatch` callback of a subscription.
    Message(Arc<Py<PyAny>>, ReceivedMessage),
    /// A publish result for the `resolve_batch` callback of a publisher.
    Publish(Arc<Py<PyAny>>, PublishOutcome),
}

/// Convert a received message to the Python tuple that `dispatch` gets.
///
/// The tuple is `(message_id, data, attributes, delivery_attempt,
/// ack_handle)`. The delivery attempt is 0 when the subscription has no
/// dead-letter policy.
///
/// # Errors
///
/// Returns the Python error if Python cannot make an object.
fn build_message_entry(
    python: Python<'_>,
    message: PubSubMessage,
    handler: Handler,
) -> PyResult<Bound<'_, PyAny>> {
    let delivery_attempt = handler.delivery_attempt().unwrap_or(0);
    let attributes = PyDict::new(python);
    for (key, value) in &message.attributes {
        attributes.set_item(key, value)?;
    }
    let ack_handle = AckHandle {
        handler: Mutex::new(Some(handler)),
    };
    let entry = PyTuple::new(
        python,
        [
            message.message_id.into_pyobject(python)?.into_any(),
            PyBytes::new(python, &message.data).into_any(),
            attributes.into_any(),
            delivery_attempt.into_pyobject(python)?.into_any(),
            Bound::new(python, ack_handle)?.into_any(),
        ],
    )?;
    Ok(entry.into_any())
}

/// Convert a publish result to the Python tuple that `resolve_batch` gets.
///
/// The tuple is `(future, True, message_id)` for a success, or
/// `(future, False, exception)` for an error.
///
/// # Errors
///
/// Returns the Python error if Python cannot make an object.
fn build_publish_entry(
    python: Python<'_>,
    future: Py<PyAny>,
    result: Result<String, PublishError>,
) -> PyResult<Bound<'_, PyAny>> {
    match result {
        Ok(message_id) => Ok((future, true, message_id).into_pyobject(python)?.into_any()),
        Err(error) => {
            let exception = publish_error_to_python(&error).into_value(python);
            Ok((future, false, exception).into_pyobject(python)?.into_any())
        }
    }
}

/// Send one batch of items to the event loop.
///
/// The function holds the GIL one time. It groups the items by callback
/// and calls `call_soon_threadsafe(run_callbacks, calls)`, where `calls`
/// is a list of `(callback, entries)` pairs.
///
/// # Errors
///
/// Returns the Python error if an entry cannot be made or if
/// `call_soon_threadsafe` fails, for example because the loop is closed.
fn send_batch(
    batch: Vec<BridgeItem>,
    call_soon_threadsafe: &Py<PyAny>,
    run_callbacks: &Py<PyAny>,
) -> PyResult<()> {
    Python::attach(|python| {
        let mut groups: Vec<(Arc<Py<PyAny>>, Bound<'_, PyList>)> = Vec::new();
        for item in batch {
            let (callback, entry) = match item {
                BridgeItem::Message(callback, (message, handler)) => {
                    (callback, build_message_entry(python, message, handler)?)
                }
                BridgeItem::Publish(callback, (future, outcome)) => {
                    (callback, build_publish_entry(python, future, outcome)?)
                }
            };
            if let Some((_, entries)) = groups
                .iter()
                .find(|(existing, _)| Arc::ptr_eq(existing, &callback))
            {
                entries.append(entry)?;
            } else {
                let entries = PyList::empty(python);
                entries.append(entry)?;
                groups.push((callback, entries));
            }
        }
        let calls = PyList::empty(python);
        for (callback, entries) in groups {
            calls.append((callback.bind(python), entries))?;
        }
        call_soon_threadsafe.call1(python, (run_callbacks.bind(python), calls))?;
        Ok(())
    })
}

/// Run the bridge thread until all senders are dropped.
///
/// The thread waits for the first item. Then it takes the items that are
/// ready, up to `max_batch_size`, and sends them as one batch with
/// [`send_batch`]. An error stops only that batch, and the thread writes
/// it to the standard error.
fn run_bridge(
    receiver: &mut mpsc::UnboundedReceiver<BridgeItem>,
    max_batch_size: usize,
    call_soon_threadsafe: &Py<PyAny>,
    run_callbacks: &Py<PyAny>,
) {
    while let Some(first) = receiver.blocking_recv() {
        let mut batch = Vec::with_capacity(max_batch_size);
        batch.push(first);
        while batch.len() < max_batch_size {
            match receiver.try_recv() {
                Ok(item) => batch.push(item),
                Err(_) => break,
            }
        }
        if let Err(error) = send_batch(batch, call_soon_threadsafe, run_callbacks) {
            eprintln!("fastpubsub-core: the bridge callback failed: {error}");
        }
    }
}

/// Send the results of the transport to one asyncio event loop.
///
/// A `Bridge` starts one thread with the name `fastpubsub-bridge`. The
/// thread collects the received messages and the publish results in
/// batches of at most `max_batch_size` items. For each batch, it holds the
/// GIL one time and calls `call_soon_threadsafe(run_callbacks, batch)`.
///
/// Create one `Bridge` for each event loop. The `Subscription` and
/// `Publisher` objects of the same loop share it. The thread stops when
/// the `Bridge` and all its users are gone.
#[pyclass(frozen)]
struct Bridge {
    /// The channel to the bridge thread.
    sender: mpsc::UnboundedSender<BridgeItem>,
}

#[pymethods]
impl Bridge {
    /// Start the bridge thread.
    ///
    /// `max_batch_size` is the maximum number of items in one call to the
    /// event loop. A value of 0 is changed to 1. `call_soon_threadsafe` is
    /// the method of the target event loop. `run_callbacks` is the function
    /// that the event loop calls with each batch.
    ///
    /// Raises `CoreError` if the operating system cannot start the thread.
    #[new]
    fn new(
        max_batch_size: usize,
        call_soon_threadsafe: Py<PyAny>,
        run_callbacks: Py<PyAny>,
    ) -> PyResult<Self> {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let max_batch_size = max_batch_size.max(1);
        std::thread::Builder::new()
            .name("fastpubsub-bridge".into())
            .spawn(move || {
                run_bridge(
                    &mut receiver,
                    max_batch_size,
                    &call_soon_threadsafe,
                    &run_callbacks,
                );
            })
            .map_err(|error| CoreError::new_err(error.to_string()))?;
        Ok(Self { sender })
    }
}

/// One streaming pull on a Pub/Sub subscription.
///
/// The stream starts in the constructor and runs on the tokio runtime. It
/// sends each received message to `dispatch` through the `Bridge`, as a
/// `(message_id, data, attributes, delivery_attempt, ack_handle)` tuple.
/// The handler must call `ack_handle.ack()` or `ack_handle.nack()` for each
/// message.
#[pyclass(frozen)]
struct Subscription {
    /// The tokio task that reads the stream.
    tasks: Mutex<Vec<JoinHandle<()>>>,
    /// The token that stops the stream and waits for the pending acks.
    shutdown_token: ShutdownToken,
    /// True until the stream stops.
    is_running: Arc<AtomicBool>,
}

#[pymethods]
impl Subscription {
    /// Connect to Pub/Sub and start the stream.
    ///
    /// `subscription_path` is the full path, in the format
    /// `projects/<project>/subscriptions/<name>`.
    /// `max_outstanding_messages` is the maximum number of messages that
    /// the server sends before the client acks them. `bridge` is the bridge
    /// of the event loop that runs `dispatch`. `dispatch` receives each
    /// batch of messages. `endpoint` is the URL of the Pub/Sub emulator, or
    /// `None` for the Pub/Sub service with the default credentials.
    ///
    /// Raises `CoreError` if the client cannot connect or cannot get the
    /// credentials.
    #[new]
    #[pyo3(signature = (subscription_path, max_outstanding_messages, bridge, dispatch, endpoint=None))]
    fn new(
        python: Python<'_>,
        subscription_path: String,
        max_outstanding_messages: i64,
        bridge: &Bound<'_, Bridge>,
        dispatch: Py<PyAny>,
        endpoint: Option<String>,
    ) -> PyResult<Self> {
        let runtime = runtime()?;
        let sender = bridge.get().sender.clone();
        let subscriber = python.detach(|| {
            runtime.block_on(async move {
                anonymous_endpoint(
                    PubSubSubscriber::builder(),
                    endpoint,
                    |builder, endpoint| {
                        builder
                            .with_endpoint(endpoint)
                            .with_credentials(AnonymousCredentials::new().build())
                    },
                )
                .build()
                .await
                .map_err(|error| build_error_to_python(&error))
            })
        })?;
        let _guard = runtime.enter();
        let mut stream = subscriber
            .subscribe(subscription_path)
            .set_max_outstanding_messages(max_outstanding_messages)
            .build();
        let shutdown_token = stream.shutdown_token();
        let is_running = Arc::new(AtomicBool::new(true));
        let dispatch = Arc::new(dispatch);

        let reader_is_running = is_running.clone();
        let reader = runtime.spawn(async move {
            while let Some(item) = stream.next().await {
                match item {
                    Ok(received) => {
                        if sender
                            .send(BridgeItem::Message(dispatch.clone(), received))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(error) => {
                        eprintln!("fastpubsub-core: the streaming pull failed: {error}");
                        break;
                    }
                }
            }
            reader_is_running.store(false, Ordering::SeqCst);
        });
        Ok(Self {
            tasks: Mutex::new(vec![reader]),
            shutdown_token,
            is_running,
        })
    }

    /// Return true until the stream stops because of an error or a
    /// shutdown.
    fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Stop the stream and wait for the pending acks.
    ///
    /// `timeout_seconds` is the maximum time to wait. A negative or
    /// non-finite value does not wait. The call blocks, but it does not
    /// hold the GIL. Returns true if the stream stopped before the timeout.
    ///
    /// Raises `CoreError` if the tokio runtime did not start.
    fn shutdown(&self, python: Python<'_>, timeout_seconds: f64) -> PyResult<bool> {
        let runtime = runtime()?;
        let token = self.shutdown_token.clone();
        let timeout = Duration::try_from_secs_f64(timeout_seconds).unwrap_or(Duration::ZERO);
        let is_complete = python.detach(|| {
            runtime.block_on(async move {
                tokio::time::timeout(timeout, token.shutdown())
                    .await
                    .is_ok()
            })
        });
        for task in self
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .drain(..)
        {
            task.abort();
        }
        self.is_running.store(false, Ordering::SeqCst);
        Ok(is_complete)
    }
}

/// A pending publish and the Python future that waits for its result.
type PublishRequest = (PublishFuture, Py<PyAny>);

/// Wait for the publish results and send them to the bridge.
///
/// The task receives each new publish from `requests`. It waits for all
/// pending publishes at the same time, and sends each result to the bridge
/// as soon as it is ready. When the publisher is gone, the task waits for
/// the last results and stops.
async fn forward_publish_outcomes(
    mut requests: mpsc::UnboundedReceiver<PublishRequest>,
    bridge: mpsc::UnboundedSender<BridgeItem>,
    resolve_batch: Arc<Py<PyAny>>,
) {
    let mut pending = FuturesUnordered::new();
    loop {
        tokio::select! {
            request = requests.recv() => match request {
                Some((publish_future, future)) => {
                    pending.push(async move { (future, publish_future.await) });
                }
                None => break,
            },
            Some(outcome) = pending.next(), if !pending.is_empty() => {
                if bridge.send(BridgeItem::Publish(resolve_batch.clone(), outcome)).is_err() {
                    return;
                }
            }
        }
    }
    while let Some(outcome) = pending.next().await {
        if bridge
            .send(BridgeItem::Publish(resolve_batch.clone(), outcome))
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
struct Publisher {
    /// The batching publisher of the Pub/Sub client.
    inner: PubSubPublisher,
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
    /// Raises `CoreError` if the client cannot connect or cannot get the
    /// credentials.
    #[new]
    #[pyo3(signature = (topic_path, bridge, resolve_batch, endpoint=None))]
    fn new(
        python: Python<'_>,
        topic_path: String,
        bridge: &Bound<'_, Bridge>,
        resolve_batch: Py<PyAny>,
        endpoint: Option<String>,
    ) -> PyResult<Self> {
        let runtime = runtime()?;
        let bridge_sender = bridge.get().sender.clone();
        let inner = python.detach(|| {
            runtime.block_on(async move {
                anonymous_endpoint(
                    PubSubPublisher::builder(topic_path),
                    endpoint,
                    |builder, endpoint| {
                        builder
                            .with_endpoint(endpoint)
                            .with_credentials(AnonymousCredentials::new().build())
                    },
                )
                .build()
                .await
                .map_err(|error| build_error_to_python(&error))
            })
        })?;
        let (requests, request_receiver) = mpsc::unbounded_channel();
        runtime.spawn(forward_publish_outcomes(
            request_receiver,
            bridge_sender,
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
        let mut message = PubSubMessage::new()
            .set_data(data)
            .set_attributes(attributes);
        if let Some(ordering_key) = ordering_key.filter(|key| !key.is_empty()) {
            message = message.set_ordering_key(ordering_key);
        }
        let _guard = runtime()?.enter();
        let publish_future = self.inner.publish(message);
        self.requests
            .send((publish_future, future))
            .map_err(|_| CoreError::new_err("the publisher is closed"))
    }

    /// Send all pending batches and wait for the results.
    ///
    /// The call blocks, but it does not hold the GIL.
    ///
    /// Raises `CoreError` if the tokio runtime did not start.
    fn flush(&self, python: Python<'_>) -> PyResult<()> {
        let runtime = runtime()?;
        let inner = self.inner.clone();
        python.detach(|| runtime.block_on(async move { inner.flush().await }));
        Ok(())
    }
}

/// Create, update and remove topics and subscriptions.
///
/// Each method blocks until Pub/Sub answers, but it does not hold the GIL.
#[pyclass(frozen)]
struct AdminClient {
    /// The topic administration client.
    topics: TopicAdmin,
    /// The subscription administration client.
    subscriptions: SubscriptionAdmin,
}

/// Read one optional value from the settings dictionary.
///
/// A missing key or a `None` value gives `T::default()`.
///
/// # Errors
///
/// Returns a Python `TypeError` if the value has a different type.
fn read_setting<T>(settings: &Bound<'_, PyDict>, key: &str) -> PyResult<T>
where
    T: Default + for<'a, 'py> FromPyObject<'a, 'py, Error = PyErr>,
{
    match settings.get_item(key)? {
        Some(value) if !value.is_none() => value.extract(),
        _ => Ok(T::default()),
    }
}

/// Build the Pub/Sub subscription model from the Python settings.
///
/// The keys are the keys of the Python `SubscriptionSettings` type. The
/// retry policy is set only when `max_backoff_seconds` is more than 0. The
/// dead-letter policy is set only when `dead_letter_topic` is not empty.
///
/// # Errors
///
/// Returns a Python error if a value has a wrong type or does not fit in
/// the Pub/Sub field.
fn build_subscription_model(
    subscription_path: String,
    topic_path: String,
    settings: &Bound<'_, PyDict>,
) -> PyResult<SubscriptionModel> {
    let min_backoff_seconds: i64 = read_setting(settings, "min_backoff_seconds")?;
    let max_backoff_seconds: i64 = read_setting(settings, "max_backoff_seconds")?;
    let mut subscription = SubscriptionModel::new()
        .set_name(subscription_path)
        .set_topic(topic_path)
        .set_filter(read_setting::<String>(settings, "filter")?)
        .set_ack_deadline_seconds(read_setting::<i32>(settings, "ack_deadline_seconds")?)
        .set_enable_message_ordering(read_setting::<bool>(settings, "enable_message_ordering")?)
        .set_enable_exactly_once_delivery(read_setting::<bool>(
            settings,
            "enable_exactly_once_delivery",
        )?);
    if max_backoff_seconds > 0 {
        subscription = subscription.set_retry_policy(
            RetryPolicy::new()
                .set_minimum_backoff(wkt::Duration::clamp(min_backoff_seconds, 0))
                .set_maximum_backoff(wkt::Duration::clamp(max_backoff_seconds, 0)),
        );
    }
    let dead_letter_topic: String = read_setting(settings, "dead_letter_topic")?;
    if !dead_letter_topic.is_empty() {
        subscription = subscription.set_dead_letter_policy(
            DeadLetterPolicy::new()
                .set_dead_letter_topic(dead_letter_topic)
                .set_max_delivery_attempts(read_setting::<i32>(settings, "max_delivery_attempts")?),
        );
    }
    Ok(subscription)
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
        let runtime = runtime()?;
        python.detach(|| {
            runtime.block_on(async move {
                let topics = anonymous_endpoint(
                    TopicAdmin::builder(),
                    endpoint.clone(),
                    |builder, endpoint| {
                        builder
                            .with_endpoint(endpoint)
                            .with_credentials(AnonymousCredentials::new().build())
                    },
                )
                .build()
                .await
                .map_err(|error| build_error_to_python(&error))?;
                let subscriptions = anonymous_endpoint(
                    SubscriptionAdmin::builder(),
                    endpoint,
                    |builder, endpoint| {
                        builder
                            .with_endpoint(endpoint)
                            .with_credentials(AnonymousCredentials::new().build())
                    },
                )
                .build()
                .await
                .map_err(|error| build_error_to_python(&error))?;
                Ok(Self {
                    topics,
                    subscriptions,
                })
            })
        })
    }

    /// Create a topic.
    ///
    /// Raises `AlreadyExistsError` if the topic exists, or `CoreError` for
    /// a different error.
    fn create_topic(&self, python: Python<'_>, topic_path: String) -> PyResult<()> {
        let runtime = runtime()?;
        let topics = self.topics.clone();
        python.detach(|| {
            runtime.block_on(async move {
                topics
                    .create_topic()
                    .set_name(topic_path)
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|error| to_python_error(&error))
            })
        })
    }

    /// Remove a topic.
    ///
    /// Raises `NotFoundError` if the topic does not exist, or `CoreError`
    /// for a different error.
    fn delete_topic(&self, python: Python<'_>, topic_path: String) -> PyResult<()> {
        let runtime = runtime()?;
        let topics = self.topics.clone();
        python.detach(|| {
            runtime.block_on(async move {
                topics
                    .delete_topic()
                    .set_topic(topic_path)
                    .send()
                    .await
                    .map_err(|error| to_python_error(&error))
            })
        })
    }

    /// Create a subscription on a topic with the given settings.
    ///
    /// Raises `AlreadyExistsError` if the subscription exists,
    /// `NotFoundError` if the topic does not exist, or `CoreError` for a
    /// different error.
    fn create_subscription(
        &self,
        python: Python<'_>,
        subscription_path: String,
        topic_path: String,
        settings: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let runtime = runtime()?;
        let subscription = build_subscription_model(subscription_path, topic_path, settings)?;
        let subscriptions = self.subscriptions.clone();
        python.detach(|| {
            runtime.block_on(async move {
                subscriptions
                    .create_subscription()
                    .with_request(subscription)
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|error| to_python_error(&error))
            })
        })
    }

    /// Change the settings of a subscription.
    ///
    /// `update_fields` holds the names of the Pub/Sub fields to change, for
    /// example `ack_deadline_seconds` or `retry_policy`.
    ///
    /// Raises `NotFoundError` if the subscription does not exist, or
    /// `CoreError` for a different error.
    fn update_subscription(
        &self,
        python: Python<'_>,
        subscription_path: String,
        topic_path: String,
        settings: &Bound<'_, PyDict>,
        update_fields: Vec<String>,
    ) -> PyResult<()> {
        let runtime = runtime()?;
        let subscription = build_subscription_model(subscription_path, topic_path, settings)?;
        let subscriptions = self.subscriptions.clone();
        python.detach(|| {
            runtime.block_on(async move {
                subscriptions
                    .update_subscription()
                    .set_subscription(subscription)
                    .set_update_mask(wkt::FieldMask::default().set_paths(update_fields))
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|error| to_python_error(&error))
            })
        })
    }

    /// Remove a subscription.
    ///
    /// Raises `NotFoundError` if the subscription does not exist, or
    /// `CoreError` for a different error.
    fn delete_subscription(&self, python: Python<'_>, subscription_path: String) -> PyResult<()> {
        let runtime = runtime()?;
        let subscriptions = self.subscriptions.clone();
        python.detach(|| {
            runtime.block_on(async move {
                subscriptions
                    .delete_subscription()
                    .set_subscription(subscription_path)
                    .send()
                    .await
                    .map_err(|error| to_python_error(&error))
            })
        })
    }
}

/// Register the classes and the exceptions of the `fastpubsub_core`
/// module.
///
/// The module declares `gil_used = false`, so free-threaded Python keeps
/// the GIL disabled when it imports the module.
///
/// # Errors
///
/// Returns the Python error if a class or an exception cannot be added.
#[pymodule(gil_used = false)]
fn fastpubsub_core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("CoreError", module.py().get_type::<CoreError>())?;
    module.add("NotFoundError", module.py().get_type::<NotFoundError>())?;
    module.add(
        "AlreadyExistsError",
        module.py().get_type::<AlreadyExistsError>(),
    )?;
    module.add_class::<AckHandle>()?;
    module.add_class::<Bridge>()?;
    module.add_class::<Subscription>()?;
    module.add_class::<Publisher>()?;
    module.add_class::<AdminClient>()?;
    Ok(())
}
