use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
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

create_exception!(fastpubsub_core, CoreError, PyRuntimeError);
create_exception!(fastpubsub_core, NotFoundError, CoreError);
create_exception!(fastpubsub_core, AlreadyExistsError, CoreError);

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("fastpubsub-core")
        .enable_all()
        .build()
        .expect("the tokio runtime must start")
});

fn to_python_error(error: &google_cloud_pubsub::Error) -> PyErr {
    let text = error.to_string();
    match error.status().map(|status| status.code) {
        Some(Code::NotFound) => NotFoundError::new_err(text),
        Some(Code::AlreadyExists) => AlreadyExistsError::new_err(text),
        _ => CoreError::new_err(text),
    }
}

fn publish_error_to_python(error: &PublishError) -> PyErr {
    match error {
        PublishError::Rpc(inner) => to_python_error(inner),
        other => CoreError::new_err(other.to_string()),
    }
}

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

#[pyclass(frozen)]
struct AckHandle {
    handler: Mutex<Option<Handler>>,
}

#[pymethods]
impl AckHandle {
    fn ack(&self) {
        if let Some(handler) = self.handler.lock().expect("lock").take() {
            handler.ack();
        }
    }

    fn nack(&self) {
        if let Some(handler) = self.handler.lock().expect("lock").take() {
            handler.nack();
        }
    }
}

type ReceivedMessage = (PubSubMessage, Handler);
type PublishOutcome = (Py<PyAny>, Result<String, PublishError>);

enum BridgeItem {
    Message(Arc<Py<PyAny>>, ReceivedMessage),
    Publish(Arc<Py<PyAny>>, PublishOutcome),
}

fn build_message_entry<'py>(
    python: Python<'py>,
    message: PubSubMessage,
    handler: Handler,
) -> PyResult<Bound<'py, PyAny>> {
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

fn build_publish_entry<'py>(
    python: Python<'py>,
    future: Py<PyAny>,
    result: Result<String, PublishError>,
) -> PyResult<Bound<'py, PyAny>> {
    match result {
        Ok(message_id) => Ok((future, true, message_id).into_pyobject(python)?.into_any()),
        Err(error) => {
            let exception = publish_error_to_python(&error).into_value(python);
            Ok((future, false, exception).into_pyobject(python)?.into_any())
        }
    }
}

fn run_bridge(
    mut receiver: mpsc::UnboundedReceiver<BridgeItem>,
    max_batch_size: usize,
    call_soon_threadsafe: Py<PyAny>,
    run_callbacks: Py<PyAny>,
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
        let result = Python::attach(|python| -> PyResult<()> {
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
                match groups
                    .iter()
                    .find(|(existing, _)| Arc::ptr_eq(existing, &callback))
                {
                    Some((_, entries)) => entries.append(entry)?,
                    None => {
                        let entries = PyList::empty(python);
                        entries.append(entry)?;
                        groups.push((callback, entries));
                    }
                }
            }
            let calls = PyList::empty(python);
            for (callback, entries) in groups {
                calls.append((callback.bind(python), entries))?;
            }
            call_soon_threadsafe.call1(python, (run_callbacks.bind(python), calls))?;
            Ok(())
        });
        if let Err(error) = result {
            eprintln!("fastpubsub-core: the bridge callback failed: {error}");
        }
    }
}

#[pyclass(frozen)]
struct Bridge {
    sender: mpsc::UnboundedSender<BridgeItem>,
}

#[pymethods]
impl Bridge {
    #[new]
    fn new(
        max_batch_size: usize,
        call_soon_threadsafe: Py<PyAny>,
        run_callbacks: Py<PyAny>,
    ) -> PyResult<Self> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let max_batch_size = max_batch_size.max(1);
        std::thread::Builder::new()
            .name("fastpubsub-bridge".into())
            .spawn(move || {
                run_bridge(
                    receiver,
                    max_batch_size,
                    call_soon_threadsafe,
                    run_callbacks,
                )
            })
            .map_err(|error| CoreError::new_err(error.to_string()))?;
        Ok(Self { sender })
    }
}

#[pyclass(frozen)]
struct Subscription {
    tasks: Mutex<Vec<JoinHandle<()>>>,
    shutdown_token: ShutdownToken,
    is_running: Arc<AtomicBool>,
}

#[pymethods]
impl Subscription {
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
        let sender = bridge.get().sender.clone();
        let subscriber = python.detach(|| {
            RUNTIME.block_on(async move {
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
                .map_err(|error| CoreError::new_err(error.to_string()))
            })
        })?;
        let _guard = RUNTIME.enter();
        let mut stream = subscriber
            .subscribe(subscription_path)
            .set_max_outstanding_messages(max_outstanding_messages)
            .build();
        let shutdown_token = stream.shutdown_token();
        let is_running = Arc::new(AtomicBool::new(true));
        let dispatch = Arc::new(dispatch);

        let reader_is_running = is_running.clone();
        let reader = RUNTIME.spawn(async move {
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

    fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    fn shutdown(&self, python: Python<'_>, timeout_seconds: f64) -> bool {
        let token = self.shutdown_token.clone();
        let is_complete = python.detach(|| {
            RUNTIME.block_on(async move {
                tokio::time::timeout(Duration::from_secs_f64(timeout_seconds), token.shutdown())
                    .await
                    .is_ok()
            })
        });
        for task in self.tasks.lock().expect("lock").drain(..) {
            task.abort();
        }
        self.is_running.store(false, Ordering::SeqCst);
        is_complete
    }
}

type PublishRequest = (PublishFuture, Py<PyAny>);

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
                let _ = bridge.send(BridgeItem::Publish(resolve_batch.clone(), outcome));
            }
        }
    }
    while let Some(outcome) = pending.next().await {
        let _ = bridge.send(BridgeItem::Publish(resolve_batch.clone(), outcome));
    }
}

#[pyclass(frozen)]
struct Publisher {
    inner: PubSubPublisher,
    requests: mpsc::UnboundedSender<PublishRequest>,
}

#[pymethods]
impl Publisher {
    #[new]
    #[pyo3(signature = (topic_path, bridge, resolve_batch, endpoint=None))]
    fn new(
        python: Python<'_>,
        topic_path: String,
        bridge: &Bound<'_, Bridge>,
        resolve_batch: Py<PyAny>,
        endpoint: Option<String>,
    ) -> PyResult<Self> {
        let bridge_sender = bridge.get().sender.clone();
        let inner = python.detach(|| {
            RUNTIME.block_on(async move {
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
                .map_err(|error| CoreError::new_err(error.to_string()))
            })
        })?;
        let (requests, request_receiver) = mpsc::unbounded_channel();
        RUNTIME.spawn(forward_publish_outcomes(
            request_receiver,
            bridge_sender,
            Arc::new(resolve_batch),
        ));
        Ok(Self { inner, requests })
    }

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
        let _guard = RUNTIME.enter();
        let publish_future = self.inner.publish(message);
        self.requests
            .send((publish_future, future))
            .map_err(|_| CoreError::new_err("the publisher is closed"))
    }

    fn flush(&self, python: Python<'_>) {
        let inner = self.inner.clone();
        python.detach(|| RUNTIME.block_on(async move { inner.flush().await }));
    }
}

#[pyclass(frozen)]
struct AdminClient {
    topics: TopicAdmin,
    subscriptions: SubscriptionAdmin,
}

fn build_subscription_model(
    subscription_path: String,
    topic_path: String,
    settings: &Bound<'_, PyDict>,
) -> PyResult<SubscriptionModel> {
    let read_string = |key: &str| -> PyResult<String> {
        match settings.get_item(key)? {
            Some(value) if !value.is_none() => value.extract(),
            _ => Ok(String::new()),
        }
    };
    let read_integer = |key: &str| -> PyResult<i64> {
        match settings.get_item(key)? {
            Some(value) => value.extract(),
            None => Ok(0),
        }
    };
    let read_boolean = |key: &str| -> PyResult<bool> {
        match settings.get_item(key)? {
            Some(value) => value.extract(),
            None => Ok(false),
        }
    };

    let min_backoff_seconds = read_integer("min_backoff_seconds")?;
    let max_backoff_seconds = read_integer("max_backoff_seconds")?;
    let mut subscription = SubscriptionModel::new()
        .set_name(subscription_path)
        .set_topic(topic_path)
        .set_filter(read_string("filter")?)
        .set_ack_deadline_seconds(read_integer("ack_deadline_seconds")? as i32)
        .set_enable_message_ordering(read_boolean("enable_message_ordering")?)
        .set_enable_exactly_once_delivery(read_boolean("enable_exactly_once_delivery")?);
    if max_backoff_seconds > 0 {
        subscription = subscription.set_retry_policy(
            RetryPolicy::new()
                .set_minimum_backoff(wkt::Duration::clamp(min_backoff_seconds, 0))
                .set_maximum_backoff(wkt::Duration::clamp(max_backoff_seconds, 0)),
        );
    }
    let dead_letter_topic = read_string("dead_letter_topic")?;
    if !dead_letter_topic.is_empty() {
        subscription = subscription.set_dead_letter_policy(
            DeadLetterPolicy::new()
                .set_dead_letter_topic(dead_letter_topic)
                .set_max_delivery_attempts(read_integer("max_delivery_attempts")? as i32),
        );
    }
    Ok(subscription)
}

#[pymethods]
impl AdminClient {
    #[new]
    #[pyo3(signature = (endpoint=None))]
    fn new(python: Python<'_>, endpoint: Option<String>) -> PyResult<Self> {
        python.detach(|| {
            RUNTIME.block_on(async move {
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
                .map_err(|error| CoreError::new_err(error.to_string()))?;
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
                .map_err(|error| CoreError::new_err(error.to_string()))?;
                Ok(Self {
                    topics,
                    subscriptions,
                })
            })
        })
    }

    fn create_topic(&self, python: Python<'_>, topic_path: String) -> PyResult<()> {
        let topics = self.topics.clone();
        python.detach(|| {
            RUNTIME.block_on(async move {
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

    fn delete_topic(&self, python: Python<'_>, topic_path: String) -> PyResult<()> {
        let topics = self.topics.clone();
        python.detach(|| {
            RUNTIME.block_on(async move {
                topics
                    .delete_topic()
                    .set_topic(topic_path)
                    .send()
                    .await
                    .map_err(|error| to_python_error(&error))
            })
        })
    }

    fn create_subscription(
        &self,
        python: Python<'_>,
        subscription_path: String,
        topic_path: String,
        settings: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let subscription = build_subscription_model(subscription_path, topic_path, settings)?;
        let subscriptions = self.subscriptions.clone();
        python.detach(|| {
            RUNTIME.block_on(async move {
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

    fn update_subscription(
        &self,
        python: Python<'_>,
        subscription_path: String,
        topic_path: String,
        settings: &Bound<'_, PyDict>,
        update_fields: Vec<String>,
    ) -> PyResult<()> {
        let subscription = build_subscription_model(subscription_path, topic_path, settings)?;
        let subscriptions = self.subscriptions.clone();
        python.detach(|| {
            RUNTIME.block_on(async move {
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

    fn delete_subscription(&self, python: Python<'_>, subscription_path: String) -> PyResult<()> {
        let subscriptions = self.subscriptions.clone();
        python.detach(|| {
            RUNTIME.block_on(async move {
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
