use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentials;
use google_cloud_pubsub::client::{Publisher as PubSubPublisher, Subscriber as PubSubSubscriber};
use google_cloud_pubsub::model::Message as PubSubMessage;
use google_cloud_pubsub::subscriber::handler::Handler;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("fastpubsub-core")
        .enable_all()
        .build()
        .expect("the tokio runtime must start")
});

fn to_python_error(error: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(error.to_string())
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

fn build_python_batch<'py>(
    python: Python<'py>,
    batch: Vec<ReceivedMessage>,
) -> PyResult<Bound<'py, PyList>> {
    let items = PyList::empty(python);
    for (message, handler) in batch {
        let delivery_attempt = handler.delivery_attempt().unwrap_or(0);
        let attributes = PyDict::new(python);
        for (key, value) in &message.attributes {
            attributes.set_item(key, value)?;
        }
        let ack_handle = AckHandle {
            handler: Mutex::new(Some(handler)),
        };
        let item = PyTuple::new(
            python,
            [
                message.message_id.into_pyobject(python)?.into_any(),
                PyBytes::new(python, &message.data).into_any(),
                attributes.into_any(),
                delivery_attempt.into_pyobject(python)?.into_any(),
                Bound::new(python, ack_handle)?.into_any(),
            ],
        )?;
        items.append(item)?;
    }
    Ok(items)
}

async fn build_subscriber(endpoint: Option<String>) -> PyResult<PubSubSubscriber> {
    let mut builder = PubSubSubscriber::builder();
    if let Some(endpoint) = endpoint {
        builder = builder
            .with_endpoint(endpoint)
            .with_credentials(AnonymousCredentials::new().build());
    }
    builder.build().await.map_err(to_python_error)
}

async fn run_subscription(
    subscriber: PubSubSubscriber,
    subscription_path: String,
    max_messages: i64,
    max_batch_size: usize,
    call_soon_threadsafe: Py<PyAny>,
    dispatch: Py<PyAny>,
    is_running: Arc<AtomicBool>,
) {
    let mut stream = subscriber
        .subscribe(subscription_path)
        .set_max_outstanding_messages(max_messages)
        .build();
    let (sender, mut receiver) = mpsc::unbounded_channel::<ReceivedMessage>();

    let reader = tokio::spawn(async move {
        while let Some(item) = stream.next().await {
            match item {
                Ok(received) => {
                    if sender.send(received).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    eprintln!("fastpubsub-core: the streaming pull failed: {error}");
                    break;
                }
            }
        }
    });

    while let Some(first) = receiver.recv().await {
        let mut batch = Vec::with_capacity(max_batch_size);
        batch.push(first);
        while batch.len() < max_batch_size {
            match receiver.try_recv() {
                Ok(received) => batch.push(received),
                Err(_) => break,
            }
        }
        let result = Python::attach(|python| -> PyResult<()> {
            let items = build_python_batch(python, batch)?;
            call_soon_threadsafe.call1(python, (dispatch.bind(python), items))?;
            Ok(())
        });
        if let Err(error) = result {
            eprintln!("fastpubsub-core: the dispatch failed: {error}");
            break;
        }
    }

    reader.abort();
    is_running.store(false, Ordering::SeqCst);
}

#[pyclass(frozen)]
struct Subscription {
    task: Mutex<Option<JoinHandle<()>>>,
    is_running: Arc<AtomicBool>,
}

#[pymethods]
impl Subscription {
    #[new]
    #[pyo3(signature = (subscription_path, max_messages, max_batch_size, call_soon_threadsafe, dispatch, endpoint=None))]
    fn new(
        python: Python<'_>,
        subscription_path: String,
        max_messages: i64,
        max_batch_size: usize,
        call_soon_threadsafe: Py<PyAny>,
        dispatch: Py<PyAny>,
        endpoint: Option<String>,
    ) -> PyResult<Self> {
        let subscriber = python.detach(|| RUNTIME.block_on(build_subscriber(endpoint)))?;
        let is_running = Arc::new(AtomicBool::new(true));
        let task = RUNTIME.spawn(run_subscription(
            subscriber,
            subscription_path,
            max_messages,
            max_batch_size.max(1),
            call_soon_threadsafe,
            dispatch,
            is_running.clone(),
        ));
        Ok(Self {
            task: Mutex::new(Some(task)),
            is_running,
        })
    }

    fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    fn stop(&self) {
        if let Some(task) = self.task.lock().expect("lock").take() {
            task.abort();
        }
        self.is_running.store(false, Ordering::SeqCst);
    }
}

type PublishOutcome = (Py<PyAny>, bool, String);

async fn complete_publishes(
    mut receiver: mpsc::UnboundedReceiver<PublishOutcome>,
    max_batch_size: usize,
    call_soon_threadsafe: Py<PyAny>,
    resolve_batch: Py<PyAny>,
) {
    while let Some(first) = receiver.recv().await {
        let mut batch = Vec::with_capacity(max_batch_size);
        batch.push(first);
        while batch.len() < max_batch_size {
            match receiver.try_recv() {
                Ok(outcome) => batch.push(outcome),
                Err(_) => break,
            }
        }
        let result = Python::attach(|python| -> PyResult<()> {
            let outcomes = PyList::empty(python);
            for (future, is_success, value) in batch {
                outcomes.append((future, is_success, value))?;
            }
            call_soon_threadsafe.call1(python, (resolve_batch.bind(python), outcomes))?;
            Ok(())
        });
        if let Err(error) = result {
            eprintln!("fastpubsub-core: the publish callback failed: {error}");
        }
    }
}

#[pyclass(frozen)]
struct Publisher {
    inner: PubSubPublisher,
    outcomes: mpsc::UnboundedSender<PublishOutcome>,
}

#[pymethods]
impl Publisher {
    #[new]
    #[pyo3(signature = (topic_path, max_batch_size, call_soon_threadsafe, resolve_batch, endpoint=None))]
    fn new(
        python: Python<'_>,
        topic_path: String,
        max_batch_size: usize,
        call_soon_threadsafe: Py<PyAny>,
        resolve_batch: Py<PyAny>,
        endpoint: Option<String>,
    ) -> PyResult<Self> {
        let inner = python.detach(|| {
            RUNTIME.block_on(async move {
                let mut builder = PubSubPublisher::builder(topic_path);
                if let Some(endpoint) = endpoint {
                    builder = builder
                        .with_endpoint(endpoint)
                        .with_credentials(AnonymousCredentials::new().build());
                }
                builder.build().await.map_err(to_python_error)
            })
        })?;
        let (outcomes, receiver) = mpsc::unbounded_channel();
        RUNTIME.spawn(complete_publishes(
            receiver,
            max_batch_size.max(1),
            call_soon_threadsafe,
            resolve_batch,
        ));
        Ok(Self { inner, outcomes })
    }

    #[pyo3(signature = (data, attributes, future, ordering_key=None))]
    fn publish(
        &self,
        data: Vec<u8>,
        attributes: HashMap<String, String>,
        future: Py<PyAny>,
        ordering_key: Option<String>,
    ) {
        let mut message = PubSubMessage::new()
            .set_data(data)
            .set_attributes(attributes);
        if let Some(ordering_key) = ordering_key.filter(|key| !key.is_empty()) {
            message = message.set_ordering_key(ordering_key);
        }
        let _guard = RUNTIME.enter();
        let pending = self.inner.publish(message);
        let outcomes = self.outcomes.clone();
        RUNTIME.spawn(async move {
            let outcome = match pending.await {
                Ok(message_id) => (future, true, message_id),
                Err(error) => (future, false, error.to_string()),
            };
            let _ = outcomes.send(outcome);
        });
    }
}

#[pymodule]
fn fastpubsub_core(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<AckHandle>()?;
    module.add_class::<Subscription>()?;
    module.add_class::<Publisher>()?;
    Ok(())
}
