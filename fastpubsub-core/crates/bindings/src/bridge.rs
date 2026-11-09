//! The bridge thread that sends results to an asyncio event loop.

use std::sync::Arc;

use fastpubsub_messaging::{ReceivedMessage, Result};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use tokio::sync::mpsc;

use crate::acknowledgement::AckHandle;
use crate::error::{CoreError, to_python_error};

/// One item that the bridge thread sends to the event loop.
///
/// Each variant holds the Python callback that receives the item. The
/// bridge groups the items of one batch by callback.
pub(crate) enum BridgeItem {
    /// A received message for the `dispatch` callback of a subscription.
    Message(Arc<Py<PyAny>>, ReceivedMessage),
    /// A publish result for the `resolve_batch` callback of a publisher,
    /// with the Python future that waits for it.
    Publish(Arc<Py<PyAny>>, Py<PyAny>, Result<String>),
}

/// Convert a received message to the Python tuple that `dispatch` gets.
///
/// The tuple is `(message_id, data, attributes, delivery_attempt,
/// ack_handle)`.
///
/// # Errors
///
/// Returns the Python error if Python cannot make an object.
fn build_message_entry(python: Python<'_>, message: ReceivedMessage) -> PyResult<Bound<'_, PyAny>> {
    let attributes = PyDict::new(python);
    for (key, value) in &message.attributes {
        attributes.set_item(key, value)?;
    }
    let entry = PyTuple::new(
        python,
        [
            message.id.into_pyobject(python)?.into_any(),
            PyBytes::new(python, &message.data).into_any(),
            attributes.into_any(),
            message.delivery_attempt.into_pyobject(python)?.into_any(),
            Bound::new(python, AckHandle::new(message.acknowledgement))?.into_any(),
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
    result: Result<String>,
) -> PyResult<Bound<'_, PyAny>> {
    match result {
        Ok(message_id) => Ok((future, true, message_id).into_pyobject(python)?.into_any()),
        Err(error) => {
            let exception = to_python_error(&error).into_value(python);
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
                BridgeItem::Message(callback, message) => {
                    (callback, build_message_entry(python, message)?)
                }
                BridgeItem::Publish(callback, future, result) => {
                    (callback, build_publish_entry(python, future, result)?)
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
pub(crate) struct Bridge {
    /// The channel to the bridge thread.
    sender: mpsc::UnboundedSender<BridgeItem>,
}

impl Bridge {
    /// Get a new sender for the channel to the bridge thread.
    pub(crate) fn sender(&self) -> mpsc::UnboundedSender<BridgeItem> {
        self.sender.clone()
    }
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
