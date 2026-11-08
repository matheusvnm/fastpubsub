//! The Python class `AckHandle`.

use std::sync::{Mutex, PoisonError};

use fastpubsub_messaging::Acknowledge;
use pyo3::prelude::*;

/// The acknowledgement of one received message.
///
/// The first call to `ack` or `nack` has an effect. The next calls do
/// nothing. The methods do not block and do not need the GIL.
#[pyclass(frozen)]
pub(crate) struct AckHandle {
    /// The ack handle of the adapter. It is `None` after the first ack or
    /// nack.
    acknowledgement: Mutex<Option<Box<dyn Acknowledge>>>,
}

impl AckHandle {
    /// Wrap the ack handle of an adapter.
    pub(crate) fn new(acknowledgement: Box<dyn Acknowledge>) -> Self {
        Self {
            acknowledgement: Mutex::new(Some(acknowledgement)),
        }
    }

    /// Take the ack handle out of the Python object.
    ///
    /// Returns `None` if a different call took it before. A poisoned lock
    /// does not stop the call, because the handle stays valid.
    fn take(&self) -> Option<Box<dyn Acknowledge>> {
        self.acknowledgement
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

#[pymethods]
impl AckHandle {
    /// Acknowledge the message, so Pub/Sub does not send it again.
    fn ack(&self) {
        if let Some(acknowledgement) = self.take() {
            acknowledgement.ack();
        }
    }

    /// Reject the message, so Pub/Sub sends it again soon.
    fn nack(&self) {
        if let Some(acknowledgement) = self.take() {
            acknowledgement.nack();
        }
    }
}
