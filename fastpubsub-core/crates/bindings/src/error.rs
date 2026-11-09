//! The Python exceptions of the module.

use fastpubsub_messaging::{Error, ErrorKind};
use pyo3::create_exception;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

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

/// Convert an adapter error to a Python exception.
///
/// The kind selects the class: `NotFound` gives `NotFoundError`,
/// `AlreadyExists` gives `AlreadyExistsError`, `InvalidArgument` gives
/// `ValueError`, and `Other` gives `CoreError`.
pub(crate) fn to_python_error(error: &Error) -> PyErr {
    let text = error.to_string();
    match error.kind() {
        ErrorKind::NotFound => NotFoundError::new_err(text),
        ErrorKind::AlreadyExists => AlreadyExistsError::new_err(text),
        ErrorKind::InvalidArgument => PyValueError::new_err(text),
        ErrorKind::Other => CoreError::new_err(text),
    }
}

/// Convert an adapter result to a Python result.
pub(crate) trait IntoPythonResult<T> {
    /// Convert the error with [`to_python_error`].
    ///
    /// # Errors
    ///
    /// Returns the Python exception of the adapter error.
    fn into_python_result(self) -> PyResult<T>;
}

impl<T> IntoPythonResult<T> for fastpubsub_messaging::Result<T> {
    fn into_python_result(self) -> PyResult<T> {
        self.map_err(|error| to_python_error(&error))
    }
}
