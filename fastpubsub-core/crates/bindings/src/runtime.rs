//! The shared tokio runtime of the process.

use std::future::Future;
use std::sync::LazyLock;

use pyo3::prelude::*;
use tokio::runtime::Runtime;

use crate::error::CoreError;

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
/// Returns a `CoreError` if the runtime did not start.
pub(crate) fn runtime() -> PyResult<&'static Runtime> {
    RUNTIME
        .as_ref()
        .map_err(|error| CoreError::new_err(format!("the tokio runtime did not start: {error}")))
}

/// Run a future on the shared runtime and wait for its output.
///
/// The calling thread releases the GIL while it waits, so other Python
/// threads can run.
///
/// # Errors
///
/// Returns a `CoreError` if the runtime did not start.
pub(crate) fn block_on<F>(python: Python<'_>, future: F) -> PyResult<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    let runtime = runtime()?;
    Ok(python.detach(|| runtime.block_on(future)))
}
