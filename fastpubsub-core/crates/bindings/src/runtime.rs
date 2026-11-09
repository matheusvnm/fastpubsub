//! The shared tokio runtime of the process.
//!
//! The runtime starts at the first network call. It reads the number of
//! worker threads from `FASTPUBSUB_NETWORK_THREADS` at that time. The
//! `fastpubsub run` command writes the variable from its
//! `--network-threads` option, so each worker process gets the same value.

use std::future::Future;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicUsize, Ordering};

use pyo3::prelude::*;
use tokio::runtime::Runtime;

use crate::error::CoreError;

/// The environment variable that holds the number of worker threads.
const WORKER_THREADS_VARIABLE: &str = "FASTPUBSUB_NETWORK_THREADS";

/// The number of worker threads when the variable is not set.
const DEFAULT_WORKER_THREADS: usize = 2;

/// The shared tokio runtime of the process.
///
/// The runtime holds the error if it cannot start, so each caller gets a
/// Python exception and not a panic.
static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();

/// Read the number of worker threads from the environment.
///
/// An unset or empty variable gives [`DEFAULT_WORKER_THREADS`].
///
/// # Errors
///
/// Returns a message if the value is not a whole number of at least 1.
fn read_worker_threads() -> Result<usize, String> {
    let value = std::env::var(WORKER_THREADS_VARIABLE).unwrap_or_default();
    if value.trim().is_empty() {
        return Ok(DEFAULT_WORKER_THREADS);
    }
    match value.trim().parse::<usize>() {
        Ok(worker_threads) if worker_threads > 0 => Ok(worker_threads),
        _ => Err(format!(
            "{WORKER_THREADS_VARIABLE} must be a whole number of at least 1, not \"{value}\""
        )),
    }
}

/// Build the runtime with the worker threads from the environment.
///
/// The threads have the names `fastpubsub-network-<n>`.
///
/// # Errors
///
/// Returns a message if the variable has a wrong value or if the
/// operating system cannot start the threads.
fn build_runtime() -> Result<Runtime, String> {
    let worker_threads = read_worker_threads()?;
    let thread_number = AtomicUsize::new(0);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .thread_name_fn(move || {
            let number = thread_number.fetch_add(1, Ordering::Relaxed);
            format!("fastpubsub-network-{number}")
        })
        .enable_all()
        .build()
        .map_err(|error| format!("the tokio runtime did not start: {error}"))
}

/// Get the shared tokio runtime, and start it at the first call.
///
/// # Errors
///
/// Returns a `CoreError` if `FASTPUBSUB_NETWORK_THREADS` has a wrong value
/// or if the runtime did not start.
pub(crate) fn runtime() -> PyResult<&'static Runtime> {
    RUNTIME
        .get_or_init(build_runtime)
        .as_ref()
        .map_err(|message| CoreError::new_err(message.clone()))
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
