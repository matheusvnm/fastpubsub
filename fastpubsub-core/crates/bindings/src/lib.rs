//! The Python bindings of the `FastPubSub` core.
//!
//! This crate builds the Python extension module `fastpubsub_core`. The
//! `fastpubsub` package is the only expected user. The names can change in
//! each release of `fastpubsub-core`.
//!
//! The crate holds only the Python side. The broker code is in the adapter
//! crates, for example `fastpubsub-google-pubsub`, and the shared types are
//! in `fastpubsub-messaging`.
//!
//! - `runtime`: the shared tokio runtime with two worker threads. It runs
//!   all network I/O.
//! - `bridge`: one OS thread for each asyncio event loop. It sends the
//!   received messages and the publish results to the event loop in
//!   batches, and holds the GIL one time for each batch.
//! - `subscription`, `publisher` and `administration`: the Python classes
//!   `Subscription`, `Publisher` and `AdminClient`.
//! - `acknowledgement`: the Python class `AckHandle`.
//! - `settings`: the conversion of the Python subscription settings.
//! - `error`: the Python exceptions.
//!
//! The classes are frozen and the module declares `gil_used = false`, so
//! the module works on free-threaded Python.

mod acknowledgement;
mod administration;
mod bridge;
mod error;
mod publisher;
mod runtime;
mod settings;
mod subscription;

use pyo3::prelude::*;

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
    let python = module.py();
    module.add("CoreError", python.get_type::<error::CoreError>())?;
    module.add("NotFoundError", python.get_type::<error::NotFoundError>())?;
    module.add(
        "AlreadyExistsError",
        python.get_type::<error::AlreadyExistsError>(),
    )?;
    module.add_class::<acknowledgement::AckHandle>()?;
    module.add_class::<bridge::Bridge>()?;
    module.add_class::<subscription::Subscription>()?;
    module.add_class::<publisher::Publisher>()?;
    module.add_class::<administration::AdminClient>()?;
    Ok(())
}
