//! The Google Cloud Pub/Sub adapter of the `FastPubSub` core.
//!
//! The adapter wraps the `google-cloud-pubsub` crate. It converts the
//! Google types to the types of `fastpubsub-messaging`, so the Python
//! bindings do not use a Google type.
//!
//! - [`Connection`] selects the Pub/Sub service or the emulator.
//! - [`TopicPath`] and [`SubscriptionPath`] are validated resource paths.
//! - [`StreamingPull`] receives the messages of one subscription.
//! - [`Publisher`] publishes messages to one topic in batches.
//! - [`Administration`] creates, updates and removes topics and
//!   subscriptions.
//!
//! The async functions must run on a multi-thread tokio runtime.
//!
//! # Examples
//!
//! ```no_run
//! use std::num::NonZeroU32;
//!
//! use fastpubsub_google_pubsub::{Connection, StreamingPull, SubscriptionPath};
//! use fastpubsub_messaging::FlowControl;
//!
//! # async fn receive() -> fastpubsub_messaging::Result<()> {
//! let connection = Connection::new(Some("http://localhost:8085".to_owned()));
//! let subscription: SubscriptionPath = "projects/demo/subscriptions/orders".parse()?;
//! let flow_control = FlowControl::new(NonZeroU32::new(100).unwrap());
//! let stream = StreamingPull::start(&connection, &subscription, flow_control, |message| {
//!     message.acknowledgement.ack();
//!     true
//! })
//! .await?;
//! stream.shutdown(std::time::Duration::from_secs(10)).await;
//! # Ok(())
//! # }
//! ```

mod administration;
mod connection;
mod error;
mod publisher;
mod resource_path;
mod subscriber;

pub use administration::{Administration, DeadLetter, RetryBackoff, SubscriptionSettings};
pub use connection::Connection;
pub use publisher::Publisher;
pub use resource_path::{SubscriptionPath, TopicPath};
pub use subscriber::StreamingPull;
