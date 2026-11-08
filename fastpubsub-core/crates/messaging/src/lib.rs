//! The message types of the `FastPubSub` core that do not depend on a
//! broker.
//!
//! Each broker adapter converts its own types to these types. The Python
//! bindings use only these types, so a new adapter, for example one for
//! Apache Pulsar, does not change the bindings.
//!
//! - [`ReceivedMessage`] is one message that a subscription received. It
//!   holds an [`Acknowledge`] handle that acks or nacks it.
//! - [`OutgoingMessage`] is one message to publish.
//! - [`FlowControl`] holds the limits of one subscription.
//! - [`Error`] is the error of each adapter, with an [`ErrorKind`].

mod acknowledgement;
mod error;
mod flow_control;
mod message;

pub use acknowledgement::Acknowledge;
pub use error::{Error, ErrorKind, Result};
pub use flow_control::FlowControl;
pub use message::{OutgoingMessage, ReceivedMessage};
