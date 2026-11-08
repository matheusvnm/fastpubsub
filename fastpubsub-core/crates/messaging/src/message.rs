//! The messages that an adapter receives and publishes.

use std::collections::HashMap;

use bytes::Bytes;

use crate::Acknowledge;

/// One message that a subscription received.
///
/// The receiver must call [`Acknowledge::ack`] or [`Acknowledge::nack`] on
/// [`ReceivedMessage::acknowledgement`] one time.
pub struct ReceivedMessage {
    /// The message ID that the broker gave.
    pub id: String,
    /// The payload.
    pub data: Bytes,
    /// The attributes.
    pub attributes: HashMap<String, String>,
    /// The number of delivery attempts, or 0 when the broker does not count
    /// them.
    pub delivery_attempt: i32,
    /// The handle that acks or nacks this message.
    pub acknowledgement: Box<dyn Acknowledge>,
}

/// One message to publish.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OutgoingMessage {
    /// The payload.
    pub data: Bytes,
    /// The attributes.
    pub attributes: HashMap<String, String>,
    /// The ordering key, or `None` to publish without a key.
    pub ordering_key: Option<String>,
}
