//! The flow-control limits of one subscription.

use std::num::NonZeroU32;

/// The factor between the server limit and the client limit.
const SERVER_HEADROOM_FACTOR: i64 = 2;

/// The flow-control limits of one subscription.
///
/// The client runs at most `max_messages` handlers at the same time. The
/// server gets a higher limit, from
/// [`FlowControl::server_outstanding_messages`]. At the exact limit, the
/// Pub/Sub server stops the stream for about 500 milliseconds each time
/// that the client reaches the limit. With twice the limit, the next
/// messages are ready when a handler stops.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroU32;
///
/// use fastpubsub_messaging::FlowControl;
///
/// let flow_control = FlowControl::new(NonZeroU32::new(1000).unwrap());
/// assert_eq!(flow_control.server_outstanding_messages(), 2000);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowControl {
    /// The maximum number of handlers that run at the same time.
    max_messages: NonZeroU32,
}

impl FlowControl {
    /// Make the limits for a client that runs at most `max_messages`
    /// handlers at the same time.
    #[must_use]
    pub const fn new(max_messages: NonZeroU32) -> Self {
        Self { max_messages }
    }

    /// Get the maximum number of messages that the server sends before the
    /// client acks them.
    #[must_use]
    pub fn server_outstanding_messages(self) -> i64 {
        i64::from(self.max_messages.get()) * SERVER_HEADROOM_FACTOR
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use super::FlowControl;

    #[test]
    fn server_limit_is_twice_the_client_limit() {
        let flow_control = FlowControl::new(NonZeroU32::MIN);

        assert_eq!(flow_control.server_outstanding_messages(), 2);
    }

    #[test]
    fn server_limit_does_not_overflow_at_the_largest_client_limit() {
        let flow_control = FlowControl::new(NonZeroU32::MAX);

        assert_eq!(
            flow_control.server_outstanding_messages(),
            2 * i64::from(u32::MAX)
        );
    }
}
