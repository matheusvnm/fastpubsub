//! The acknowledgement of one received message.

/// Acknowledge or reject one received message.
///
/// A broker adapter implements this trait for its own ack handle. The
/// methods take the handle by value, so each message gets one ack or one
/// nack only. The methods must not block. An adapter that needs async work
/// for an ack sends the request to a task and returns at once.
///
/// # Examples
///
/// ```
/// use std::sync::mpsc;
///
/// use fastpubsub_messaging::Acknowledge;
///
/// struct ChannelAcknowledgement {
///     message_id: String,
///     results: mpsc::Sender<(String, bool)>,
/// }
///
/// impl Acknowledge for ChannelAcknowledgement {
///     fn ack(self: Box<Self>) {
///         let _ = self.results.send((self.message_id, true));
///     }
///
///     fn nack(self: Box<Self>) {
///         let _ = self.results.send((self.message_id, false));
///     }
/// }
///
/// let (sender, receiver) = mpsc::channel();
/// let handle: Box<dyn Acknowledge> = Box::new(ChannelAcknowledgement {
///     message_id: "1".to_owned(),
///     results: sender,
/// });
/// handle.ack();
/// assert_eq!(receiver.recv().unwrap(), ("1".to_owned(), true));
/// ```
pub trait Acknowledge: Send {
    /// Acknowledge the message, so the broker does not send it again.
    fn ack(self: Box<Self>);

    /// Reject the message, so the broker sends it again.
    fn nack(self: Box<Self>);
}
