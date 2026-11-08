//! The streaming pull of one subscription.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use fastpubsub_messaging::{Acknowledge, FlowControl, ReceivedMessage, Result};
use google_cloud_pubsub::model::Message;
use google_cloud_pubsub::subscriber::ShutdownToken;
use google_cloud_pubsub::subscriber::handler::Handler;
use tokio::task::JoinHandle;

use crate::{Connection, SubscriptionPath};

/// The ack handler of the Google client as an [`Acknowledge`] handle.
struct HandlerAcknowledgement(Handler);

impl Acknowledge for HandlerAcknowledgement {
    fn ack(self: Box<Self>) {
        self.0.ack();
    }

    fn nack(self: Box<Self>) {
        self.0.nack();
    }
}

/// Convert a message of the Google client to a [`ReceivedMessage`].
///
/// The delivery attempt is 0 when the subscription has no dead-letter
/// policy.
fn to_received_message(message: Message, handler: Handler) -> ReceivedMessage {
    ReceivedMessage {
        id: message.message_id,
        data: message.data,
        attributes: message.attributes,
        delivery_attempt: handler.delivery_attempt().unwrap_or(0),
        acknowledgement: Box::new(HandlerAcknowledgement(handler)),
    }
}

/// One streaming pull on a subscription.
///
/// The stream runs in a tokio task. The task gives each received message
/// to the `deliver` function of [`StreamingPull::start`].
#[derive(Debug)]
pub struct StreamingPull {
    /// The task that reads the stream.
    reader: JoinHandle<()>,
    /// The token that stops the stream and waits for the pending acks.
    shutdown_token: ShutdownToken,
    /// True until the stream stops.
    is_running: Arc<AtomicBool>,
}

impl StreamingPull {
    /// Connect to Pub/Sub and start the stream in a task on the current
    /// tokio runtime.
    ///
    /// The server sends at most
    /// [`FlowControl::server_outstanding_messages`] messages before the
    /// client acks them. `deliver` gets each message. It returns `false` to
    /// stop the stream, for example when the receiver is gone. A stream
    /// error also stops the stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the client cannot get the credentials or cannot
    /// make the transport.
    pub async fn start<F>(
        connection: &Connection,
        subscription: &SubscriptionPath,
        flow_control: FlowControl,
        mut deliver: F,
    ) -> Result<Self>
    where
        F: FnMut(ReceivedMessage) -> bool + Send + 'static,
    {
        let subscriber = connection.subscriber().await?;
        let mut stream = subscriber
            .subscribe(subscription.as_str())
            .set_max_outstanding_messages(flow_control.server_outstanding_messages())
            .build();
        let shutdown_token = stream.shutdown_token();
        let is_running = Arc::new(AtomicBool::new(true));
        let reader_is_running = Arc::clone(&is_running);
        let reader = tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match item {
                    Ok((message, handler)) => {
                        if !deliver(to_received_message(message, handler)) {
                            break;
                        }
                    }
                    Err(error) => {
                        eprintln!("fastpubsub-core: the streaming pull failed: {error}");
                        break;
                    }
                }
            }
            reader_is_running.store(false, Ordering::SeqCst);
        });
        Ok(Self {
            reader,
            shutdown_token,
            is_running,
        })
    }

    /// Return `true` until the stream stops because of an error or a
    /// shutdown.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Stop the stream and wait for the pending acks.
    ///
    /// The function waits at most `timeout`. It returns `true` if the
    /// stream stopped before the timeout.
    pub async fn shutdown(&self, timeout: Duration) -> bool {
        let is_complete = tokio::time::timeout(timeout, self.shutdown_token.shutdown())
            .await
            .is_ok();
        self.reader.abort();
        self.is_running.store(false, Ordering::SeqCst);
        is_complete
    }
}
