//! The publisher of one topic.

use std::future::Future;

use fastpubsub_messaging::{OutgoingMessage, Result};
use google_cloud_pubsub::client::Publisher as GooglePublisher;
use google_cloud_pubsub::model::Message;

use crate::error::from_publish;
use crate::{Connection, TopicPath};

/// Publish messages to one topic.
///
/// The Google client collects the messages in batches before it sends
/// them.
#[derive(Debug)]
pub struct Publisher {
    /// The batching publisher of the Google client.
    inner: GooglePublisher,
}

impl Publisher {
    /// Build a publisher for one topic.
    ///
    /// # Errors
    ///
    /// Returns an error if the client cannot get the credentials or cannot
    /// make the transport.
    pub async fn connect(connection: &Connection, topic: &TopicPath) -> Result<Self> {
        Ok(Self {
            inner: connection.publisher(topic).await?,
        })
    }

    /// Add one message to the next batch.
    ///
    /// Call this function inside the context of a tokio runtime, because
    /// the Google client starts its batch task there. The function does not
    /// block. The returned future resolves to the message ID.
    ///
    /// # Errors
    ///
    /// The returned future resolves to an error of the kind `NotFound` if
    /// the topic does not exist, or of the kind `Other` if the batch fails
    /// for a different reason.
    pub fn publish(
        &self,
        message: OutgoingMessage,
    ) -> impl Future<Output = Result<String>> + Send + 'static {
        let mut model = Message::new()
            .set_data(message.data)
            .set_attributes(message.attributes);
        if let Some(ordering_key) = message.ordering_key {
            model = model.set_ordering_key(ordering_key);
        }
        let future = self.inner.publish(model);
        async move { future.await.map_err(|error| from_publish(&error)) }
    }

    /// Send all pending batches and wait for the results.
    pub async fn flush(&self) {
        self.inner.flush().await;
    }
}
