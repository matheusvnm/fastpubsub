//! The connection settings of the Pub/Sub clients.

use fastpubsub_messaging::Result;
use google_cloud_auth::credentials::anonymous::Builder as AnonymousCredentials;
use google_cloud_pubsub::client::{Publisher, Subscriber, SubscriptionAdmin, TopicAdmin};

use crate::TopicPath;
use crate::error::from_build;

/// Where the Pub/Sub clients connect.
///
/// With an emulator endpoint, the clients connect to the emulator with
/// anonymous credentials. Without one, the clients connect to the Pub/Sub
/// service with the default credentials of the environment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Connection {
    /// The URL of the Pub/Sub emulator, or `None` for the Pub/Sub service.
    emulator_endpoint: Option<String>,
}

impl Connection {
    /// Make a connection to the emulator at `emulator_endpoint`, or to the
    /// Pub/Sub service when `emulator_endpoint` is `None`.
    #[must_use]
    pub fn new(emulator_endpoint: Option<String>) -> Self {
        Self { emulator_endpoint }
    }

    /// Apply the emulator settings to a client builder.
    ///
    /// When the connection has an emulator endpoint, the function calls
    /// `use_emulator` with the builder and the endpoint. Otherwise the
    /// builder does not change.
    fn configure<B>(&self, builder: B, use_emulator: impl FnOnce(B, String) -> B) -> B {
        match &self.emulator_endpoint {
            Some(endpoint) => use_emulator(builder, endpoint.clone()),
            None => builder,
        }
    }

    /// Build a subscriber client.
    ///
    /// # Errors
    ///
    /// Returns an error if the client cannot get the credentials or cannot
    /// make the transport.
    pub(crate) async fn subscriber(&self) -> Result<Subscriber> {
        self.configure(Subscriber::builder(), |builder, endpoint| {
            builder
                .with_endpoint(endpoint)
                .with_credentials(AnonymousCredentials::new().build())
        })
        .build()
        .await
        .map_err(|error| from_build(&error))
    }

    /// Build a publisher client for one topic.
    ///
    /// # Errors
    ///
    /// Returns an error if the client cannot get the credentials or cannot
    /// make the transport.
    pub(crate) async fn publisher(&self, topic: &TopicPath) -> Result<Publisher> {
        self.configure(Publisher::builder(topic.as_str()), |builder, endpoint| {
            builder
                .with_endpoint(endpoint)
                .with_credentials(AnonymousCredentials::new().build())
        })
        .build()
        .await
        .map_err(|error| from_build(&error))
    }

    /// Build a topic administration client.
    ///
    /// # Errors
    ///
    /// Returns an error if the client cannot get the credentials or cannot
    /// make the transport.
    pub(crate) async fn topic_admin(&self) -> Result<TopicAdmin> {
        self.configure(TopicAdmin::builder(), |builder, endpoint| {
            builder
                .with_endpoint(endpoint)
                .with_credentials(AnonymousCredentials::new().build())
        })
        .build()
        .await
        .map_err(|error| from_build(&error))
    }

    /// Build a subscription administration client.
    ///
    /// # Errors
    ///
    /// Returns an error if the client cannot get the credentials or cannot
    /// make the transport.
    pub(crate) async fn subscription_admin(&self) -> Result<SubscriptionAdmin> {
        self.configure(SubscriptionAdmin::builder(), |builder, endpoint| {
            builder
                .with_endpoint(endpoint)
                .with_credentials(AnonymousCredentials::new().build())
        })
        .build()
        .await
        .map_err(|error| from_build(&error))
    }
}
