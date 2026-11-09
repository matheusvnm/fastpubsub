//! Integration tests of the adapter against the Pub/Sub emulator.
//!
//! The tests run only when `PUBSUB_EMULATOR_HOST` is set. Each test makes
//! its own topic and subscription with a unique name, and removes them at
//! the end.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fastpubsub_google_pubsub::{
    Administration, Connection, Publisher, StreamingPull, SubscriptionPath, SubscriptionSettings,
    TopicPath,
};
use fastpubsub_messaging::{ErrorKind, FlowControl, OutgoingMessage, Result};
use tokio::sync::mpsc;

/// The project of the emulator.
const PROJECT: &str = "fastpubsub-test-project";

/// Make a connection to the emulator, or `None` when no emulator is set.
fn emulator_connection() -> Option<Connection> {
    let host = std::env::var("PUBSUB_EMULATOR_HOST").ok()?;
    Some(Connection::new(Some(format!("http://{host}"))))
}

/// Make a name that no other test run uses.
fn unique_name(prefix: &str) -> String {
    let nanoseconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    format!("{prefix}-{}-{nanoseconds}", std::process::id())
}

/// Make the topic path and the subscription path for a test.
///
/// # Errors
///
/// Returns an error if a path has a wrong format.
fn resource_paths(prefix: &str) -> Result<(TopicPath, SubscriptionPath)> {
    let name = unique_name(prefix);
    Ok((
        format!("projects/{PROJECT}/topics/{name}").parse()?,
        format!("projects/{PROJECT}/subscriptions/{name}").parse()?,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topic_errors_have_the_already_exists_and_not_found_kinds() {
    let Some(connection) = emulator_connection() else {
        return;
    };
    let administration = Administration::connect(&connection).await.unwrap();
    let (topic, _) = resource_paths("topic-errors").unwrap();

    administration.create_topic(&topic).await.unwrap();
    let duplicate = administration.create_topic(&topic).await.unwrap_err();
    administration.delete_topic(&topic).await.unwrap();
    let missing = administration.delete_topic(&topic).await.unwrap_err();

    assert_eq!(duplicate.kind(), ErrorKind::AlreadyExists);
    assert_eq!(missing.kind(), ErrorKind::NotFound);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn published_messages_arrive_with_their_attributes() {
    let Some(connection) = emulator_connection() else {
        return;
    };
    let administration = Administration::connect(&connection).await.unwrap();
    let (topic, subscription) = resource_paths("round-trip").unwrap();
    administration.create_topic(&topic).await.unwrap();
    administration
        .create_subscription(&subscription, &topic, &SubscriptionSettings::default())
        .await
        .unwrap();
    let (sender, mut deliveries) = mpsc::unbounded_channel();
    let stream = StreamingPull::start(
        &connection,
        &subscription,
        FlowControl::new(NonZeroU32::new(10).unwrap()),
        move |message| {
            let attribute = message.attributes.get("index").cloned();
            message.acknowledgement.ack();
            sender.send((message.id, message.data, attribute)).is_ok()
        },
    )
    .await
    .unwrap();
    let publisher = Publisher::connect(&connection, &topic).await.unwrap();

    let mut published_ids = Vec::new();
    for index in 0..3 {
        let message = OutgoingMessage {
            data: format!("payload-{index}").into(),
            attributes: HashMap::from([("index".to_owned(), index.to_string())]),
            ordering_key: None,
        };
        published_ids.push(publisher.publish(message).await.unwrap());
    }
    let mut arrived = Vec::new();
    while arrived.len() < 3 {
        let item = tokio::time::timeout(Duration::from_secs(30), deliveries.recv())
            .await
            .unwrap()
            .unwrap();
        arrived.push(item);
    }
    let is_stopped_in_time = stream.shutdown(Duration::from_secs(10)).await;
    administration
        .delete_subscription(&subscription)
        .await
        .unwrap();
    administration.delete_topic(&topic).await.unwrap();

    let mut received_ids: Vec<String> = arrived.iter().map(|item| item.0.clone()).collect();
    received_ids.sort();
    published_ids.sort();
    assert_eq!(received_ids, published_ids);
    arrived.sort_by(|left, right| left.2.cmp(&right.2));
    for (index, (_, data, attribute)) in arrived.iter().enumerate() {
        assert_eq!(data.as_ref(), format!("payload-{index}").as_bytes());
        assert_eq!(attribute.as_deref(), Some(index.to_string().as_str()));
    }
    assert!(is_stopped_in_time);
    assert!(!stream.is_running());
}
