"""Title: Cloud Logging Integration

Demonstrates integration with Google Cloud Logging.

This example shows:
- Setting up Cloud Logging client
- Using FastPubSub logger with Cloud Logging
- Automatic log forwarding

Run with:
    fastpubsub run docs.snippets.observability.e1_04_cloud_logging:app

Requirements:
    - Set GOOGLE_APPLICATION_CREDENTIALS for GCP
    - Install google-cloud-logging: pip install google-cloud-logging
"""

# --8<-- [start:cloud_logging_full]
import google.cloud.logging

from fastpubsub import FastPubSub, Message, PubSubBroker
from fastpubsub.logger import logger

# Set up Cloud Logging
client = google.cloud.logging.Client()
client.setup_logging()


broker = PubSubBroker(project_id="your-project-id")
app = FastPubSub(broker)


# --8<-- [start:cloud_logging_handler]
@broker.subscriber(
    alias="cloud-handler",
    topic_name="events",
    subscription_name="events-subscription",
)
async def handler(message: Message):
    logger.info("Message processed")  # Sent to Cloud Logging


# --8<-- [end:cloud_logging_handler]
# --8<-- [end:cloud_logging_full]
