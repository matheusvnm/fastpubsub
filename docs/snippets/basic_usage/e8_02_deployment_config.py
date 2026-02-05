import os

from fastpubsub import FastPubSub, PubSubBroker

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
if not PROJECT_ID:
    raise RuntimeError("GCP_PROJECT_ID environment variable not set.")

broker = PubSubBroker(project_id=PROJECT_ID)
app = FastPubSub(broker)
