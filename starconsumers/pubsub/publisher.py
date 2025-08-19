import orjson
from concurrent.futures import Future
from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub_v1 import PublisherClient
from google.cloud.pubsub_v1.types import PublisherOptions

from starconsumers import observability

class PubSubPublisher:

    def __init__(self, project_id: str, topic_name: str):
        self.topic = PublisherClient.topic_path(project=project_id, topic=topic_name)        

    def create_topic(self):
        """
        Creates the configured Pub/Sub topic.
        """

        client = PublisherClient()
        try:
            client.create_topic(name=self.topic,)
            print("Created topic sucessfully.")
        except AlreadyExists:
            print("The topic already exists.")

    def publish(self, *, data: dict, attributes: dict = {}, ordering_key: str = ""):
        """
        Publishes some data on a configured Pub/Sub topic.
        It considers that the topic is already created
        """
        apm = observability.get_apm_provider()
        attributes = {} if attributes is None else attributes
        headers = apm.get_distributed_trace_context()
        headers.update(attributes)

        ordered = True if ordering_key else False
        publisher_options = PublisherOptions(enable_message_ordering=ordered)
        client = PublisherClient(publisher_options=publisher_options)
        
        try:
            encoded_data = orjson.dumps(data)
            future: Future = client.publish(topic=self.topic, data=encoded_data, ordering_key=ordering_key, **headers)
            message_id = future.result()
            print(f"Message published for topic {self.topic} with id {message_id}")
            print(f"We sent {data} with metadata {attributes}")
        except Exception as e:
            print(f"Publisher failure: {str(e)}")
            raise
