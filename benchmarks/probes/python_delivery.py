"""Measure the delivery delay of the Google Python client.

The probe uses the Google Python client without fastpubsub. It measures
the publish round trip, then the delivery delay for a burst of messages
and for a slow, steady rate. Run it from outside Google Cloud to see the
effect of the blocking receipt modack. Run it in the same region to see
the effect without network latency.

Environment variables:
    BENCHMARK_PROJECT_ID: The Google Cloud project.
"""

import os
import statistics
import threading
import time
import uuid
from concurrent.futures import wait

from google.cloud import pubsub_v1

BURST_SIZE = 500
STEADY_COUNT = 40
STEADY_INTERVAL_SECONDS = 0.2


def run_probe() -> None:
    """Publish a burst and a steady stream and print the delays."""
    project = os.environ["BENCHMARK_PROJECT_ID"]
    suffix = uuid.uuid4().hex[:8]
    topic = f"projects/{project}/topics/probe-{suffix}"
    subscription = f"projects/{project}/subscriptions/probe-{suffix}"
    publisher = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    publisher.create_topic(name=topic)
    subscriber.create_subscription(
        name=subscription, topic=topic, ack_deadline_seconds=60
    )
    delays: list[float] = []
    lock = threading.Lock()

    def receive(message: pubsub_v1.subscriber.message.Message) -> None:
        delay = time.time() - float(message.attributes["sent"])
        message.ack()
        with lock:
            delays.append(delay * 1000)

    def take_delays() -> list[float]:
        with lock:
            taken = sorted(delays)
            delays.clear()
        return taken

    stream = subscriber.subscribe(
        subscription,
        receive,
        flow_control=pubsub_v1.types.FlowControl(max_messages=1000),
    )
    try:
        time.sleep(3)
        round_trips = []
        for _ in range(20):
            start = time.time()
            publisher.publish(
                topic, b"x" * 200, sent=str(time.time())
            ).result()
            round_trips.append((time.time() - start) * 1000)
        time.sleep(3)
        take_delays()
        futures = [
            publisher.publish(topic, b"x" * 200, sent=str(time.time()))
            for _ in range(BURST_SIZE)
        ]
        wait(futures)
        time.sleep(8)
        burst = take_delays()
        for _ in range(STEADY_COUNT):
            publisher.publish(topic, b"x" * 200, sent=str(time.time()))
            time.sleep(STEADY_INTERVAL_SECONDS)
        time.sleep(5)
        steady = take_delays()
    finally:
        stream.cancel()
        subscriber.delete_subscription(subscription=subscription)
        publisher.delete_topic(topic=topic)

    print(f"publish round trip p50: {statistics.median(round_trips):.0f} ms")
    for name, values in (("burst", burst), ("steady", steady)):
        if values:
            print(
                f"{name} delivery: p50={values[len(values) // 2]:.0f} ms "
                f"p99={values[int(len(values) * 0.99)]:.0f} ms "
                f"({len(values)} messages)"
            )


if __name__ == "__main__":
    run_probe()
