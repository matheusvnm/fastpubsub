import argparse
import json
import os
import sys

from starconsumers.pubsub.publisher import PubSubPublisher

# --- Path Manipulation (Keep this for standalone execution from 'scripts' dir) ---
current_script_dir = os.path.dirname(os.path.abspath(__file__))
scripts_root = os.path.join(current_script_dir, os.pardir)
project_root = os.path.join(scripts_root, os.pardir)
sys.path.insert(0, project_root)


# --- Imports from the project ---


def _send_message(message: dict, topic_name: str):
    """
    Sends a message to the specified Pub/Sub topic.
    """

    publisher = PubSubPublisher(project_id="starconsumers-pubsub-local", topic_name=topic_name)

    print(f"Attempting to create/verify topic: {topic_name}")
    try:
        publisher.create_topic()
        print(f"Topic '{topic_name}' created or verified successfully.")
    except Exception as e:
        print(f"ERROR: Failed to create or verify topic '{topic_name}': {e}", file=sys.stderr)
        sys.exit(1)

    print(f"Publishing message to topic '{topic_name}':\n {message}")
    try:
        publisher.publish(data=message)
        print("Message processing initiated successfully.")
    except Exception as e:
        print(f"ERROR: Failed to publish message to topic '{topic_name}': {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Send a JSON message to a Google Cloud Pub/Sub topic."
    )
    parser.add_argument(
        "-m",
        "--message",
        type=str,
        required=True,
        help="Must be a valid JSON string.",
    )
    parser.add_argument(
        "-t",
        "--topic",
        type=str,
        help=f"The name of the Pub/Sub topic to send the message to. "
    )

    args = parser.parse_args()
    message_payload = {}
    try:
        message_payload = json.loads(args.message)
        if not isinstance(message_payload, dict):
            raise ValueError("JSON input must be a dictionary.")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format provided for message: {args.message}")
        print(f"Details: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Error: {e}")
        sys.exit(1)

    _send_message(message_payload, args.topic)
