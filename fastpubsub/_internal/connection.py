"""The Pub/Sub endpoint and the resource paths of a project."""

import os

EMULATOR_HOST_VARIABLE = "PUBSUB_EMULATOR_HOST"


class Connection:
    """The Pub/Sub endpoint and the resource paths of one project.

    The endpoint comes from ``PUBSUB_EMULATOR_HOST``. When the variable has
    a value, the core connects to the emulator with anonymous credentials.
    Otherwise it connects to the Pub/Sub service with the default
    credentials of the environment.
    """

    def __init__(self, project_id: str) -> None:
        """Initializes the Connection.

        Args:
            project_id: The Google Cloud project of the resources.
        """
        self.project_id = project_id

    @staticmethod
    def read_endpoint() -> str | None:
        """Reads the emulator endpoint.

        Returns:
            The emulator URL, or None for the Pub/Sub service.
        """
        emulator_host = os.getenv(EMULATOR_HOST_VARIABLE)
        if not emulator_host:
            return None
        return f"http://{emulator_host}"

    @staticmethod
    def is_emulator() -> bool:
        """Checks if the clients connect to the emulator.

        Returns:
            True when ``PUBSUB_EMULATOR_HOST`` has a value.
        """
        return bool(os.getenv(EMULATOR_HOST_VARIABLE))

    def topic_path(self, topic_name: str) -> str:
        """Builds the full path of a topic of this project.

        Args:
            topic_name: The name of the topic.

        Returns:
            The path in the format ``projects/<project>/topics/<name>``.
        """
        return f"projects/{self.project_id}/topics/{topic_name}"

    def subscription_path(self, subscription_name: str) -> str:
        """Builds the full path of a subscription of this project.

        Args:
            subscription_name: The name of the subscription.

        Returns:
            The path in the format
            ``projects/<project>/subscriptions/<name>``.
        """
        return f"projects/{self.project_id}/subscriptions/{subscription_name}"
