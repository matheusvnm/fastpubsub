from fastpubsub.middlewares import BasePublisherMiddleware, BaseSubscriberMiddleware
from fastpubsub.registrator import Registrator


class PubSubRouter(Registrator):
    def __init__(
        self,
        prefix: str = "",
        middlewares: list[type[BaseSubscriberMiddleware], type[BasePublisherMiddleware]] = [],
    ):
        super().__init__(middlewares=middlewares)
        self.prefix = prefix

    def set_project_id(self, project_id: str) -> None:
        if not (project_id and isinstance(project_id, str)):
            return  # TODO: Should raise an exception

        self.project_id = project_id
        for publisher in self.publishers.values():
            publisher.set_project_id(self.project_id)

        for subscriber in self.subscribers.values():
            subscriber.set_project_id(self.project_id)
