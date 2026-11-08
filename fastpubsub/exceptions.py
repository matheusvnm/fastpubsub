"""FastPubSub exceptions."""


class FastPubSubCLIException(Exception):
    """Base exception for FastPubSub CLI."""


class FastPubSubException(Exception):
    """Base exception for FastPubSub."""


class NotFoundError(FastPubSubException):
    """The topic or the subscription does not exist.

    A publish raises it when the topic does not exist and the publisher
    does not create topics.
    """


class Drop(Exception):
    """Exception to drop a message.

    Raising it results in a ack on the message.
    """


class Retry(Exception):
    """Exception to retry a message.

    Raising it results in a nack on the message.
    """
