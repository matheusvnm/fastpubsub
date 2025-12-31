"""FastPubSub exceptions."""


class FastPubSubCLIException(Exception):
    """Base exception for FastPubSub CLI."""


class FastPubSubException(Exception):
    """Base exception for FastPubSub."""


class Drop(Exception):
    """Exception to drop a message.

    Raising it results in an ack on the message (message is acknowledged and discarded).
    """


class Retry(Exception):
    """Exception to retry a message.

    Raising it results in a nack on the message (message will be redelivered).
    """
