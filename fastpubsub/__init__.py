"""A high performance FastAPI-based message consumer framework for Google PubSub."""

from fast_depends import Depends

from fastpubsub.applications import FastPubSub
from fastpubsub.broker import PubSubBroker
from fastpubsub.datastructures import PullMessage, PushMessage
from fastpubsub.di import Body, Context, Header, Message, MessageAttributes, MessageData
from fastpubsub.middlewares import BaseMiddleware
from fastpubsub.publisher import Publisher
from fastpubsub.router import PubSubRouter
from fastpubsub.subscriber import Subscriber
from fastpubsub.testing import PubSubTestClient

__all__ = [
    "Depends",
    "FastPubSub",
    "PubSubBroker",
    "PubSubRouter",
    "Publisher",
    "Subscriber",
    "BaseMiddleware",
    "PullMessage",
    "PushMessage",
    "PubSubTestClient",
    "Message",
    "MessageData",
    "MessageAttributes",
    "Body",
    "Header",
    "Context",
]
