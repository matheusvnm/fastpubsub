"""Dependency injection module for FastPubSub.

This module provides dependency injection capabilities using fast_depends,
allowing handlers to receive auto-unwrapped message data, headers, and
custom dependencies.

Key components:
- Body: Annotation for extracting fields from message body
- Header: Annotation for extracting message attributes
- Context: Low-level annotation for accessing the context repository
- Message, MessageData, MessageAttributes: Type aliases for handler params
"""

from fastpubsub.di.aliases import Message, MessageAttributes, MessageData
from fastpubsub.di.annotations import Body, Context, Header
from fastpubsub.di.handler import Handler
from fastpubsub.di.repository import context_repo

__all__ = [
    "context_repo",
    "Message",
    "MessageData",
    "MessageAttributes",
    "Context",
    "Body",
    "Header",
    "Handler",
]
