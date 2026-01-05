"""Aliases for extracting data from messages."""

from typing import Annotated

from fastpubsub.datastructures import PullMessage
from fastpubsub.di.annotations import Context

Message = Annotated[PullMessage, Context("message")]
MessageData = Annotated[bytes, Context("message.data")]
MessageAttributes = Annotated[dict[str, str], Context("message.attributes")]
