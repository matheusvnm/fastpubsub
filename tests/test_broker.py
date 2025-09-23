

import pytest
from fastpubsub.baserouter import BaseRouter
from fastpubsub.broker import PubSubBroker
from fastpubsub.exceptions import FastPubSubException


def test_mandatory_project_id():

    with pytest.raises(FastPubSubException):
       PubSubBroker(project_id=None)

    with pytest.raises(FastPubSubException):
       PubSubBroker(project_id="   ")

    with pytest.raises(FastPubSubException):
       PubSubBroker(project_id="")

    with pytest.raises(FastPubSubException):
       PubSubBroker(project_id=213)


def test_enforce_pubsub_router():

    class SomeOtherRouter(BaseRouter): ...

    with pytest.raises(FastPubSubException):
       broker = PubSubBroker(project_id="cloud-project")
       broker.include_router(SomeOtherRouter())
