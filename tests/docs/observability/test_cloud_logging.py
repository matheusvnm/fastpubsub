import importlib
import sys
import types
from unittest.mock import MagicMock

import pytest

from fastpubsub.testing import PubSubTestClient

SNIPPET_MODULE = "docs.snippets.observability.e1_02_cloud_logging"


class TestObservabilityCloudLogging:
    def install_fake_google_cloud_logging(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> MagicMock:
        sys.modules.pop(SNIPPET_MODULE, None)

        fake_client_cls = MagicMock()

        logging_module = types.ModuleType("google.cloud.logging")
        logging_module.Client = fake_client_cls
        monkeypatch.setitem(
            sys.modules, "google.cloud.logging", logging_module
        )

        cloud_module = types.ModuleType("google.cloud")
        cloud_module.__path__ = []
        cloud_module.logging = logging_module
        monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)

        google_module = types.ModuleType("google")
        google_module.__path__ = []
        google_module.cloud = cloud_module
        monkeypatch.setitem(sys.modules, "google", google_module)

        return fake_client_cls

    @pytest.mark.asyncio
    @pytest.mark.docs
    async def test_cloud_logging_snippet_publish_flow_with_stubbed_client(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_client_cls = self.install_fake_google_cloud_logging(monkeypatch)
        snippet = importlib.import_module(SNIPPET_MODULE)

        fake_client_cls.assert_called_once_with()
        fake_client_cls.return_value.setup_logging.assert_called_once_with()

        async with PubSubTestClient(snippet.broker) as client:
            await client.publish(topic="events", data={"event": "created"})
            processed_results = client.get_results()

        assert len(processed_results) == 1
        processed_result = next(iter(processed_results))

        assert processed_result.error is None
        assert processed_result.message.subscriber_name == "handler"
        assert processed_result.message.topic_name == "events"
