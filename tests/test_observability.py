import os
from unittest.mock import MagicMock

import pytest

from fastpubsub import observability
from fastpubsub.exceptions import FastPubSubException


@pytest.fixture
def agent() -> MagicMock:
    new_relic_agent = MagicMock()
    observability._new_relic_agent = new_relic_agent
    return new_relic_agent


class TestNewRelicAPMProvider:
    def test_start(self, agent: MagicMock):
        provider = observability.NewRelicProvider()

        provider.active = lambda: False
        provider.start()

        agent.initialize.assert_called_once()
        agent.register_application.assert_called_once_with(timeout=5.0)

    def test_start_only_once(self, agent: MagicMock):
        provider = observability.NewRelicProvider()

        provider.active = lambda: True
        provider.start()

        agent.initialize.assert_not_called()
        agent.register_application.assert_not_called()

    def test_start_no_module_found(self):
        observability._new_relic_agent = None
        with pytest.raises(FastPubSubException):
            observability.NewRelicProvider()

    def test_start_error_should_not_raise(self, agent: MagicMock):
        def raise_error(self):
            raise ValueError()

        provider = observability.NewRelicProvider()
        provider.active = raise_error
        provider.start()

        agent.initialize.assert_not_called()
        agent.register_application.assert_not_called()

    def test_shutdown(self, agent: MagicMock):
        provider = observability.NewRelicProvider()
        provider.shutdown()

        agent.shutdown_agent.assert_called_once()

    def test_shutdown_error_should_not_raise(self, agent: MagicMock):
        agent.shutdown_agent.side_effect = ValueError()
        provider = observability.NewRelicProvider()
        provider.shutdown()

    def test_start_trace(self, agent: MagicMock):
        name = "new_trace"
        provider = observability.NewRelicProvider()
        application = agent.application.return_value
        with provider.start_trace(name=name):
            agent.BackgroundTask.assert_called_once_with(application=application, name=name)
            agent.BackgroundTask.return_value.__enter__.assert_called_once()

    def test_start_trace_with_context(self, agent: MagicMock):
        name = "new_trace"
        context = {"key": "value"}
        application = agent.application.return_value
        provider = observability.NewRelicProvider()
        with provider.start_trace(name=name, context=context):
            agent.BackgroundTask.assert_called_once_with(application=application, name=name)
            agent.BackgroundTask.return_value.__enter__.assert_called_once()
            agent.accept_distributed_trace_headers.assert_called_once()

    def test_start_span(self, agent: MagicMock):
        name = "new_span"
        provider = observability.NewRelicProvider()
        with provider.start_span(name=name):
            agent.FunctionTrace.assert_called_once_with(name=name)
            agent.FunctionTrace.return_value.__enter__.assert_called_once()

    def test_set_distributed_trace_context(self, agent: MagicMock):
        provider = observability.NewRelicProvider()

        tracestate = "tracestate: congo=t61rcWkgMzE"
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

        provider.set_distributed_trace_context(
            {"traceparent": traceparent, "TraceState": tracestate}
        )

        agent.accept_distributed_trace_headers.assert_called_once_with(
            [
                (
                    "traceparent",
                    traceparent,
                ),
                (
                    "tracestate",
                    tracestate,
                ),
            ],
            transport_type="Queue",
        )

    def test_set_empty_distributed_trace_context(self, agent: MagicMock):
        provider = observability.NewRelicProvider()
        provider.set_distributed_trace_context({})
        provider.set_distributed_trace_context(None)
        agent.accept_distributed_trace_headers.assert_not_called()

    def test_get_distributed_trace_context(self, agent: MagicMock):
        def write(headers: list[tuple[str, str]]):
            headers.append(("key", "value"))

        agent.insert_distributed_trace_headers = write

        provider = observability.NewRelicProvider()
        headers = provider.get_distributed_trace_context()

        assert headers == {"key": "value"}

    def test_report_custom_event(self, agent: MagicMock):
        event_name = "MyCoolEvent"
        params = {"some": "param"}

        provider = observability.NewRelicProvider()
        provider.report_custom_event(event_name, params)

        agent.record_custom_event.assert_called_once_with(event_type=event_name, param=params)

    def test_report_custom_event_error_should_not_raise(self, agent: MagicMock):
        agent.record_custom_event.side_effect = ValueError()

        provider = observability.NewRelicProvider()
        provider.report_custom_event("event", {"param1": "value1"})

        agent.record_custom_event.assert_called_once()

    def test_report_log_record(self, agent: MagicMock):
        message = "hi"
        level = "info"
        timestamp = 155878

        provider = observability.NewRelicProvider()
        provider.report_log_record(
            message=message, level=level, timestamp=timestamp, attributes=None
        )
        agent.record_log_event.assert_called_once_with(
            message=message, level=level, timestamp=timestamp, attributes=None
        )

    def test_report_exception(self, agent: MagicMock):
        provider = observability.NewRelicProvider()
        try:
            raise ValueError("Some unexpected input was sent")
        except Exception as e:
            details = {"error": "something is not right"}
            provider.report_exception(
                exc_type=type(e), exc_value=e, traceback=None, attributes=details
            )
            agent.record_exception.assert_called_once_with(
                exc=type(e), value=e, tb=None, params=details
            )

    def test_add_custom_metric(self, agent: MagicMock):
        metric_value = 123
        metric_name = "exec_time"

        provider = observability.NewRelicProvider()
        provider.add_custom_metric(metric_name, metric_value)

        agent.record_custom_metric.assert_called_once_with(name=metric_name, value=metric_value)

    def test_get_trace_id(self, agent: MagicMock):
        expected_trace_id = "0af7651916cd43dd8448eb211c80319c"
        agent.current_trace_id.return_value = expected_trace_id

        provider = observability.NewRelicProvider()
        trace_id = provider.get_trace_id()

        assert trace_id == expected_trace_id

    def test_get_trace_id_inactive_agent(self, agent: MagicMock):
        agent.current_trace_id.return_value = None

        provider = observability.NewRelicProvider()
        assert not provider.get_trace_id()

    def test_get_span_id(self, agent: MagicMock):
        expected_span_id = "0af7651916cd43dd8448eb211c80319c"
        agent.current_span_id.return_value = expected_span_id

        provider = observability.NewRelicProvider()
        span_id = provider.get_span_id()

        assert span_id == expected_span_id

    def test_get_span_id_inactive_agent(self, agent: MagicMock):
        agent.current_span_id.return_value = None

        provider = observability.NewRelicProvider()
        assert not provider.get_span_id()

    def test_apm_active(self, agent: MagicMock):
        application = MagicMock()
        application.active = True
        agent.application.return_value = application

        provider = observability.NewRelicProvider()
        assert provider.active()

    def test_apm_inactive(self, agent: MagicMock):
        provider = observability.NewRelicProvider()

        application = MagicMock()
        application.active = False

        agent.application.return_value = application
        assert not provider.active()

        agent.application.return_value = None
        assert not provider.active()


class TestObservabilityTools:
    @pytest.fixture(scope="class")
    def mock_provider_cls(self) -> MagicMock:
        provider_cls = MagicMock()
        provider_cls.__name__ = "Provider"
        observability.PROVIDER_MAP["newrelic"] = provider_cls
        return provider_cls

    def test_get_provider_only_once(self, mock_provider_cls: MagicMock):
        provider = observability.get_apm_provider("newrelic")
        assert isinstance(provider, MagicMock)

        provider = observability.get_apm_provider("newrelic")
        assert isinstance(provider, MagicMock)

        mock_provider_cls.assert_called_once()

    def test_get_provider(self):
        provider = observability.get_apm_provider("newrelic")
        assert isinstance(provider, MagicMock)

        provider = observability.get_apm_provider()
        assert isinstance(provider, observability.NoOpProvider)

        observability.get_apm_provider.cache_clear()

        os.environ["FASTPUBSUB_APM_PROVIDER"] = "NEWRELIC"
        provider = observability.get_apm_provider()
        assert isinstance(provider, MagicMock)

        #  203-208, 212-213, 238
