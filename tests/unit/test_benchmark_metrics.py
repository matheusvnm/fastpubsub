import pytest

from benchmarks.worker import calculate_percentile_ms
from benchmarks.workload import SENT_AT_ATTRIBUTE, Recorder


class TestRecorder:
    def test_counts_repeated_message_id_as_redelivery(self):
        recorder = Recorder()

        recorder.record("message-1", {})
        recorder.record("message-2", {})
        recorder.record("message-1", {})

        events, redeliveries, _ = recorder.read()
        assert events == 3
        assert redeliveries == 1

    def test_reset_keeps_message_ids_seen_in_warmup(self):
        recorder = Recorder()
        recorder.record("message-1", {})

        recorder.reset()
        recorder.record("message-1", {})

        events, redeliveries, _ = recorder.read()
        assert events == 1
        assert redeliveries == 1

    def test_records_latency_only_with_sent_at_attribute(self):
        recorder = Recorder()

        recorder.record("message-1", {SENT_AT_ATTRIBUTE: "0"})
        recorder.record("message-2", {})

        _, _, latencies_ns = recorder.read()
        assert len(latencies_ns) == 1
        assert latencies_ns[0] > 0


class TestCalculatePercentile:
    @pytest.mark.parametrize(
        ("rank", "expected_ms"),
        [(50, 50.0), (95, 95.0), (99, 99.0), (100, 100.0)],
    )
    def test_uses_nearest_rank(self, rank: float, expected_ms: float):
        latencies_ns = [value * 1_000_000 for value in range(1, 101)]

        assert calculate_percentile_ms(latencies_ns, rank) == expected_ms

    def test_no_values_returns_zero(self):
        assert calculate_percentile_ms([], 99) == 0.0
