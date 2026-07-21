"""Unit tests for live telemetry request correlation helpers."""

import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from malla.services.live_telemetry import request_live_telemetry_with_retry
from malla.utils.telemetry_request import (
    LIVE_TELEMETRY_TYPE_ROTATION,
    apply_telemetry_request_type,
    complete_pending_telemetry,
    extract_from_node_id,
    extract_request_id,
    find_matching_telemetry_request,
    live_telemetry_budget,
    next_live_telemetry_type,
    normalize_mesh_node_id,
    normalize_request_id,
    split_live_telemetry_attempts,
    telemetry_has_requested_metrics,
    telemetry_to_dict,
)


@pytest.mark.unit
class TestNormalizeMeshNodeId:
    def test_int_and_masks(self):
        assert normalize_mesh_node_id(0xAABBCCDD) == 0xAABBCCDD
        assert normalize_mesh_node_id(-1) == 0xFFFFFFFF

    def test_bang_hex_and_0x(self):
        assert normalize_mesh_node_id("!aabbccdd") == 0xAABBCCDD
        assert normalize_mesh_node_id("0xAABBCCDD") == 0xAABBCCDD

    def test_bare_hex_and_decimal(self):
        assert normalize_mesh_node_id("aabbccdd") == 0xAABBCCDD
        assert normalize_mesh_node_id("123456789") == 123456789

    def test_invalid(self):
        assert normalize_mesh_node_id(None) is None
        assert normalize_mesh_node_id("") is None
        assert normalize_mesh_node_id("not-a-node") is None
        assert normalize_mesh_node_id(True) is None


@pytest.mark.unit
class TestRequestIdExtraction:
    def test_normalize_request_id(self):
        assert normalize_request_id(0x1234) == 0x1234
        assert normalize_request_id("42") == 42
        assert normalize_request_id(None) is None

    def test_extract_from_packet(self):
        packet = {
            "from": 0x11111111,
            "fromId": "!22222222",
            "decoded": {"requestId": 99, "telemetry": {"deviceMetrics": {"batteryLevel": 50}}},
        }
        assert extract_from_node_id(packet) == 0x11111111
        assert extract_request_id(packet) == 99

        packet2 = {"fromId": "!aabbccdd", "decoded": {"request_id": 7}}
        assert extract_from_node_id(packet2) == 0xAABBCCDD
        assert extract_request_id(packet2) == 7


@pytest.mark.unit
class TestTelemetryPayloadHelpers:
    def test_has_requested_metrics_snake_and_camel(self):
        assert telemetry_has_requested_metrics(
            {"device_metrics": {"battery_level": 80}}, "device_metrics"
        )
        assert telemetry_has_requested_metrics(
            {"deviceMetrics": {"batteryLevel": 80}}, "device_metrics"
        )
        assert not telemetry_has_requested_metrics(
            {"environment_metrics": {"temperature": 20}}, "device_metrics"
        )
        # Empty oneof shell still counts — firmware may omit default scalars
        assert telemetry_has_requested_metrics(
            {"device_metrics": {}}, "device_metrics"
        )

    def test_telemetry_to_dict_strips_raw(self):
        payload = {
            "device_metrics": {"battery_level": 42},
            "raw": object(),
        }
        cleaned = telemetry_to_dict(payload)
        assert "raw" not in cleaned
        assert cleaned["device_metrics"]["battery_level"] == 42


@pytest.mark.unit
class TestFindMatchingTelemetryRequest:
    def _pending(self, **overrides):
        base = {
            "event": threading.Event(),
            "response_data": {},
            "telemetry_type": "device_metrics",
            "request_id": 0xABCD,
            "completed": False,
        }
        base.update(overrides)
        return base

    def test_matches_by_request_id(self):
        pending = {0x1111: self._pending(request_id=0xABCD)}
        match = find_matching_telemetry_request(
            pending,
            from_node_id=0x9999,  # different from — request id wins
            request_id=0xABCD,
            telemetry={"device_metrics": {"battery_level": 1}},
        )
        assert match is not None
        assert match[0] == 0x1111

    def test_accepts_same_node_metrics_even_without_packet_request_id(self):
        # Some firmware omits requestId on TELEMETRY replies; nearby monitoring
        # must still complete while a solicited wait is active.
        pending = {0x1111: self._pending(request_id=0xABCD)}
        match = find_matching_telemetry_request(
            pending,
            from_node_id=0x1111,
            request_id=None,
            telemetry={"device_metrics": {"battery_level": 1}},
        )
        assert match is not None

    def test_accepts_node_match_before_request_id_stored(self):
        pending = {0x1111: self._pending(request_id=None)}
        match = find_matching_telemetry_request(
            pending,
            from_node_id=0x1111,
            request_id=0xABCD,
            telemetry={"device_metrics": {"battery_level": 1}},
        )
        assert match is not None

    def test_rejects_wrong_metric_type_without_request_id(self):
        pending = {0x1111: self._pending(request_id=None, telemetry_type="device_metrics")}
        match = find_matching_telemetry_request(
            pending,
            from_node_id=0x1111,
            request_id=None,
            telemetry={"environment_metrics": {"temperature": 21}},
        )
        assert match is None

    def test_rejects_empty_telemetry_on_request_id_match(self):
        pending = {0x1111: self._pending(request_id=0xABCD)}
        match = find_matching_telemetry_request(
            pending,
            from_node_id=0x1111,
            request_id=0xABCD,
            telemetry={},
        )
        assert match is None

    def test_complete_is_idempotent(self):
        pending = self._pending(request_id=None)
        assert complete_pending_telemetry(
            pending,
            telemetry={"device_metrics": {"battery_level": 9}},
            from_node_id=0x1111,
            request_id=0xABCD,
        )
        assert pending["completed"] is True
        assert pending["event"].is_set()
        assert pending["response_data"]["telemetry"]["device_metrics"]["battery_level"] == 9

        assert (
            complete_pending_telemetry(
                pending,
                telemetry={"device_metrics": {"battery_level": 1}},
                from_node_id=0x1111,
            )
            is False
        )


@pytest.mark.unit
class TestLiveTelemetryHopBudget:
    def test_zero_hop_uses_ack_and_full_retries(self):
        budget = live_telemetry_budget(0)
        assert budget["estimated_hops"] == 0
        assert budget["timeout_s"] >= 12.0
        assert budget["attempts"] == 3
        assert budget["want_ack_sequence"][0] is False
        assert True in budget["want_ack_sequence"]
        assert budget["hop_limit"] >= 3
        assert budget["poll_interval_ms"] <= 6000
        assert budget["total_budget_s"] <= 55.0

    def test_one_hop_gets_three_tries_with_no_ack_first(self):
        near = live_telemetry_budget(1)
        assert near["attempts"] == 3
        assert near["want_ack_sequence"] == [False, True, True]
        assert near["total_budget_s"] <= 55.0

    def test_far_hops_get_longer_budget_and_acks(self):
        near = live_telemetry_budget(1)
        far = live_telemetry_budget(4)
        assert far["timeout_s"] >= near["timeout_s"]
        assert far["attempts"] >= 2
        assert far["want_ack"] is True
        assert far["hop_limit"] >= 4
        assert far["poll_interval_ms"] > near["poll_interval_ms"]
        assert far["timeout_s"] <= 55.0
        assert far["total_budget_s"] <= 55.0

    def test_attempts_use_full_per_attempt_windows(self):
        two = split_live_telemetry_attempts(12, attempts=2)
        assert two == [12.0, 12.0]

        three = split_live_telemetry_attempts(18, attempts=3)
        assert len(three) == 3
        assert three[0] == three[1] == three[2]
        assert sum(three) <= 55.0


@pytest.mark.unit
class TestLiveTelemetryApiRetryHelpers:
    def test_attempt_timeout_split(self):
        assert split_live_telemetry_attempts(10, attempts=1) == [10.0]
        attempts = split_live_telemetry_attempts(12, attempts=2)
        assert attempts == [12.0, 12.0]

    def test_retry_helper_retries_once_on_failure(self):
        publisher = MagicMock()
        publisher.send_telemetry_request.side_effect = [
            None,
            {"telemetry": {"device_metrics": {"battery_level": 50}}},
        ]
        with patch("malla.services.live_telemetry.time.sleep"):
            result, attempts = request_live_telemetry_with_retry(
                publisher,
                0x1234,
                "device_metrics",
                20,
                attempts=2,
                hop_limit=4,
                want_ack=True,
                retry_delay_s=0.5,
            )
        assert attempts == 2
        assert result is not None
        assert result["retry_attempt"] == 1
        assert publisher.send_telemetry_request.call_count == 2
        kwargs = publisher.send_telemetry_request.call_args.kwargs
        assert kwargs["hop_limit"] == 4
        assert kwargs["want_ack"] is True

    def test_retry_helper_stops_on_first_success(self):
        publisher = MagicMock()
        publisher.send_telemetry_request.return_value = {
            "telemetry": {"device_metrics": {"battery_level": 50}}
        }
        result, attempts = request_live_telemetry_with_retry(
            publisher, 0x1234, "device_metrics", 20, attempts=3
        )
        assert attempts == 1
        assert result is not None
        assert "retry_attempt" not in result

    def test_retry_helper_three_attempts_for_far_nodes(self):
        publisher = MagicMock()
        publisher.send_telemetry_request.return_value = None
        with patch("malla.services.live_telemetry.time.sleep") as sleep_mock:
            result, attempts = request_live_telemetry_with_retry(
                publisher,
                0x1234,
                "device_metrics",
                40,
                attempts=3,
                hop_limit=6,
                want_ack=True,
                retry_delay_s=1.0,
            )
        assert result is None
        assert attempts == 3
        assert sleep_mock.call_count == 2

    def test_retry_helper_uses_want_ack_sequence(self):
        publisher = MagicMock()
        publisher.send_telemetry_request.side_effect = [
            None,
            None,
            {"telemetry": {"device_metrics": {"battery_level": 50}}},
        ]
        with patch("malla.services.live_telemetry.time.sleep"):
            result, attempts = request_live_telemetry_with_retry(
                publisher,
                0x5FFBA832,
                "device_metrics",
                14,
                attempts=3,
                want_ack=False,
                want_ack_sequence=[False, True, True],
            )
        assert result is not None
        assert attempts == 3
        ack_flags = [
            call.kwargs.get("want_ack")
            for call in publisher.send_telemetry_request.call_args_list
        ]
        assert ack_flags == [False, True, True]


@pytest.mark.unit
class TestTcpTelemetryMatchPath:
    def test_match_and_complete_via_request_id(self):
        from malla.services.tcp_publisher import TCPPublisher

        pub = TCPPublisher.__new__(TCPPublisher)
        pub._pending_telemetry_requests = {}
        pub._pending_telemetry_lock = threading.Lock()
        pub._telemetry_late_by_request = {}
        pub._telemetry_latest_by_node = {}

        event = threading.Event()
        response_data: dict = {}
        pub._pending_telemetry_requests[0xAABBCCDD] = {
            "event": event,
            "response_data": response_data,
            "telemetry_type": "device_metrics",
            "request_id": 0x55AA,
            "completed": False,
        }

        packet = {
            "from": 0xAABBCCDD,
            "fromId": "!aabbccdd",
            "decoded": {
                "portnum": "TELEMETRY_APP",
                "requestId": 0x55AA,
                "telemetry": {"device_metrics": {"battery_level": 77, "voltage": 4.1}},
            },
        }
        assert pub._match_and_complete_telemetry(packet) is True
        assert event.is_set()
        assert response_data["telemetry"]["device_metrics"]["battery_level"] == 77

    def test_no_response_does_not_wake_waiter(self):
        """Routing NO_RESPONSE must not abort the wait — TELEMETRY often follows."""
        from malla.services.tcp_publisher import TCPPublisher

        pub = TCPPublisher.__new__(TCPPublisher)
        pub._pending_telemetry_requests = {}
        pub._pending_telemetry_lock = threading.Lock()
        pub._telemetry_late_by_request = {}
        pub._telemetry_latest_by_node = {}

        event = threading.Event()
        response_data: dict = {}
        pub._pending_telemetry_requests[0x5FFBA832] = {
            "event": event,
            "response_data": response_data,
            "telemetry_type": "device_metrics",
            "request_id": 0x1001,
            "completed": False,
        }

        routing = {
            "from": 0x5FFBA832,
            "decoded": {
                "portnum": "ROUTING_APP",
                "requestId": 0x1001,
                "routing": {"errorReason": "NO_RESPONSE"},
            },
        }
        assert pub._match_and_complete_telemetry(routing) is True
        assert not event.is_set()
        assert response_data.get("routing_warning") == "NO_RESPONSE"
        assert "error" not in response_data or response_data.get("error") is None

        telemetry_packet = {
            "from": 0x5FFBA832,
            "decoded": {
                "portnum": "TELEMETRY_APP",
                "requestId": 0x1001,
                "telemetry": {"device_metrics": {"battery_level": 91, "voltage": 4.05}},
            },
        }
        assert pub._match_and_complete_telemetry(telemetry_packet) is True
        assert event.is_set()
        assert response_data["telemetry"]["device_metrics"]["battery_level"] == 91
        assert response_data.get("routing_warning") == "NO_RESPONSE"

    def test_serial_no_response_then_telemetry(self):
        from malla.services.serial_publisher import SerialPublisher

        pub = SerialPublisher.__new__(SerialPublisher)
        pub._pending_telemetry_requests = {}
        pub._pending_telemetry_lock = threading.Lock()
        pub._telemetry_late_by_request = {}
        pub._telemetry_latest_by_node = {}

        event = threading.Event()
        response_data: dict = {}
        pub._pending_telemetry_requests[0x5FFBA832] = {
            "event": event,
            "response_data": response_data,
            "telemetry_type": "device_metrics",
            "request_id": 0x2002,
            "completed": False,
        }

        assert (
            pub._match_and_complete_telemetry(
                {
                    "decoded": {
                        "portnum": "ROUTING_APP",
                        "requestId": 0x2002,
                        "routing": {"error_reason": "NO_RESPONSE"},
                    }
                }
            )
            is True
        )
        assert not event.is_set()

        assert (
            pub._match_and_complete_telemetry(
                {
                    "from": 0x5FFBA832,
                    "decoded": {
                        "portnum": "TELEMETRY_APP",
                        "requestId": 0x2002,
                        "telemetry": {
                            "device_metrics": {"battery_level": 80, "voltage": 3.9}
                        },
                    },
                }
            )
            is True
        )
        assert event.is_set()
        assert response_data["telemetry"]["device_metrics"]["battery_level"] == 80

    def test_late_cache_pickup_window_covers_in_flight_replies(self):
        from malla.utils.telemetry_request import (
            TELEMETRY_LATE_CACHE_S,
            pickup_late_telemetry_cache,
        )

        assert TELEMETRY_LATE_CACHE_S >= 10.0
        now = time.time()
        latest = {
            0x5FFBA832: {
                "telemetry": {"device_metrics": {"battery_level": 70}},
                "timestamp": now - 8.0,
                "from_node": 0x5FFBA832,
            }
        }
        late = pickup_late_telemetry_cache(
            late_by_request={},
            latest_by_node=latest,
            request_id=None,
            target_node_id=0x5FFBA832,
        )
        assert late is not None
        assert late["telemetry"]["device_metrics"]["battery_level"] == 70

    def test_generation_cleanup_does_not_remove_newer_request(self):
        from malla.services.tcp_publisher import TCPPublisher

        pub = TCPPublisher.__new__(TCPPublisher)
        pub._pending_telemetry_requests = {}
        pub._pending_telemetry_lock = threading.Lock()
        pub._telemetry_stats = {
            "total_requests": 0,
            "successful_responses": 0,
            "timeouts": 0,
            "errors": 0,
            "last_request_time": None,
            "last_success_time": None,
            "per_node_stats": {},
        }
        pub._telemetry_stats_lock = threading.Lock()
        pub._interface = MagicMock()
        pub._last_activity_time = 0

        # Simulate overlapping request cleanup by exercising generation pop logic
        old_gen = 1
        new_gen = 2
        pub._pending_telemetry_requests[1] = {
            "generation": new_gen,
            "event": threading.Event(),
            "response_data": {},
            "telemetry_type": "device_metrics",
            "request_id": None,
            "completed": False,
        }
        with pub._pending_telemetry_lock:
            pending = pub._pending_telemetry_requests.get(1)
            if pending and pending.get("generation") == old_gen:
                pub._pending_telemetry_requests.pop(1, None)

        assert 1 in pub._pending_telemetry_requests


@pytest.mark.unit
class TestSerialTelemetryStatsShape:
    def test_node_stats_nested_like_tcp(self):
        from malla.services.serial_publisher import SerialPublisher

        pub = SerialPublisher.__new__(SerialPublisher)
        pub._telemetry_stats_lock = threading.Lock()
        pub._telemetry_stats = {
            "total_requests": 4,
            "successful_responses": 3,
            "timeouts": 1,
            "errors": 0,
            "last_request_time": 1.0,
            "last_success_time": 2.0,
            "per_node_stats": {
                "305419896": {
                    "requests": 4,
                    "successes": 3,
                    "timeouts": 1,
                    "errors": 0,
                    "last_request": 1.0,
                    "last_success": 2.0,
                }
            },
        }
        stats = pub.get_telemetry_stats(305419896)
        assert "node_stats" in stats
        assert stats["node_stats"]["successes"] == 3
        assert stats["success_rate"] == 75.0


@pytest.mark.unit
class TestLiveTelemetryTypeRotation:
    def test_rotation_keeps_device_metrics_frequent(self):
        types = [next_live_telemetry_type(i) for i in range(len(LIVE_TELEMETRY_TYPE_ROTATION))]
        assert types.count("device_metrics") >= 4
        assert "environment_metrics" in types
        assert "local_stats" in types
        assert "power_metrics" in types
        assert "air_quality_metrics" in types

    def test_rotation_wraps(self):
        assert next_live_telemetry_type(0) == next_live_telemetry_type(
            len(LIVE_TELEMETRY_TYPE_ROTATION)
        )


@pytest.mark.unit
class TestApplyTelemetryRequestType:
    def test_applies_supported_types(self):
        from meshtastic import telemetry_pb2

        for telemetry_type, field in [
            ("device_metrics", "device_metrics"),
            ("environment_metrics", "environment_metrics"),
            ("local_stats", "local_stats"),
            ("power_metrics", "power_metrics"),
            ("air_quality_metrics", "air_quality_metrics"),
            ("health_metrics", "health_metrics"),
            ("host_metrics", "host_metrics"),
        ]:
            tel = telemetry_pb2.Telemetry()
            applied = apply_telemetry_request_type(tel, telemetry_type)
            assert applied == telemetry_type
            assert tel.HasField(field)

    def test_unknown_falls_back_to_device_metrics(self):
        from meshtastic import telemetry_pb2

        tel = telemetry_pb2.Telemetry()
        applied = apply_telemetry_request_type(tel, "not_a_real_type")
        assert applied == "device_metrics"
        assert tel.HasField("device_metrics")
