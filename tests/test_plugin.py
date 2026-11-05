#!/usr/bin/env python3
"""The plugin chooses keys. The Lua script still stores one key."""

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from osprey_velocity.plugin import (  # noqa: E402
    Config,
    Field,
    RecordingStream,
    RedisCounter,
    config_from_env,
    count,
    record,
    route,
    stream_from_jsonl,
)
from test_counter import Redis  # noqa: E402

CONFIG = Config(
    fields=(
        Field("user.id", "user", "message_sent", True),
        Field("ip", "ip", "request", False),
    ),
    traffic_source_path="traffic_source",
    api_key_path="api_key",
    retention=2,
)

HUMAN = {"user": {"id": 42}, "ip": "10.0.0.1"}
API = {
    "user": {"id": 42},
    "ip": "10.0.0.1",
    "traffic_source": "api",
    "api_key": "bot-9",
}


class RouteTest(unittest.TestCase):
    def test_human_visit_ticks_the_user_and_the_ip(self):
        self.assertEqual(
            route(HUMAN, CONFIG),
            ["user:42:message_sent", "ip:10.0.0.1:request"],
        )

    def test_missing_traffic_source_counts_as_a_human(self):
        self.assertEqual(route(dict(HUMAN), CONFIG), route(HUMAN, CONFIG))

    def test_api_call_rewrites_only_the_marked_field(self):
        self.assertEqual(
            route(API, CONFIG),
            ["api-client:bot-9:message_sent", "ip:10.0.0.1:request"],
        )

    def test_missing_field_is_skipped(self):
        self.assertEqual(
            route({"ip": "10.0.0.1"}, CONFIG),
            ["ip:10.0.0.1:request"],
        )

    def test_api_call_without_a_key_skips_the_rewritten_field(self):
        data = {"user": {"id": 42}, "ip": "10.0.0.1", "traffic_source": "API"}
        self.assertEqual(route(data, CONFIG), ["ip:10.0.0.1:request"])

    def test_config_comes_from_the_environment(self):
        parsed = config_from_env(
            {
                "VELOCITY_COUNTERS": "user.id:user:message_sent:1,ip:ip:request:0",
                "VELOCITY_TRAFFIC_SOURCE": "traffic_source",
                "VELOCITY_API_KEY": "api_key",
                "VELOCITY_RETENTION": "2",
            }
        )
        self.assertEqual(parsed, CONFIG)


class _Counter:
    def __init__(self, fail_on=None):
        self.calls = []
        self.fail_on = fail_on

    def call(self, key, op, *args):
        if self.fail_on == (key, op):
            raise RuntimeError("redis down")
        self.calls.append((key, op, args))
        return 1


class RecordTest(unittest.TestCase):
    def test_count_returns_zero_and_alerts_when_redis_is_down(self):
        counter = _Counter(fail_on=("user:42:message_sent", "count"))
        with self.assertLogs("osprey_velocity", level="ERROR") as logs:
            self.assertEqual(count("user", "42", "message_sent", 10, 12, counter), 0)
        self.assertIn("user:42:message_sent", logs.output[0])

    def test_a_failed_record_alerts_and_still_raises(self):
        counter = _Counter(fail_on=("user:42:message_sent", "apply"))
        with self.assertLogs("osprey_velocity", level="ERROR") as logs:
            with self.assertRaises(RuntimeError):
                record("1", 10, HUMAN, CONFIG, counter)
        self.assertIn("user:42:message_sent", logs.output[0])
        self.assertEqual(counter.calls, [])

    def test_a_failed_key_is_finished_by_the_retry(self):
        counter = _Counter(fail_on=("ip:10.0.0.1:request", "apply"))
        with self.assertLogs("osprey_velocity", level="ERROR"):
            with self.assertRaises(RuntimeError):
                record("1", 10, HUMAN, CONFIG, counter)
        self.assertEqual(
            counter.calls,
            [
                ("user:42:message_sent", "apply", ("1", 10)),
                ("user:42:message_sent", "cleanup", (10, 2)),
            ],
        )
        counter.fail_on = None
        record("1", 10, HUMAN, CONFIG, counter)
        self.assertEqual(
            [call[0] for call in counter.calls if call[1] == "apply"],
            ["user:42:message_sent", "user:42:message_sent", "ip:10.0.0.1:request"],
        )

    def test_record_stores_the_event_time_and_count_ignores_an_expired_tick(self):
        server = Redis()
        self.addCleanup(server.close)
        import redis

        counter = RedisCounter(redis.Redis(host="127.0.0.1", port=server.port))
        record("1", 1, HUMAN, CONFIG, counter)
        record("2", 3, HUMAN, CONFIG, counter)
        self.assertEqual(count("user", "42", "message_sent", 2, 3, counter), 1)
        redis = server
        self.assertIsNone(redis.score("user:42:message_sent", "1"))
        self.assertEqual(redis.score("user:42:message_sent", "2"), 3)

    def test_the_stream_records_before_the_caller_sees_the_action(self):
        seen = []

        class Action:
            action_id = "7"
            timestamp = 10
            data = HUMAN

        class Context:
            def __enter__(self):
                return Action()

            def __exit__(self, *args):
                return False

        class Inner:
            def __iter__(self):
                return self

            def __next__(self):
                if seen:
                    raise StopIteration
                return Context()

        counter = _Counter()
        stream = RecordingStream(Inner(), CONFIG, counter)
        with next(stream) as action:
            seen.append(action.action_id)
            self.assertEqual(counter.calls[0][1], "apply")
        self.assertEqual(seen, ["7"])

    def test_a_jsonl_file_records_a_human_then_an_api_event(self):
        flat = Config(
            fields=(
                Field("user_id", "user", "message_sent", True),
                Field("ip", "ip", "request", False),
            ),
            traffic_source_path="traffic_source",
            api_key_path="api_key",
            retention=3600,
        )
        lines = [
            {
                "action_id": 1,
                "action_name": "message_sent",
                "timestamp": "2026-11-04T00:00:10+00:00",
                "data": {"user_id": "42", "ip": "10.0.0.1"},
            },
            {
                "action_id": 2,
                "action_name": "message_sent",
                "timestamp": "2026-11-04T00:00:12+00:00",
                "data": {
                    "user_id": "42",
                    "ip": "10.0.0.1",
                    "traffic_source": "api",
                    "api_key": "bot-9",
                },
            },
        ]
        handle = tempfile.NamedTemporaryFile("w", delete=False)
        handle.write("\n".join(json.dumps(line) for line in lines))
        handle.close()
        self.addCleanup(os.remove, handle.name)
        counter = _Counter()
        for context in stream_from_jsonl(handle.name, flat, counter):
            with context:
                pass
        applied = [call[0] for call in counter.calls if call[1] == "apply"]
        self.assertEqual(
            applied,
            [
                "user:42:message_sent",
                "ip:10.0.0.1:request",
                "api-client:bot-9:message_sent",
                "ip:10.0.0.1:request",
            ],
        )


if __name__ == "__main__":
    unittest.main()
