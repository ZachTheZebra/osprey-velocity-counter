#!/usr/bin/env python3
"""One Redis key, one Lua script. Matches the velocity-counter model.

A tick is inside the window when now - window < timestamp <= now.
A tick is expired when timestamp <= now - retention.
"""

import os
import socket
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "lua" / "counter.lua"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Redis:
    def __init__(self):
        self.port = free_port()
        self.dir = tempfile.TemporaryDirectory()
        self.proc = subprocess.Popen(
            [
                os.environ.get("REDIS_SERVER", "redis-server"),
                "--port",
                str(self.port),
                "--bind",
                "127.0.0.1",
                "--save",
                "",
                "--appendonly",
                "no",
                "--dir",
                self.dir.name,
                "--daemonize",
                "no",
                "--loglevel",
                "warning",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self._wait()

    def _wait(self):
        for _ in range(50):
            try:
                self.cli("PING")
                return
            except subprocess.CalledProcessError:
                self.proc.poll()
        raise RuntimeError("redis-server did not start")

    def cli(self, *args):
        result = subprocess.run(
            [os.environ.get("REDIS_CLI", "redis-cli"), "-p", str(self.port), *args],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()

    def call(self, key, *args):
        return int(self.cli("EVAL", SCRIPT.read_text(), "1", key, *map(str, args)))

    def score(self, key, member):
        raw = self.cli("ZSCORE", key, member)
        return None if raw == "" else float(raw)

    def close(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=5)
        self.dir.cleanup()


class CounterTest(unittest.TestCase):
    def setUp(self):
        self.redis = Redis()

    def tearDown(self):
        self.redis.close()

    def test_second_apply_keeps_the_first_timestamp(self):
        added = self.redis.call("user:user-1:message_sent", "apply", "1", 10)
        again = self.redis.call("user:user-1:message_sent", "apply", "1", 99)
        self.assertEqual(added, 1)
        self.assertEqual(again, 0)
        self.assertEqual(self.redis.score("user:user-1:message_sent", "1"), 10)

    def test_count_ignores_an_expired_tick_until_cleanup_removes_it(self):
        key = "ip:ip-1:request"
        self.redis.call(key, "apply", "1", 1)
        self.redis.call(key, "apply", "2", 2)
        # now=3, window=2, retention=2: ts 1 is expired, ts 2 is inside.
        self.assertEqual(self.redis.call(key, "count", 3, 2), 1)
        self.assertEqual(self.redis.score(key, "1"), 1)
        removed = self.redis.call(key, "cleanup", 3, 2)
        self.assertEqual(removed, 1)
        self.assertIsNone(self.redis.score(key, "1"))
        self.assertEqual(self.redis.score(key, "2"), 2)

    def test_redelivery_after_cleanup_restores_the_original_timestamp(self):
        key = "user:user-1:message_sent"
        self.redis.call(key, "apply", "1", 1)
        self.redis.call(key, "cleanup", 3, 2)
        restored = self.redis.call(key, "apply", "1", 1)
        self.assertEqual(restored, 1)
        self.assertEqual(self.redis.score(key, "1"), 1)

    def test_a_crash_between_keys_leaves_the_finished_key(self):
        self.redis.call("user:user-1:message_sent", "apply", "1", 1)
        self.assertIsNone(self.redis.score("ip:ip-1:request", "1"))
        self.redis.call("ip:ip-1:request", "apply", "1", 1)
        self.assertEqual(self.redis.score("user:user-1:message_sent", "1"), 1)
        self.assertEqual(self.redis.score("ip:ip-1:request", "1"), 1)


if __name__ == "__main__":
    unittest.main()
