#!/usr/bin/env python3
"""Postgres keeps the evidence. Redis keeps the live counter."""

import subprocess
import sys
import time
import unittest
from pathlib import Path

import psycopg
import redis

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from osprey_velocity.audit import (  # noqa: E402
    capture,
    clear,
    confirm,
    ensure,
    hold_status,
    place_hold,
    purge_due,
    ticks,
)
from test_counter import Redis, free_port  # noqa: E402


class Postgres:
    def __init__(self):
        self.port = free_port()
        self.name = "velocity-audit-%s" % self.port
        self.proc = subprocess.Popen(
            [
                "docker",
                "run",
                "--rm",
                "--name",
                self.name,
                "-e",
                "POSTGRES_HOST_AUTH_METHOD=trust",
                "-e",
                "POSTGRES_USER=postgres",
                "-p",
                "127.0.0.1:%s:5432" % self.port,
                "postgres:16-alpine",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.dsn = "postgresql://postgres@127.0.0.1:%s/postgres" % self.port
        self._wait()

    def _wait(self):
        for _ in range(60):
            if self.proc.poll() is not None:
                raise RuntimeError("postgres exited")
            try:
                with psycopg.connect(self.dsn, connect_timeout=1) as conn:
                    conn.execute("SELECT 1")
                return
            except Exception:
                time.sleep(0.5)
        raise RuntimeError("postgres did not start")

    def close(self):
        subprocess.run(
            ["docker", "rm", "-f", self.name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if self.proc.poll() is None:
            self.proc.wait(timeout=10)


class PolicyTest(unittest.TestCase):
    def test_grace_and_retention_come_from_the_environment(self):
        from osprey_velocity.audit import policy_from_env

        self.assertEqual(
            policy_from_env(
                {"VELOCITY_HOLD_GRACE": "15", "VELOCITY_SNAPSHOT_RETENTION": "90"}
            ),
            (15, 90),
        )


class AuditTest(unittest.TestCase):
    def setUp(self):
        self.redis = Redis()
        self.pg = Postgres()

    def tearDown(self):
        self.pg.close()
        self.redis.close()

    def test_a_rule_fire_copies_the_sorted_set_and_holds_the_entity(self):
        key = "user:42:message_sent"
        self.redis.call(key, "apply", "1", 10)
        self.redis.call(key, "apply", "2", 12)
        client = redis.Redis(host="127.0.0.1", port=self.redis.port)
        with psycopg.connect(self.pg.dsn) as conn:
            ensure(conn)
            snap = capture(conn, client, "RecentMessage", key, "rule", 12)
            self.assertEqual(ticks(conn, snap), [("1", 10), ("2", 12)])
            self.redis.call(key, "apply", "3", 99)
            self.assertEqual(ticks(conn, snap), [("1", 10), ("2", 12)])
            self.assertEqual(hold_status(conn, "user", "42"), "open")
        self.assertEqual(client.get("hold:user:42"), b"1")

    def test_a_cleared_hold_stays_until_the_grace_window(self):
        key = "user:42:message_sent"
        self.redis.call(key, "apply", "1", 10)
        client = redis.Redis(host="127.0.0.1", port=self.redis.port)
        with psycopg.connect(self.pg.dsn) as conn:
            ensure(conn)
            snap = capture(conn, client, "RecentMessage", key, "rule", 12)
            hold_id = conn.execute(
                "SELECT id FROM hold WHERE snapshot_id = %s", (snap,)
            ).fetchone()[0]
            clear(conn, hold_id, 20)
            purge_due(conn, client, 29, grace=10, snapshot_retention=100, who="retention")
            self.assertEqual(ticks(conn, snap), [("1", 10)])
            self.assertEqual(hold_status(conn, "user", "42"), "cleared")
            self.assertEqual(client.get("hold:user:42"), b"1")
            purge_due(conn, client, 30, grace=10, snapshot_retention=100, who="retention")
            self.assertEqual(ticks(conn, snap), [])
            self.assertIsNone(hold_status(conn, "user", "42"))
            log = conn.execute("SELECT who, what FROM deletion_log").fetchall()
            self.assertEqual(log, [("retention", "hold %s snapshot %s user:42" % (hold_id, snap))])
        self.assertIsNone(client.get("hold:user:42"))

    def test_an_analyst_can_hold_an_entity_before_a_rule_fires(self):
        client = redis.Redis(host="127.0.0.1", port=self.redis.port)
        with psycopg.connect(self.pg.dsn) as conn:
            ensure(conn)
            place_hold(conn, client, "ip", "10.0.0.1", "analyst", 5)
            self.assertEqual(hold_status(conn, "ip", "10.0.0.1"), "open")
            self.assertEqual(
                conn.execute("SELECT count(*) FROM snapshot").fetchone()[0],
                0,
            )
        self.assertEqual(client.get("hold:ip:10.0.0.1"), b"1")

    def test_a_confirmed_hold_waits_for_the_retention_schedule(self):
        key = "user:42:message_sent"
        self.redis.call(key, "apply", "1", 10)
        client = redis.Redis(host="127.0.0.1", port=self.redis.port)
        with psycopg.connect(self.pg.dsn) as conn:
            ensure(conn)
            snap = capture(conn, client, "RecentMessage", key, "rule", 12)
            hold_id = conn.execute(
                "SELECT id FROM hold WHERE snapshot_id = %s", (snap,)
            ).fetchone()[0]
            confirm(conn, hold_id, 20)
            purge_due(conn, client, 30, grace=10, snapshot_retention=100, who="retention")
            self.assertEqual(ticks(conn, snap), [("1", 10)])
            self.assertEqual(hold_status(conn, "user", "42"), "confirmed")
            purge_due(conn, client, 120, grace=10, snapshot_retention=100, who="retention")
            self.assertEqual(ticks(conn, snap), [])
            self.assertIsNone(hold_status(conn, "user", "42"))
        self.assertIsNone(client.get("hold:user:42"))

    def test_purging_one_hold_keeps_the_flag_while_another_is_open(self):
        client = redis.Redis(host="127.0.0.1", port=self.redis.port)
        with psycopg.connect(self.pg.dsn) as conn:
            ensure(conn)
            first = place_hold(conn, client, "user", "42", "analyst", 5)
            place_hold(conn, client, "user", "42", "analyst", 6)
            clear(conn, first, 10)
            purge_due(conn, client, 20, grace=10, snapshot_retention=100, who="retention")
            self.assertEqual(hold_status(conn, "user", "42"), "open")
        self.assertEqual(client.get("hold:user:42"), b"1")


if __name__ == "__main__":
    unittest.main()
