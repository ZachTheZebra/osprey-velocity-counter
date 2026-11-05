#!/usr/bin/env python3
"""Assert the two demo events landed in Redis and the rule read the counts."""

import sys
from datetime import datetime

import redis

LOG = sys.argv[1]
URL = sys.argv[2] if len(sys.argv) > 2 else "redis://redis:6379/0"

FIRST = int(datetime.fromisoformat("2026-11-04T00:00:10+00:00").timestamp())
SECOND = int(datetime.fromisoformat("2026-11-04T00:00:12+00:00").timestamp())

client = redis.Redis.from_url(URL)
log = open(LOG).read()


def score(key, member):
    raw = client.zscore(key, member)
    return None if raw is None else int(raw)


failures = []


def expect(label, got, want):
    if got != want:
        failures.append(f"{label}: got {got}, want {want}")


expect("human user tick", score("user:42:message_sent", "1"), FIRST)
expect("api event stays off the user key", score("user:42:message_sent", "2"), None)
expect("api client tick", score("api-client:bot-9:message_sent", "2"), SECOND)
expect("human ip tick", score("ip:10.0.0.1:request", "1"), FIRST)
expect("api ip tick", score("ip:10.0.0.1:request", "2"), SECOND)

if "UserMessages" not in log or "IpRequests" not in log:
    failures.append("worker log never printed UserMessages and IpRequests")

if failures:
    print("\n".join(failures))
    print("--- worker log ---")
    print(log[-4000:])
    sys.exit(1)

print("demo ok")
