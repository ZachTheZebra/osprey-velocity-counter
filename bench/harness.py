#!/usr/bin/env python3
"""Compare one dedup-and-tick at 1k, 2k, 5k, and 10k offered events per second.

Redis runs counter.lua apply (ZADD NX). Postgres locks the key with
pg_advisory_xact_lock, then INSERT ... ON CONFLICT DO NOTHING. One hundred
keys. Each level starts empty and is offered for a few seconds. A level above
one second at p99 is not followed by a harder level.

redis-benchmark and pgbench do not run this operation, so both stores use this
client. Both servers are Docker containers on localhost and pay the same port hop.
"""

import asyncio
import math
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import psycopg
import redis.asyncio as aioredis

from bench.stats import percentile
HERE = Path(__file__).resolve().parent
SCRIPT = (ROOT / "osprey_velocity" / "counter.lua").read_text()
RATES = (1000, 2000, 5000, 10000)
SECONDS = 3
KEYS = 100
POOL = 32
SLOT = 0.05
P99_STOP = 1.0


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Container:
    def __init__(self, image, prefix, args):
        self.port = free_port()
        self.name = "%s-%s" % (prefix, self.port)
        self.proc = subprocess.Popen(
            [
                "docker",
                "run",
                "--rm",
                "--name",
                self.name,
                "-p",
                "127.0.0.1:%s:%s" % (self.port, args["container_port"]),
                *args["env"],
                image,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def stop(self):
        subprocess.run(
            ["docker", "rm", "-f", self.name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if self.proc.poll() is None:
            self.proc.wait(timeout=10)


def wait_redis(port):
    async def ping():
        client = aioredis.Redis(host="127.0.0.1", port=port)
        try:
            for _ in range(60):
                try:
                    await client.ping()
                    return
                except Exception:
                    await asyncio.sleep(0.25)
            raise RuntimeError("redis did not start")
        finally:
            await client.aclose()

    asyncio.run(ping())


def wait_postgres(port):
    dsn = "postgresql://postgres@127.0.0.1:%s/postgres" % port
    for _ in range(60):
        try:
            with psycopg.connect(dsn, connect_timeout=1) as conn:
                conn.execute("SELECT 1")
            return dsn
        except Exception:
            time.sleep(0.25)
    raise RuntimeError("postgres did not start")


async def sweep(worker, rate):
    latencies = []
    errors = 0
    first_error = None
    in_flight = set()
    started = time.perf_counter()
    deadline = started + SECONDS
    n = 0

    async def run_one(i):
        nonlocal errors, first_error
        key = "user:%s:message_sent" % (i % KEYS)
        t0 = time.perf_counter()
        try:
            await worker(key, "%s-%s" % (rate, i), i)
        except Exception as exc:
            errors += 1
            if first_error is None:
                first_error = "%s: %s" % (type(exc).__name__, exc)
        else:
            latencies.append(time.perf_counter() - t0)

    while time.perf_counter() < deadline:
        slot_end = time.perf_counter() + SLOT
        batch = max(1, int(round(rate * SLOT)))
        for _ in range(batch):
            n += 1
            task = asyncio.create_task(run_one(n))
            in_flight.add(task)
            task.add_done_callback(in_flight.discard)
            if n % 25 == 0:
                await asyncio.sleep(0)
        delay = slot_end - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
    if in_flight:
        await asyncio.gather(*in_flight)
    elapsed = time.perf_counter() - started
    return {
        "target": rate,
        "achieved": (len(latencies) / elapsed) if elapsed else 0,
        "p50": percentile(latencies, 50) if latencies else None,
        "p95": percentile(latencies, 95) if latencies else None,
        "p99": percentile(latencies, 99) if latencies else None,
        "errors": errors,
        "error": first_error,
        "samples": len(latencies),
    }


def too_slow(row):
    return row["samples"] == 0 or (row["p99"] is not None and row["p99"] > P99_STOP)


async def redis_levels(port):
    # The default pool raises when all 32 are busy. Wait, as the Postgres queue does.
    client = aioredis.Redis(
        connection_pool=aioredis.BlockingConnectionPool(
            host="127.0.0.1",
            port=port,
            decode_responses=True,
            max_connections=POOL,
            timeout=None,
        )
    )
    script = client.register_script(SCRIPT)

    async def worker(key, event_id, ts):
        await script(keys=[key], args=["apply", event_id, str(ts)])

    first = await script(keys=["user:bench:message_sent"], args=["apply", "same", "1"])
    second = await script(keys=["user:bench:message_sent"], args=["apply", "same", "9"])
    if int(first) != 1 or int(second) != 0:
        raise RuntimeError("redis dedup self-check failed: %s then %s" % (first, second))
    rows = []
    try:
        for rate in RATES:
            await client.flushdb()
            row = await sweep(worker, rate)
            row["store"] = "redis"
            rows.append(row)
            _print(row)
            if too_slow(row):
                break
    finally:
        await client.aclose()
    return rows


async def postgres_levels(dsn):
    setup = await psycopg.AsyncConnection.connect(dsn)
    await setup.execute(
        """
        CREATE TABLE IF NOT EXISTS tick (
            key text NOT NULL,
            event_id text NOT NULL,
            ts bigint NOT NULL,
            PRIMARY KEY (key, event_id)
        )
        """
    )
    await setup.commit()
    await setup.close()
    holders = []
    for _ in range(POOL):
        conn = await psycopg.AsyncConnection.connect(dsn)
        await conn.execute("SET statement_timeout = '2s'")
        await conn.commit()
        holders.append(conn)
    queue = asyncio.Queue()
    for conn in holders:
        queue.put_nowait(conn)

    async def worker(key, event_id, ts):
        conn = await queue.get()
        try:
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s)::bigint)", (key,))
                await conn.execute(
                    """
                    INSERT INTO tick (key, event_id, ts)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (key, event_id, int(ts)),
                )
        finally:
            queue.put_nowait(conn)

    probe = holders[0]
    async with probe.transaction():
        await probe.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s)::bigint)",
            ("user:bench:message_sent",),
        )
        added = await probe.execute(
            "INSERT INTO tick (key, event_id, ts) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            ("user:bench:message_sent", "same", 1),
        )
        again = await probe.execute(
            "INSERT INTO tick (key, event_id, ts) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            ("user:bench:message_sent", "same", 9),
        )
    if added.rowcount != 1 or again.rowcount != 0:
        raise RuntimeError(
            "postgres dedup self-check failed: %s then %s" % (added.rowcount, again.rowcount)
        )
    rows = []
    try:
        for rate in RATES:
            await probe.execute("TRUNCATE tick")
            await probe.commit()
            row = await sweep(worker, rate)
            row["store"] = "postgres"
            rows.append(row)
            _print(row)
            if too_slow(row):
                break
    finally:
        for conn in holders:
            await conn.close()
    return rows


def _print(row):
    print(
        "%s target %s achieved %.0f p50 %s p99 %s errors %s"
        % (
            row["store"],
            row["target"],
            row["achieved"],
            _ms(row["p50"]),
            _ms(row["p99"]),
            row["errors"],
        ),
        flush=True,
    )
    if row["error"]:
        print("  first error: %s" % row["error"], flush=True)


def _ms(seconds):
    return "n/a" if seconds is None else "%.2f ms" % (seconds * 1000)


def write_report(rows):
    lines = [
        "# Redis versus Postgres",
        "",
        "Each event is one dedup-and-tick for one of 100 keys.",
        "Redis runs `counter.lua` `apply` (`ZADD NX`).",
        "Postgres takes `pg_advisory_xact_lock` on the key, then `INSERT ... ON CONFLICT DO NOTHING`.",
        "Each level starts from an empty store and is offered for %s seconds in 50 ms bursts."
        % SECONDS,
        "Latency is the time from when the client starts the operation until it finishes,",
        "including waiting for a free connection from a pool of %s." % POOL,
        "A store stops at the first level whose p99 is over one second.",
        "Achieved rate is completed calls divided by wall time, including calls still in flight at the end of the offer.",
        "A full sample count and a low achieved rate means those calls finished late. It does not mean they were dropped.",
        "The 100 keys share the traffic. This is not one hot key.",
        "Postgres does not use SELECT FOR UPDATE. A missing row does not lock the gap, so the lock is pg_advisory_xact_lock and the primary key rejects the duplicate.",
        "Both servers are Docker containers published on localhost, so both pay that port hop.",
        "`redis-benchmark` and `pgbench` are not used. They do not run this operation.",
        "",
        "| store | target /s | achieved /s | p50 | p95 | p99 | errors | samples |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| %s | %s | %.0f | %s | %s | %s | %s | %s |"
            % (
                row["store"],
                row["target"],
                row["achieved"],
                _ms(row["p50"]),
                _ms(row["p95"]),
                _ms(row["p99"]),
                row["errors"],
                row["samples"],
            )
        )
    notes = [row["error"] for row in rows if row["error"]]
    if notes:
        lines.extend(["", "First error: %s" % notes[0]])
    lines.extend(["", "![Latency versus offered load](latency.svg)", ""])
    (HERE / "results.md").write_text("\n".join(lines))
    (HERE / "latency.svg").write_text(_svg(rows))


def _svg(rows):
    width, height = 720, 400
    left, right, top, bottom = 64, 160, 28, 48
    rates = list(RATES)
    ys = [row[field] * 1000 for row in rows for field in ("p50", "p99") if row[field] is not None]
    ymax = max(max(ys) * 1.15, 10) if ys else 10
    ymin = 1.0
    plot_w = width - left - right
    plot_h = height - top - bottom
    log_min = math.log10(ymin)
    log_span = math.log10(ymax) - log_min

    def x_of(rate):
        return left + plot_w * rates.index(rate) / (len(rates) - 1)

    def y_of(ms):
        frac = (math.log10(min(max(ms, ymin), ymax)) - log_min) / log_span
        return top + plot_h * (1 - frac)

    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" width="%s" height="%s">' % (width, height),
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="%s" y="18" font-family="sans-serif" font-size="14">Dedup-and-tick latency, log scale</text>'
        % left,
    ]
    decade = 1.0
    while decade <= ymax:
        y = y_of(decade)
        parts.append(
            '<line x1="%s" y1="%.1f" x2="%s" y2="%.1f" stroke="#ddd"/>' % (left, y, left + plot_w, y)
        )
        parts.append(
            '<text x="8" y="%.1f" font-family="sans-serif" font-size="11">%.0f ms</text>'
            % (y + 4, decade)
        )
        decade *= 10
    for rate in rates:
        x = x_of(rate)
        parts.append(
            '<text x="%.1f" y="%s" text-anchor="middle" font-family="sans-serif" font-size="11">%s</text>'
            % (x, height - 16, rate)
        )
    series = (
        ("redis", "p99", "#1d4e89", ""),
        ("postgres", "p99", "#b85c38", ""),
        ("redis", "p50", "#1d4e89", "5 4"),
        ("postgres", "p50", "#b85c38", "5 4"),
    )
    labels = {"p99": "p99", "p50": "p50"}
    for index, (store, field, color, dash) in enumerate(series):
        points = [
            (x_of(row["target"]), y_of(row[field] * 1000))
            for row in rows
            if row["store"] == store and row[field] is not None
        ]
        if len(points) >= 2:
            drawn = " ".join("%.1f,%.1f" % point for point in points)
            parts.append(
                '<polyline fill="none" stroke="%s" stroke-width="2" stroke-dasharray="%s" points="%s"/>'
                % (color, dash, drawn)
            )
        for point in points:
            parts.append('<circle cx="%.1f" cy="%.1f" r="3" fill="%s"/>' % (point[0], point[1], color))
        parts.append(
            '<line x1="%s" y1="%s" x2="%s" y2="%s" stroke="%s" stroke-width="2" stroke-dasharray="%s"/>'
            % (width - 148, 48 + index * 18, width - 116, 48 + index * 18, color, dash)
        )
        parts.append(
            '<text x="%s" y="%s" font-family="sans-serif" font-size="12">%s %s</text>'
            % (width - 110, 52 + index * 18, store, labels[field])
        )
    parts.append("</svg>")
    return "\n".join(parts)


def main():
    redis_box = Container(
        "redis:7",
        "bench-redis",
        {"container_port": 6379, "env": []},
    )
    postgres_box = Container(
        "postgres:16-alpine",
        "bench-pg",
        {
            "container_port": 5432,
            "env": ["-e", "POSTGRES_HOST_AUTH_METHOD=trust", "-e", "POSTGRES_USER=postgres"],
        },
    )
    try:
        wait_redis(redis_box.port)
        dsn = wait_postgres(postgres_box.port)
        rows = asyncio.run(redis_levels(redis_box.port))
        rows.extend(asyncio.run(postgres_levels(dsn)))
        write_report(rows)
        print("wrote %s" % (HERE / "results.md"), flush=True)
    finally:
        redis_box.stop()
        postgres_box.stop()


if __name__ == "__main__":
    main()
