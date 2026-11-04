"""Freeze a counter when a rule fires, and hold the entity until the case is done.

Postgres is the record. Redis holds a flag so cleanup can skip the entity without
a database round trip. The flag is set after the Postgres commit.
shortcut: a crash between that commit and the Redis flag lets cleanup run until
the next capture or hold rewrites the flag.
"""

import os

STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS snapshot (
        id bigserial PRIMARY KEY,
        fired_at bigint NOT NULL,
        rule text NOT NULL,
        entity_type text NOT NULL,
        entity_id text NOT NULL,
        redis_key text NOT NULL,
        purged_at bigint
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tick (
        id bigserial PRIMARY KEY,
        snapshot_id bigint NOT NULL REFERENCES snapshot (id),
        entity_type text NOT NULL,
        entity_id text NOT NULL,
        event_id text NOT NULL,
        ts bigint NOT NULL,
        purged_at bigint
    )
    """,
    "CREATE INDEX IF NOT EXISTS tick_entity_ts ON tick (entity_type, entity_id, ts)",
    """
    CREATE TABLE IF NOT EXISTS hold (
        id bigserial PRIMARY KEY,
        entity_type text NOT NULL,
        entity_id text NOT NULL,
        placed_by text NOT NULL,
        placed_at bigint NOT NULL,
        status text NOT NULL,
        decided_at bigint,
        snapshot_id bigint REFERENCES snapshot (id),
        purged_at bigint
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS deletion_log (
        id bigserial PRIMARY KEY,
        at bigint NOT NULL,
        who text NOT NULL,
        what text NOT NULL
    )
    """,
)


def policy_from_env(env=None):
    env = os.environ if env is None else env
    return int(env["VELOCITY_HOLD_GRACE"]), int(env["VELOCITY_SNAPSHOT_RETENTION"])


def hold_redis_key(entity_type, entity_id):
    return "hold:" + entity_type + ":" + entity_id


def _entity(key):
    # shortcut: an entity id that contains ":" collides with the key format, change the separator if that shows up
    entity_type, entity_id, _behavior = key.split(":")
    return entity_type, entity_id


def _text(value):
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def ensure(conn):
    for statement in STATEMENTS:
        conn.execute(statement)
    conn.commit()


def capture(conn, redis, rule, key, who, now):
    """Read the sorted set, then write the header, the ticks, and an open hold."""
    entity_type, entity_id = _entity(key)
    members = redis.zrange(key, 0, -1, withscores=True)
    with conn.transaction():
        snap = conn.execute(
            """
            INSERT INTO snapshot (fired_at, rule, entity_type, entity_id, redis_key)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (int(now), rule, entity_type, entity_id, key),
        ).fetchone()[0]
        for member, score in members:
            conn.execute(
                """
                INSERT INTO tick (snapshot_id, entity_type, entity_id, event_id, ts)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (snap, entity_type, entity_id, _text(member), int(score)),
            )
        conn.execute(
            """
            INSERT INTO hold (entity_type, entity_id, placed_by, placed_at, status, snapshot_id)
            VALUES (%s, %s, %s, %s, 'open', %s)
            """,
            (entity_type, entity_id, who, int(now), snap),
        )
    redis.set(hold_redis_key(entity_type, entity_id), "1")
    return snap


def ticks(conn, snapshot_id):
    rows = conn.execute(
        """
        SELECT event_id, ts FROM tick
        WHERE snapshot_id = %s AND purged_at IS NULL
        ORDER BY ts, event_id
        """,
        (snapshot_id,),
    ).fetchall()
    return [(event_id, int(ts)) for event_id, ts in rows]


def place_hold(conn, redis, entity_type, entity_id, who, now):
    """Hold an entity before any rule has fired. No snapshot is copied."""
    with conn.transaction():
        hold_id = conn.execute(
            """
            INSERT INTO hold (entity_type, entity_id, placed_by, placed_at, status)
            VALUES (%s, %s, %s, %s, 'open')
            RETURNING id
            """,
            (entity_type, entity_id, who, int(now)),
        ).fetchone()[0]
    redis.set(hold_redis_key(entity_type, entity_id), "1")
    return hold_id


def confirm(conn, hold_id, now):
    conn.execute(
        """
        UPDATE hold SET status = 'confirmed', decided_at = %s
        WHERE id = %s AND purged_at IS NULL
        """,
        (int(now), hold_id),
    )
    conn.commit()


def clear(conn, hold_id, now):
    conn.execute(
        """
        UPDATE hold SET status = 'cleared', decided_at = %s
        WHERE id = %s AND purged_at IS NULL
        """,
        (int(now), hold_id),
    )
    conn.commit()


def purge_due(conn, redis, now, grace, snapshot_retention, who):
    """Soft-delete holds whose grace or retention has elapsed, then lift a lone Redis flag."""
    due = conn.execute(
        """
        SELECT id, entity_type, entity_id, snapshot_id
        FROM hold
        WHERE purged_at IS NULL
          AND decided_at IS NOT NULL
          AND (
            (status = 'cleared' AND decided_at <= %s - %s)
            OR (status = 'confirmed' AND decided_at <= %s - %s)
          )
        ORDER BY id
        """,
        (int(now), int(grace), int(now), int(snapshot_retention)),
    ).fetchall()
    for hold_id, entity_type, entity_id, snapshot_id in due:
        _purge(conn, redis, hold_id, entity_type, entity_id, snapshot_id, int(now), who)


def _purge(conn, redis, hold_id, entity_type, entity_id, snapshot_id, now, who):
    with conn.transaction():
        conn.execute(
            "UPDATE hold SET purged_at = %s WHERE id = %s AND purged_at IS NULL",
            (now, hold_id),
        )
        if snapshot_id is not None:
            conn.execute(
                "UPDATE snapshot SET purged_at = %s WHERE id = %s AND purged_at IS NULL",
                (now, snapshot_id),
            )
            conn.execute(
                "UPDATE tick SET purged_at = %s WHERE snapshot_id = %s AND purged_at IS NULL",
                (now, snapshot_id),
            )
        conn.execute(
            "INSERT INTO deletion_log (at, who, what) VALUES (%s, %s, %s)",
            (now, who, "hold %s snapshot %s %s:%s" % (hold_id, snapshot_id, entity_type, entity_id)),
        )
        remaining = conn.execute(
            """
            SELECT 1 FROM hold
            WHERE entity_type = %s AND entity_id = %s AND purged_at IS NULL
            """,
            (entity_type, entity_id),
        ).fetchone()
    if remaining is None:
        redis.delete(hold_redis_key(entity_type, entity_id))


def hold_status(conn, entity_type, entity_id):
    row = conn.execute(
        """
        SELECT status FROM hold
        WHERE entity_type = %s AND entity_id = %s AND purged_at IS NULL
        ORDER BY id
        """,
        (entity_type, entity_id),
    ).fetchone()
    return None if row is None else row[0]
