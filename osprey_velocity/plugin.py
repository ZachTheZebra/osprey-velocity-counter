"""Choose counter keys from config, then call the one-key Lua script."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# shortcut: loaded from the repo, not the installed wheel, bundle it when this is pip-installed elsewhere
_SCRIPT = (Path(__file__).resolve().parents[1] / "lua" / "counter.lua").read_text()


@dataclass(frozen=True)
class Field:
    path: str
    entity_type: str
    behavior: str
    population_sensitive: bool


@dataclass(frozen=True)
class Config:
    fields: tuple
    traffic_source_path: str
    api_key_path: str
    retention: int


def config_from_env(env=None):
    env = os.environ if env is None else env
    fields = []
    for piece in env.get("VELOCITY_COUNTERS", "").split(","):
        if not piece:
            continue
        path, entity_type, behavior, flag = piece.split(":")
        fields.append(Field(path, entity_type, behavior, flag == "1"))
    return Config(
        fields=tuple(fields),
        traffic_source_path=env.get("VELOCITY_TRAFFIC_SOURCE", "traffic_source"),
        api_key_path=env.get("VELOCITY_API_KEY", "api_key"),
        retention=int(env.get("VELOCITY_RETENTION", "0")),
    )


def _lookup(data, path):
    current = data
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    if current is None or current == "" or isinstance(current, bool):
        return None
    if isinstance(current, float):
        if not current.is_integer():
            return None
        return str(int(current))
    if not isinstance(current, (str, int)):
        return None
    return str(current)


def route(data, config):
    source = _lookup(data, config.traffic_source_path)
    api = source is not None and source.lower() == "api"
    keys = []
    for field in config.fields:
        if api and field.population_sensitive:
            entity_id = _lookup(data, config.api_key_path)
            entity_type = "api-client"
        else:
            entity_id = _lookup(data, field.path)
            entity_type = field.entity_type
        if entity_id is None:
            continue
        # shortcut: an id that contains ":" collides with the key format, change the separator if that shows up
        keys.append(entity_type + ":" + entity_id + ":" + field.behavior)
    return keys


def _time(value):
    if hasattr(value, "timestamp"):
        return int(value.timestamp())
    return int(value)


def record(event_id, timestamp, data, config, counter):
    """Apply each routed key. A raised call leaves the later keys for the retry."""
    tick = _time(timestamp)
    for key in route(data, config):
        counter.call(key, "apply", str(event_id), tick)
        counter.call(key, "cleanup", tick, config.retention)


def count(entity_type, entity_id, behavior, window, now, counter):
    key = str(entity_type) + ":" + str(entity_id) + ":" + str(behavior)
    return counter.call(key, "count", _time(now), int(window))


class RedisCounter:
    _shared = None

    def __init__(self, client):
        self._client = client

    def call(self, key, op, *args):
        return int(self._client.eval(_SCRIPT, 1, key, op, *[str(arg) for arg in args]))

    @classmethod
    def from_env(cls, env=None):
        import redis

        if cls._shared is None:
            env = os.environ if env is None else env
            cls._shared = redis.Redis.from_url(env.get("REDIS_URL", "redis://127.0.0.1:6379/0"))
        return cls(cls._shared)


class RecordingContext:
    def __init__(self, inner, config, counter):
        self._inner = inner
        self._config = config
        self._counter = counter

    def __enter__(self):
        action = self._inner.__enter__()
        record(action.action_id, action.timestamp, action.data, self._config, self._counter)
        return action

    def __exit__(self, exc_type, exc, tb):
        return self._inner.__exit__(exc_type, exc, tb)


class RecordingStream:
    """Record, then hand the action to the rules. Does not replace a Kafka source by itself."""

    def __init__(self, inner, config, counter):
        self._inner = inner
        self._config = config
        self._counter = counter

    def __iter__(self):
        return self

    def __next__(self):
        return RecordingContext(next(self._inner), self._config, self._counter)
