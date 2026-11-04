"""Osprey entry point. The count UDF always registers.

A file of events is the input stream only when VELOCITY_EVENTS_FILE is set.
Otherwise this hook returns None and leaves the stream to another plugin.
"""

import os
from contextlib import contextmanager

from osprey.engine.executor.execution_context import Action
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.osprey_shared.labels import EntityLabels
from osprey.worker.lib.storage.labels import LabelsServiceBase
from osprey.worker.sinks.utils.acking_contexts import NoopAckingContext

from osprey_velocity.plugin import RecordingStream, RedisCounter, config_from_env, read_events
from osprey_velocity.udf import VelocityCount


class _EmptyLabels(LabelsServiceBase):
    """This Osprey build asserts a labels provider exists. The demo has no Postgres."""

    def read_labels(self, entity):
        return EntityLabels()

    @contextmanager
    def read_modify_write_labels_atomically(self, entity):
        yield EntityLabels()


@hookimpl_osprey
def register_udfs():
    return [VelocityCount]


@hookimpl_osprey
def register_labels_service_or_provider(config):
    if os.environ.get("VELOCITY_EMPTY_LABELS") != "1":
        return None
    return _EmptyLabels()


@hookimpl_osprey
def register_input_stream(config):
    path = os.environ.get("VELOCITY_EVENTS_FILE")
    if not path:
        return None
    contexts = [
        NoopAckingContext(
            Action(
                action_id=int(event["action_id"]),
                action_name=event["action_name"],
                data=event["data"],
                timestamp=event["timestamp"],
            )
        )
        for event in read_events(path)
    ]
    return RecordingStream(iter(contexts), config_from_env(), RedisCounter.from_env())
