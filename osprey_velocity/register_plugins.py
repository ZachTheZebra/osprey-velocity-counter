"""Osprey entry point. Registers the read UDF and does not take the input stream."""

from osprey.worker.adaptor.plugin_manager import hookimpl_osprey

from osprey_velocity.udf import VelocityCount


@hookimpl_osprey
def register_udfs():
    return [VelocityCount]
