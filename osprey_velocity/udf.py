"""A rule reads a count. It does not write one."""

import os
from typing import Any

from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.entities import EntityT
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase

from osprey_velocity.plugin import RedisCounter, count


class VelocityCountArguments(ArgumentsBase):
    entity: EntityT[Any]
    behavior: str
    window: int


class VelocityCount(UDFBase[VelocityCountArguments, int]):
    category = UdfCategories.ENTITY

    def execute(self, execution_context: ExecutionContext, arguments: VelocityCountArguments) -> int:
        return count(
            arguments.entity.type,
            arguments.entity.id,
            arguments.behavior,
            arguments.window,
            execution_context.get_action_time(),
            RedisCounter.from_env(os.environ),
        )
