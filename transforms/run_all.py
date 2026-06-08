"""Run all Silver-layer Snowpark transforms in order.

Executes the AIS, schedules, and weather transforms sequentially, logging the
source row count before and the target row count after each step.
"""

from __future__ import annotations

from loguru import logger
from snowflake.snowpark import Session

from config.settings import get_session
from transforms.ais_flatten import (
    SOURCE_TABLE as AIS_SRC,
    TARGET_TABLE as AIS_TGT,
    flatten_ais,
)
from transforms.schedules import (
    SOURCE_TABLE as SCHED_SRC,
    TARGET_TABLE as SCHED_TGT,
    transform_schedules,
)
from transforms.weather import (
    SOURCE_TABLE as WX_SRC,
    TARGET_TABLE as WX_TGT,
    flatten_weather,
)

_STEPS = (
    ("AIS flatten", AIS_SRC, AIS_TGT, flatten_ais),
    ("Schedules", SCHED_SRC, SCHED_TGT, transform_schedules),
    ("Weather flatten", WX_SRC, WX_TGT, flatten_weather),
)


def run_all(session: Session) -> None:
    """Run every Silver transform in dependency order.

    Args:
        session: An active Snowpark session.
    """
    for name, src_table, tgt_table, fn in _STEPS:
        before = session.table(src_table).count()
        logger.info("[{}] source {} has {} rows", name, src_table, before)
        after = fn(session)
        logger.info("[{}] target {} now has {} rows", name, tgt_table, after)
    logger.success("All Silver transforms complete.")


def main() -> None:
    session = get_session()
    try:
        run_all(session)
    finally:
        session.close()
        logger.info("Snowpark session closed.")


if __name__ == "__main__":
    main()
