"""Silver transform: vessel schedules with delay computation.

Reads BRONZE.VESSEL_SCHEDULES, derives a delay_hours column as the difference
(in hours) between estimated and actual arrival, and writes the result to
SILVER.VESSEL_SCHEDULES.
"""

from __future__ import annotations

from loguru import logger
from snowflake.snowpark import Session
from snowflake.snowpark.functions import call_builtin, col, lit

SOURCE_TABLE = "BRONZE.VESSEL_SCHEDULES"
TARGET_TABLE = "SILVER.VESSEL_SCHEDULES"


def transform_schedules(session: Session) -> int:
    """Add a delay_hours column to the vessel schedules.

    Args:
        session: An active Snowpark session.

    Returns:
        The number of rows written to the Silver table.
    """
    logger.info("Reading {}", SOURCE_TABLE)
    src = session.table(SOURCE_TABLE)

    enriched = src.with_column(
        "DELAY_HOURS",
        call_builtin(
            "DATEDIFF",
            lit("hour"),
            col("estimated_arrival"),
            col("actual_arrival"),
        ),
    )

    enriched.write.mode("overwrite").save_as_table(TARGET_TABLE)
    count = session.table(TARGET_TABLE).count()
    logger.success("Wrote {} rows to {}", count, TARGET_TABLE)
    return count
