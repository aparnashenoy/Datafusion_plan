"""Silver transform: flatten raw weather payloads.

Reads BRONZE.WEATHER (raw VARIANT), extracts and types the observation fields,
and writes the result to SILVER.WEATHER.
"""

from __future__ import annotations

from loguru import logger
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import FloatType, StringType, TimestampType

SOURCE_TABLE = "BRONZE.WEATHER"
TARGET_TABLE = "SILVER.WEATHER"


def flatten_weather(session: Session) -> int:
    """Flatten and type the raw weather VARIANT payloads.

    Args:
        session: An active Snowpark session.

    Returns:
        The number of rows written to the Silver table.
    """
    logger.info("Reading {}", SOURCE_TABLE)
    src = session.table(SOURCE_TABLE)

    flattened = src.select(
        col("raw_payload")["voyage_id"].cast(StringType()).alias("VOYAGE_ID"),
        col("raw_payload")["observed_at"].cast(TimestampType()).alias("OBSERVED_AT"),
        col("raw_payload")["wind_speed_knots"].cast(FloatType()).alias("WIND_SPEED_KNOTS"),
        col("raw_payload")["wave_height_m"].cast(FloatType()).alias("WAVE_HEIGHT_M"),
        col("raw_payload")["condition"].cast(StringType()).alias("CONDITION"),
    )

    flattened.write.mode("overwrite").save_as_table(TARGET_TABLE)
    count = session.table(TARGET_TABLE).count()
    logger.success("Wrote {} rows to {}", count, TARGET_TABLE)
    return count
