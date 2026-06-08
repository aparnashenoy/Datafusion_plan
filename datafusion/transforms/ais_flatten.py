"""Silver transform: flatten raw AIS position payloads.

Reads BRONZE.AIS_POSITIONS (raw VARIANT), extracts and types the fields of
interest, and writes the result to SILVER.AIS_POSITIONS.
"""

from __future__ import annotations

from loguru import logger
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import FloatType, StringType, TimestampType

SOURCE_TABLE = "BRONZE.AIS_POSITIONS"
TARGET_TABLE = "SILVER.AIS_POSITIONS"


def flatten_ais(session: Session) -> int:
    """Flatten and type the raw AIS VARIANT payloads.

    Args:
        session: An active Snowpark session.

    Returns:
        The number of rows written to the Silver table.
    """
    logger.info("Reading {}", SOURCE_TABLE)
    src = session.table(SOURCE_TABLE)

    flattened = src.select(
        col("raw_payload")["vessel_mmsi"].cast(StringType()).alias("VESSEL_MMSI"),
        col("raw_payload")["vessel_name"].cast(StringType()).alias("VESSEL_NAME"),
        col("raw_payload")["latitude"].cast(FloatType()).alias("LATITUDE"),
        col("raw_payload")["longitude"].cast(FloatType()).alias("LONGITUDE"),
        col("raw_payload")["position_ts"].cast(TimestampType()).alias("POSITION_TS"),
        col("raw_payload")["speed_knots"].cast(FloatType()).alias("SPEED_KNOTS"),
        col("raw_payload")["nav_status"].cast(StringType()).alias("NAV_STATUS"),
    )

    flattened.write.mode("overwrite").save_as_table(TARGET_TABLE)
    count = session.table(TARGET_TABLE).count()
    logger.success("Wrote {} rows to {}", count, TARGET_TABLE)
    return count
