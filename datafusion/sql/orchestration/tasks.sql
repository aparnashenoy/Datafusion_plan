-- DataFusion :: Orchestration Tasks DAG
-- 3-node DAG: flatten AIS -> enrich notices -> build Gold delay view.
-- Driven by the append-only streams in streams.sql.
--
-- Replace DATAFUSION_WH with your warehouse name before running.

USE DATABASE DATAFUSION;

-- ---------------------------------------------------------------------------
-- Stored procedure wrapping the Snowpark AIS flatten logic (transforms/ais_flatten.py).
-- Mirrors that transform so the root task can run it server-side.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SILVER.SP_FLATTEN_AIS()
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.10'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import FloatType, StringType, TimestampType


def run(session: Session) -> str:
    src = session.table("BRONZE.AIS_POSITIONS")
    flattened = src.select(
        col("raw_payload")["vessel_mmsi"].cast(StringType()).alias("VESSEL_MMSI"),
        col("raw_payload")["vessel_name"].cast(StringType()).alias("VESSEL_NAME"),
        col("raw_payload")["latitude"].cast(FloatType()).alias("LATITUDE"),
        col("raw_payload")["longitude"].cast(FloatType()).alias("LONGITUDE"),
        col("raw_payload")["position_ts"].cast(TimestampType()).alias("POSITION_TS"),
        col("raw_payload")["speed_knots"].cast(FloatType()).alias("SPEED_KNOTS"),
        col("raw_payload")["nav_status"].cast(StringType()).alias("NAV_STATUS"),
    )
    flattened.write.mode("overwrite").save_as_table("SILVER.AIS_POSITIONS")
    return f"SILVER.AIS_POSITIONS rows: {session.table('SILVER.AIS_POSITIONS').count()}"
$$;

-- ---------------------------------------------------------------------------
-- Node 1 (root): flatten AIS. Runs every 5 minutes when its stream has data.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TASK SILVER.FLATTEN_AIS_TASK
    WAREHOUSE = DATAFUSION_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.AIS_POSITIONS_STREAM')
AS
    CALL SILVER.SP_FLATTEN_AIS();

-- ---------------------------------------------------------------------------
-- Node 2: enrich port notices with Cortex (runs after node 1).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TASK SILVER.ENRICH_NOTICES_TASK
    AFTER SILVER.FLATTEN_AIS_TASK
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.PORT_NOTICES_STREAM')
AS
    CREATE OR REPLACE TABLE SILVER.PORT_NOTICES_ENRICHED AS
    SELECT
        doc_id,
        port_code,
        issued_at,
        doc_text,
        SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
            doc_text,
            ['weather', 'congestion', 'inspection', 'mechanical', 'customs', 'strike']
        ):label::VARCHAR                                        AS delay_category,
        SNOWFLAKE.CORTEX.SUMMARIZE(doc_text)                   AS notice_summary,
        SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
            doc_text,
            'What is the reason for the port delay?'
        )                                                      AS extracted_delay_reason
    FROM BRONZE.PORT_NOTICES;

-- ---------------------------------------------------------------------------
-- Node 3 (leaf): refresh the Gold delay dynamic table (runs after node 2).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE TASK GOLD.BUILD_DELAY_VIEW_TASK
    AFTER SILVER.ENRICH_NOTICES_TASK
AS
    ALTER DYNAMIC TABLE GOLD.VOYAGE_DELAY_ANALYSIS REFRESH;

-- ---------------------------------------------------------------------------
-- Resume root -> leaf (Snowflake requires the root task before its children).
-- Suspend in the opposite order: leaf -> root.
-- ---------------------------------------------------------------------------
ALTER TASK SILVER.FLATTEN_AIS_TASK RESUME;
ALTER TASK SILVER.ENRICH_NOTICES_TASK RESUME;
ALTER TASK GOLD.BUILD_DELAY_VIEW_TASK RESUME;
