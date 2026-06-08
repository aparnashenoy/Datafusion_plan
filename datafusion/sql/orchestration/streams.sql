-- DataFusion :: Orchestration streams
-- Append-only CDC streams on the Bronze landing tables. These drive the
-- Tasks DAG (see tasks.sql) via SYSTEM$STREAM_HAS_DATA.

USE DATABASE DATAFUSION;
USE SCHEMA BRONZE;

CREATE OR REPLACE STREAM BRONZE.AIS_POSITIONS_STREAM
    ON TABLE BRONZE.AIS_POSITIONS
    APPEND_ONLY = TRUE
    COMMENT = 'New raw AIS position payloads to flatten into Silver.';

CREATE OR REPLACE STREAM BRONZE.VESSEL_SCHEDULES_STREAM
    ON TABLE BRONZE.VESSEL_SCHEDULES
    APPEND_ONLY = TRUE
    COMMENT = 'New voyage schedules to refresh delay calculations.';

CREATE OR REPLACE STREAM BRONZE.PORT_NOTICES_STREAM
    ON TABLE BRONZE.PORT_NOTICES
    APPEND_ONLY = TRUE
    COMMENT = 'New port notices to enrich with Cortex AI.';
