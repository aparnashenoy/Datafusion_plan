-- DataFusion :: Bronze loads
-- COPY INTO statements pulling raw files from @BRONZE.RAW_STAGE.
-- Adjust the stage sub-paths/patterns to match your bucket layout.

USE DATABASE DATAFUSION;
USE SCHEMA BRONZE;

-- AIS positions (JSON -> VARIANT).
COPY INTO BRONZE.AIS_POSITIONS (raw_payload)
    FROM @BRONZE.RAW_STAGE/ais/
    FILE_FORMAT = (FORMAT_NAME = BRONZE.JSON_FORMAT)
    PATTERN = '.*[.]json'
    ON_ERROR = 'CONTINUE';

-- Vessel schedules (CSV -> structured columns).
COPY INTO BRONZE.VESSEL_SCHEDULES
    (voyage_id, vessel_mmsi, origin_port, destination_port,
     estimated_arrival, actual_arrival)
    FROM @BRONZE.RAW_STAGE/schedules/
    FILE_FORMAT = (FORMAT_NAME = BRONZE.CSV_FORMAT)
    PATTERN = '.*[.]csv'
    ON_ERROR = 'CONTINUE';

-- Weather observations (JSON -> VARIANT).
COPY INTO BRONZE.WEATHER (raw_payload)
    FROM @BRONZE.RAW_STAGE/weather/
    FILE_FORMAT = (FORMAT_NAME = BRONZE.JSON_FORMAT)
    PATTERN = '.*[.]json'
    ON_ERROR = 'CONTINUE';

-- Port notices (JSON with doc_id/port_code/doc_text/issued_at).
COPY INTO BRONZE.PORT_NOTICES (doc_id, port_code, doc_text, issued_at)
    FROM (
        SELECT
            $1:doc_id::VARCHAR,
            $1:port_code::VARCHAR,
            $1:doc_text::VARCHAR,
            $1:issued_at::TIMESTAMP_NTZ
        FROM @BRONZE.RAW_STAGE/port_notices/
    )
    FILE_FORMAT = (FORMAT_NAME = BRONZE.JSON_FORMAT)
    PATTERN = '.*[.]json'
    ON_ERROR = 'CONTINUE';

-- Media file catalog (JSON metadata).
COPY INTO BRONZE.MEDIA_FILES (file_name, file_url, file_type, size_bytes)
    FROM (
        SELECT
            $1:file_name::VARCHAR,
            $1:file_url::VARCHAR,
            $1:file_type::VARCHAR,
            $1:size_bytes::INTEGER
        FROM @BRONZE.RAW_STAGE/media/
    )
    FILE_FORMAT = (FORMAT_NAME = BRONZE.JSON_FORMAT)
    PATTERN = '.*[.]json'
    ON_ERROR = 'CONTINUE';
