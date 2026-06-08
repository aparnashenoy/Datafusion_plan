-- DataFusion :: Bronze external stage
-- Creates a JSON file format and an external S3 stage for raw ingestion.

USE DATABASE DATAFUSION;
USE SCHEMA BRONZE;

-- JSON file format used by the raw VARIANT tables.
CREATE OR REPLACE FILE FORMAT BRONZE.JSON_FORMAT
    TYPE = JSON
    STRIP_OUTER_ARRAY = TRUE
    COMMENT = 'Standard JSON parsing for AIS / weather payloads.';

-- CSV file format used by the structured schedule loads.
CREATE OR REPLACE FILE FORMAT BRONZE.CSV_FORMAT
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL')
    COMMENT = 'CSV parsing for vessel schedule files.';

-- External S3 stage. Replace the URL and credentials/storage integration
-- with your own before loading. Prefer a STORAGE INTEGRATION over inline keys.
CREATE OR REPLACE STAGE BRONZE.RAW_STAGE
    URL = 's3://datafusion-raw/'
    -- STORAGE_INTEGRATION = DATAFUSION_S3_INT
    FILE_FORMAT = BRONZE.JSON_FORMAT
    COMMENT = 'External S3 stage for raw multimodal source files.';
