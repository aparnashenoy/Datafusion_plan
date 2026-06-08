-- DataFusion :: Bronze tables
-- Raw landing tables. VARIANT columns hold unparsed JSON payloads;
-- structured tables mirror the source schema as-is.

USE DATABASE DATAFUSION;
USE SCHEMA BRONZE;

-- Raw AIS position reports (JSON).
CREATE TABLE IF NOT EXISTS BRONZE.AIS_POSITIONS (
    raw_payload   VARIANT
);

-- Structured voyage schedules (CSV).
CREATE TABLE IF NOT EXISTS BRONZE.VESSEL_SCHEDULES (
    voyage_id          VARCHAR,
    vessel_mmsi        VARCHAR,
    origin_port        VARCHAR,
    destination_port   VARCHAR,
    estimated_arrival  TIMESTAMP_NTZ,
    actual_arrival     TIMESTAMP_NTZ
);

-- Raw weather observations (JSON).
CREATE TABLE IF NOT EXISTS BRONZE.WEATHER (
    raw_payload   VARIANT
);

-- Port notices extracted from PDFs.
CREATE TABLE IF NOT EXISTS BRONZE.PORT_NOTICES (
    doc_id      VARCHAR,
    port_code   VARCHAR,
    doc_text    VARCHAR,
    issued_at   TIMESTAMP_NTZ
);

-- Catalog of scanned media / inspection image files.
CREATE TABLE IF NOT EXISTS BRONZE.MEDIA_FILES (
    file_name    VARCHAR,
    file_url     VARCHAR,
    file_type    VARCHAR,
    size_bytes   INTEGER
);
