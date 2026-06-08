-- DataFusion :: Silver Cortex AI enrichment
-- Classifies, summarises, and extracts delay reasons from port notices.
-- Enrichment runs at write time, not query time.

USE DATABASE DATAFUSION;
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE SILVER.PORT_NOTICES_ENRICHED AS
SELECT
    doc_id,
    port_code,
    issued_at,
    doc_text,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        doc_text,
        ['weather', 'congestion', 'inspection', 'mechanical', 'customs', 'strike']
    ):label::VARCHAR                                              AS delay_category,
    SNOWFLAKE.CORTEX.SUMMARIZE(doc_text)                         AS notice_summary,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        doc_text,
        'What is the reason for the port delay?'
    )                                                            AS extracted_delay_reason
FROM BRONZE.PORT_NOTICES;
