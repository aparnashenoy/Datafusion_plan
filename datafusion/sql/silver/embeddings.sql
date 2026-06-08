-- DataFusion :: Silver document embeddings
-- Generates 768-dim vector embeddings for each port notice so they can be
-- searched semantically (see semantic_search.sql).

USE DATABASE DATAFUSION;
USE SCHEMA SILVER;

CREATE OR REPLACE TABLE SILVER.DOC_EMBEDDINGS AS
SELECT
    doc_id,
    port_code,
    issued_at,
    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
        'snowflake-arctic-embed-m',
        doc_text
    )                                AS embedding_vector
FROM BRONZE.PORT_NOTICES;
