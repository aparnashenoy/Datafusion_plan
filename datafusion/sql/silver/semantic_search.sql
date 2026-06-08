-- DataFusion :: Silver semantic search
-- Parameterised template: finds the top 10 port notices most relevant to a
-- free-text query using cosine similarity over the stored embeddings.
--
-- Replace :query_text with your search string (or bind it as a parameter),
-- e.g. via snowsql:  snowsql -f semantic_search.sql -D query_text='berth congestion at night'

USE DATABASE DATAFUSION;
USE SCHEMA SILVER;

SELECT
    e.doc_id,
    e.port_code,
    e.issued_at,
    n.notice_summary,
    n.delay_category,
    VECTOR_COSINE_SIMILARITY(
        e.embedding_vector,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', :query_text)
    )                                AS similarity
FROM SILVER.DOC_EMBEDDINGS         AS e
JOIN SILVER.PORT_NOTICES_ENRICHED  AS n
    ON e.doc_id = n.doc_id
ORDER BY similarity DESC
LIMIT 10;
