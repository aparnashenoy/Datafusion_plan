-- DataFusion :: Gold voyage delay analysis
-- Primary Gold output. Joins the Silver layers into one delay-analysis view,
-- maintained automatically as a dynamic table with a 15-minute target lag.
--
-- Replace DATAFUSION_WH with your warehouse name before running.

USE DATABASE DATAFUSION;
USE SCHEMA GOLD;

CREATE OR REPLACE DYNAMIC TABLE GOLD.VOYAGE_DELAY_ANALYSIS
    TARGET_LAG = '15 minutes'
    WAREHOUSE = DATAFUSION_WH
AS
WITH weather_agg AS (
    SELECT
        voyage_id,
        AVG(wind_speed_knots)   AS avg_wind_speed,
        MAX(wave_height_m)      AS max_wave_height
    FROM SILVER.WEATHER
    GROUP BY voyage_id
),
ais_agg AS (
    SELECT
        vessel_mmsi,
        MAX(vessel_name)        AS vessel_name,
        COUNT(*)                AS position_reports,
        AVG(speed_knots)        AS avg_speed_knots
    FROM SILVER.AIS_POSITIONS
    GROUP BY vessel_mmsi
),
port_queue AS (
    -- Naive congestion proxy: number of notices per destination port.
    SELECT
        port_code,
        COUNT(*)                AS port_queue_depth
    FROM SILVER.PORT_NOTICES_ENRICHED
    GROUP BY port_code
)
SELECT
    s.voyage_id,
    a.vessel_name,
    s.delay_hours,
    w.avg_wind_speed,
    w.max_wave_height,
    q.port_queue_depth,
    n.delay_category,
    n.extracted_delay_reason,
    -- Heuristic risk score in [0, 1] until the ML model scores are joined in.
    LEAST(
        1.0,
        GREATEST(
            0.0,
            (COALESCE(s.delay_hours, 0) / 48.0) * 0.5
                + (COALESCE(w.max_wave_height, 0) / 10.0) * 0.3
                + (COALESCE(q.port_queue_depth, 0) / 20.0) * 0.2
        )
    )                                                  AS predicted_risk_score
FROM SILVER.VESSEL_SCHEDULES        AS s
LEFT JOIN ais_agg                   AS a ON s.vessel_mmsi = a.vessel_mmsi
LEFT JOIN weather_agg               AS w ON s.voyage_id = w.voyage_id
LEFT JOIN port_queue                AS q ON s.destination_port = q.port_code
LEFT JOIN SILVER.PORT_NOTICES_ENRICHED AS n ON s.destination_port = n.port_code;
