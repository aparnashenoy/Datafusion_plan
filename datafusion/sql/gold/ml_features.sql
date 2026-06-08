-- DataFusion :: Gold ML feature table
-- Engineered features for the delay-prediction classification model.
-- is_delayed is the supervised label (1 when actual arrival is late).

USE DATABASE DATAFUSION;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE GOLD.DELAY_FEATURES AS
WITH route_history AS (
    -- Historical average delay per origin->destination route.
    SELECT
        origin_port,
        destination_port,
        AVG(delay_hours)        AS route_historical_delay_avg
    FROM SILVER.VESSEL_SCHEDULES
    GROUP BY origin_port, destination_port
),
weather_agg AS (
    SELECT
        voyage_id,
        AVG(wind_speed_knots)   AS avg_wind_speed,
        MAX(wave_height_m)      AS max_wave_height
    FROM SILVER.WEATHER
    GROUP BY voyage_id
),
port_queue AS (
    SELECT
        port_code,
        COUNT(*)                AS port_congestion_index
    FROM SILVER.PORT_NOTICES_ENRICHED
    GROUP BY port_code
)
SELECT
    s.voyage_id,
    DATEDIFF('day', CURRENT_TIMESTAMP(), s.estimated_arrival)        AS days_to_arrival,
    COALESCE(r.route_historical_delay_avg, 0)                        AS route_historical_delay_avg,
    -- Weather risk: scaled combination of wind and wave severity.
    LEAST(1.0,
        COALESCE(w.avg_wind_speed, 0) / 50.0 * 0.5
        + COALESCE(w.max_wave_height, 0) / 10.0 * 0.5
    )                                                                AS weather_risk_score,
    COALESCE(q.port_congestion_index, 0)                            AS port_congestion_index,
    IFF(COALESCE(s.delay_hours, 0) > 0, 1, 0)                       AS is_delayed
FROM SILVER.VESSEL_SCHEDULES    AS s
LEFT JOIN route_history         AS r
    ON s.origin_port = r.origin_port
   AND s.destination_port = r.destination_port
LEFT JOIN weather_agg           AS w ON s.voyage_id = w.voyage_id
LEFT JOIN port_queue            AS q ON s.destination_port = q.port_code;
