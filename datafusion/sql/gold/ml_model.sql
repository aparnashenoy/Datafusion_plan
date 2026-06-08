-- DataFusion :: Gold delay-prediction model
-- Trains a Snowflake ML CLASSIFICATION model on GOLD.DELAY_FEATURES and scores
-- upcoming voyages by delay probability.

USE DATABASE DATAFUSION;
USE SCHEMA GOLD;

-- Training view: only rows with a known label and complete features.
CREATE OR REPLACE VIEW GOLD.DELAY_TRAINING_DATA AS
SELECT
    days_to_arrival,
    route_historical_delay_avg,
    weather_risk_score,
    port_congestion_index,
    is_delayed
FROM GOLD.DELAY_FEATURES;

-- Train the classifier (target column: is_delayed).
CREATE OR REPLACE SNOWFLAKE.ML.CLASSIFICATION GOLD.DELAY_MODEL(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'GOLD.DELAY_TRAINING_DATA'),
    TARGET_COLNAME => 'IS_DELAYED'
);

-- Score all upcoming voyages (future arrivals), ordered by risk descending.
SELECT
    f.voyage_id,
    pred.class                                          AS predicted_is_delayed,
    pred.probability['1']::FLOAT                        AS delay_probability
FROM GOLD.DELAY_FEATURES AS f,
    TABLE(
        GOLD.DELAY_MODEL!PREDICT(
            INPUT_DATA => OBJECT_CONSTRUCT(
                'DAYS_TO_ARRIVAL',            f.days_to_arrival,
                'ROUTE_HISTORICAL_DELAY_AVG', f.route_historical_delay_avg,
                'WEATHER_RISK_SCORE',         f.weather_risk_score,
                'PORT_CONGESTION_INDEX',      f.port_congestion_index
            )
        )
    ) AS pred
WHERE f.days_to_arrival >= 0
ORDER BY delay_probability DESC;
