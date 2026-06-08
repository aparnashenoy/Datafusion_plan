-- DataFusion :: Bronze setup
-- Creates the DATAFUSION database and the medallion schemas.

CREATE DATABASE IF NOT EXISTS DATAFUSION;

USE DATABASE DATAFUSION;

CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = 'Raw landing layer - never modified.';

CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Typed, cleaned, AI-enriched layer.';

CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Joined views, ML features, and scores.';
