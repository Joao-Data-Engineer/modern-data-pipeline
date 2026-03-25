-- Schemas
CREATE SCHEMA IF NOT EXISTS raw;

-- Control table: incremental watermark
CREATE TABLE IF NOT EXISTS raw.ingestion_watermark (
    source_name TEXT PRIMARY KEY,
    last_loaded_month DATE NOT NULL
);

-- Raw table: NYC Taxi yellow trips
CREATE TABLE IF NOT EXISTS raw.nyc_taxi_yellow_trips (
    vendor_id              INT,
    tpep_pickup_datetime   TIMESTAMP,
    tpep_dropoff_datetime  TIMESTAMP,
    passenger_count        INT,
    trip_distance          DOUBLE PRECISION,
    ratecode_id            INT,
    store_and_fwd_flag     TEXT,
    pu_location_id         INT,
    do_location_id         INT,
    payment_type           INT,
    fare_amount            NUMERIC(10, 2),
    extra                  NUMERIC(10, 2),
    mta_tax                NUMERIC(10, 2),
    tip_amount             NUMERIC(10, 2),
    tolls_amount           NUMERIC(10, 2),
    improvement_surcharge  NUMERIC(10, 2),
    total_amount           NUMERIC(10, 2),
    congestion_surcharge   NUMERIC(10, 2),
    airport_fee            NUMERIC(10, 2),
    source_file            TEXT,
    loaded_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Audit table: one row per pipeline execution
CREATE TABLE IF NOT EXISTS raw.ingestion_audit (
    id             SERIAL PRIMARY KEY,
    source_name    TEXT        NOT NULL,
    month          TEXT        NOT NULL,
    source_file    TEXT        NOT NULL,
    status         TEXT        NOT NULL,  -- 'success' | 'error'
    rows_loaded    INT,
    error_message  TEXT,
    started_at     TIMESTAMP   NOT NULL,
    finished_at    TIMESTAMP,
    duration_secs  NUMERIC(10, 2)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_yellow_pickup_dt ON raw.nyc_taxi_yellow_trips (tpep_pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_yellow_pu_loc    ON raw.nyc_taxi_yellow_trips (pu_location_id);
CREATE INDEX IF NOT EXISTS idx_yellow_source_file ON raw.nyc_taxi_yellow_trips (source_file);
CREATE INDEX IF NOT EXISTS idx_audit_source     ON raw.ingestion_audit (source_name, month);
