-- Schemas
CREATE SCHEMA IF NOT EXISTS raw;

-- Control table to track incremental loads
CREATE TABLE IF NOT EXISTS raw.ingestion_watermark (
    source_name TEXT PRIMARY KEY,
    last_loaded_month DATE NOT NULL
);

-- Raw table for NYC Taxi
CREATE TABLE IF NOT EXISTS raw.nyc_taxi_yellow_trips (
    vendor_id INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance DOUBLE PRECISION,
    ratecode_id INT,
    store_and_fwd_flag TEXT,
    pu_location_id INT,
    do_location_id INT,
    payment_type INT,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,
    source_file TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_yellow_pickup_dt ON raw.nyc_taxi_yellow_trips (tpep_pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_yellow_pu_loc ON raw.nyc_taxi_yellow_trips (pu_location_id);
