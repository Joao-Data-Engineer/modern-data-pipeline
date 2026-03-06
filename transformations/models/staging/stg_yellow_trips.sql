-- models/staging/stg_yellow_trips.sql
with source as (
    select * from {{ source('raw', 'nyc_taxi_yellow_trips') }}
),

cleaned as (
    select
        -- identifiers
        vendor_id,
        pu_location_id,
        do_location_id,

        -- timestamps
        tpep_pickup_datetime                            as pickup_at,
        tpep_dropoff_datetime                           as dropoff_at,

        -- trip details
        passenger_count,
        trip_distance,
        ratecode_id,
        store_and_fwd_flag,
        payment_type,

        -- amounts
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        coalesce(airport_fee, 0)                        as airport_fee,
        total_amount,

        -- derived (PostgreSQL syntax)
        EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60 as trip_duration_minutes,

        -- metadata
        source_file,
        loaded_at

    from source

    where
        tpep_pickup_datetime is not null
        and tpep_dropoff_datetime is not null
        and total_amount >= 0
        and trip_distance >= 0
        and tpep_dropoff_datetime > tpep_pickup_datetime
)

select * from cleaned
