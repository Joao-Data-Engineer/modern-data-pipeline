-- models/marts/mart_trips_zone.sql
-- Agregados por zona de pickup: qué zonas generan más viajes y revenue

with trips as (
    select * from {{ ref('stg_yellow_trips') }}
),

by_zone as (
    select
        pu_location_id                              as pickup_zone_id,

        -- volumen
        count(*)                                    as total_trips,

        -- destinos más frecuentes
        mode() within group (
            order by do_location_id
        )                                           as most_common_dropoff_zone,

        -- distancia
        round(avg(trip_distance)::numeric, 2)       as avg_distance_miles,

        -- revenue
        round(sum(total_amount)::numeric, 2)        as total_revenue,
        round(avg(total_amount)::numeric, 2)        as avg_fare,

        -- propinas
        round(avg(tip_amount)::numeric, 2)          as avg_tip,

        -- duración
        round(avg(trip_duration_minutes)::numeric, 1) as avg_duration_minutes

    from trips
    group by 1
)

select * from by_zone
order by total_trips desc
