-- models/marts/mart_trips_daily.sql
with trips as (
    select * from {{ ref('stg_yellow_trips') }}
),

daily as (
    select
        date_trunc('day', pickup_at)::date              as trip_date,

        -- volumen
        count(*)                                        as total_trips,
        sum(passenger_count)                            as total_passengers,

        -- distancia
        round(sum(trip_distance)::numeric, 2)           as total_distance_miles,
        round(avg(trip_distance)::numeric, 2)           as avg_distance_miles,

        -- duración
        round(avg(trip_duration_minutes)::numeric, 1)   as avg_duration_minutes,

        -- revenue
        round(sum(total_amount)::numeric, 2)            as total_revenue,
        round(avg(total_amount)::numeric, 2)            as avg_fare,

        -- propinas
        round(sum(tip_amount)::numeric, 2)              as total_tips,
        round(avg(tip_amount)::numeric, 2)              as avg_tip,
        round(
            (100.0 * sum(tip_amount) / nullif(sum(total_amount), 0))::numeric, 2
        )                                               as tip_pct_of_revenue

    from trips
    group by 1
)

select * from daily
order by trip_date
