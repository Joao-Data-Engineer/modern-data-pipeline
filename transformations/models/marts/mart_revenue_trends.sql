-- models/marts/mart_revenue_trends.sql
--
-- Tendencias de revenue diario con análisis de ventana temporal.
-- Responde: ¿está creciendo el negocio? ¿qué días son outliers?
--
-- Usa window functions: LAG, AVG OVER, RANK — no reemplazable con GROUP BY.

with daily as (
    select * from {{ ref('mart_trips_daily') }}
),

with_trends as (
    select
        trip_date,
        EXTRACT(DOW FROM trip_date)::int          AS day_of_week,

        -- volumen
        total_trips,
        total_revenue,
        avg_fare,
        avg_tip,
        tip_pct_of_revenue,

        -- ── media móvil 7 días ──────────────────────────────────────────
        -- suaviza picos diarios para ver la tendencia real
        round(
            avg(total_trips) over (
                order by trip_date
                rows between 6 preceding and current row
            )::numeric, 1
        )                                               as trips_7d_moving_avg,

        round(
            avg(total_revenue) over (
                order by trip_date
                rows between 6 preceding and current row
            )::numeric, 2
        )                                               as revenue_7d_moving_avg,

        -- ── comparativa semana anterior (WoW) ──────────────────────────
        -- requiere al menos 7 días de datos para no ser null
        lag(total_trips, 7)  over (order by trip_date)  as trips_same_day_last_week,
        lag(total_revenue, 7) over (order by trip_date) as revenue_same_day_last_week,

        -- ── crecimiento WoW en % ────────────────────────────────────────
        round(
            (100.0 * (total_trips - lag(total_trips, 7) over (order by trip_date))
            / nullif(lag(total_trips, 7) over (order by trip_date), 0))::numeric, 2
        )                                               as trips_wow_growth_pct,

        round(
            (100.0 * (total_revenue - lag(total_revenue, 7) over (order by trip_date))
            / nullif(lag(total_revenue, 7) over (order by trip_date), 0))::numeric, 2
        )                                               as revenue_wow_growth_pct,

        -- ── ranking diario dentro del mes ──────────────────────────────
        -- útil para detectar los mejores y peores días del mes
        rank() over (
            partition by date_trunc('month', trip_date)
            order by total_revenue desc
        )                                               as revenue_rank_in_month

    from daily
),

-- clasificación semántica del día
classified as (
    select
        *,
        case
            when trips_wow_growth_pct >  10 then 'growing'
            when trips_wow_growth_pct < -10 then 'declining'
            when trips_wow_growth_pct is null then 'insufficient_data'
            else 'stable'
        end                                             as demand_trend
    from with_trends
)

select * from classified
order by trip_date
