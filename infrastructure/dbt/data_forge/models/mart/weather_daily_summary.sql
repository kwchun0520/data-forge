{{ config(
    materialized='table',
    schema='mart'
) }}

with daily_weather as (
    select
        city,
        date(inserted_at) as measurement_date,
        avg(temperature) as avg_temperature,
        min(temperature) as min_temperature,
        max(temperature) as max_temperature,
        avg(wind_speed) as avg_wind_speed,
        min(wind_speed) as min_wind_speed,
        max(wind_speed) as max_wind_speed,
        count(*) as measurement_count,
        array_agg(distinct weather_description) as weather_conditions
    from {{ ref('stg_weather_data') }}
    group by city, date(inserted_at)
)

select * from daily_weather
order by city, measurement_date