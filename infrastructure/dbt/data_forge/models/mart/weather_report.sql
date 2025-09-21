{{
    config(
        materialized='table',
        schema='mart'
    )
}}

SELECT 
    city,
    temperature,
    weather_description,
    wind_speed,
    inserted_at
FROM {{ ref('stg_weather_data') }}
WHERE city IS NOT NULL
  AND temperature IS NOT NULL
  AND weather_description IS NOT NULL
  AND wind_speed IS NOT NULL
ORDER BY inserted_at DESC