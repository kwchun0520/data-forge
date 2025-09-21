{{ config(
    materialized='incremental',
    schema='staging',
    unique_key='id',
    on_schema_change='fail'
) }}

with source_data as (
    select * from {{ ref('raw_weather_data') }}
    {% if is_incremental() %}
        where inserted_at > (select max(inserted_at) from {{ this }})
    {% endif %}
),

cleaned as (
    select
        -- Location info
        trim(lower(location_name)) as city,
        trim(location_country) as country,
        trim(location_region) as region,
        location_lat as latitude,
        location_lon as longitude,
        location_timezone_id as timezone,
        
        -- Weather conditions
        current_temperature as temperature,
        trim(current_weather_descriptions) as weather_description,
        current_weather_code as weather_code,
        current_wind_speed as wind_speed,
        current_wind_degree as wind_degree,
        current_wind_dir as wind_direction,
        current_pressure as pressure,
        current_precip as precipitation,
        current_humidity as humidity,
        current_cloudcover as cloud_cover,
        current_feelslike as feels_like_temperature,
        current_uv_index as uv_index,
        current_visibility as visibility,
        current_is_day as is_day,
        
        -- Air quality
        air_quality_co as co_level,
        air_quality_no2 as no2_level,
        air_quality_o3 as o3_level,
        air_quality_so2 as so2_level,
        air_quality_pm2_5 as pm2_5_level,
        air_quality_pm10 as pm10_level,
        air_quality_us_epa_index as us_epa_index,
        
        -- Astro data
        astro_sunrise as sunrise_time,
        astro_sunset as sunset_time,
        astro_moon_phase as moon_phase,
        astro_moon_illumination as moon_illumination,
        
        -- Metadata
        inserted_at,
        data_source
        
    from source_data
    where current_temperature is not null
      and location_name is not null
      and current_weather_descriptions is not null
      and current_wind_speed is not null
      and inserted_at is not null
)

select * from cleaned