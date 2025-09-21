{{ config(
    materialized='table',
    schema='raw',
    unique_key='id',
    on_schema_change='fail'
) }}

select * from {{ source('source', 'weather_data') }}