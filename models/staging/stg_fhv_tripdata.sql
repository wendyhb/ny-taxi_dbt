{{ config(materialized='view') }}
 
with tripdata as (

    select * 
    from {{ source('staging', 'fhv_tripdata') }}
    where dispatching_base_num is not null 
    AND EXTRACT(YEAR FROM pickup_datetime) = 2019
)
select
    -- identifiers
    dispatching_base_num,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    cast(SR_Flag as integer) as SR_Flag,
    Affiliated_base_number,

from {{ source('staging', 'fhv_tripdata') }}
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=false) %}

  limit 100

{% endif %}