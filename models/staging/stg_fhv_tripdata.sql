{{ config(materialized="view") }}

with
    tripdata as (

        select *
        from {{ source("staging", "fhv_tripdata") }}
        where
            dispatching_base_num is not null
            and extract(year from pickup_datetime) = 2019
    )
select
    -- identifiers
    dispatching_base_num,
    cast(p_ulocation_id as integer) as pickup_location_id,
    cast(d_olocation_id as integer) as dropoff_location_id,
    cast(sr_flag as integer) as sr_flag,
    affiliated_base_number,
    pickup_datetime,
    drop_off_datetime as dropoff_datetime

from {{ source("staging", "fhv_tripdata") }}
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var("is_test_run", default=false) %} limit 100 {% endif %}
