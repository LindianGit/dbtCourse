{{ config(materialized='table', schema='mart') }}

with latest_vehicle as (
    select *
    from {{ ref('dim_vehicle_snapshot') }}
    where dbt_valid_to is null
)
select
    vin,
    car_make,
    car_model,
    year,
    color,
    initial_mileage,
    acquisition_source,
    acquisition_date,
    acquisition_cost,
    current_status,
    outlet_id,
    dbt_loaded_at
from latest_vehicle