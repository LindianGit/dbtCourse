{{ config(materialized='table', schema='mart') }}

with latest_outlet as (
    select *
    from {{ ref('dim_outlet_snapshot') }}
    where dbt_valid_to is null
)
select
    outlet_id,
    outlet_name,
    address,
    city,
    state,
    zip,
    dbt_loaded_at
from latest_outlet