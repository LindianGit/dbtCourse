{{ config(
    materialized='table',
    schema='mart'
) }}

with latest_service_center as (
    select *
    from {{ ref('dim_service_center_snapshot') }}
    where dbt_valid_to is null
)

select
    service_center_id,         -- Business (natural) key
    service_center_name,       -- Human-readable name
    address,
    city,
    state,
    zip,
    outlet_id,                 -- Foreign key to outlet (if you have outlet dimension)
    dbt_loaded_at              -- Snapshot load timestamp
from latest_service_center