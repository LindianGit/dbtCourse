{{ config(materialized='table', schema='mart') }}

with latest_customer as (
    select *
    from {{ ref('dim_customer_snapshot') }}
    where dbt_valid_to is null
)
select
    customer_id,
    first_name,
    last_name,
    phone,
    email,
    address,
    city,
    state,
    zip,
    creation_date,
    preferred_contact_method,
    dbt_loaded_at
from latest_customer