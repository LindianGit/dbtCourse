{{ config(materialized='table', schema='mart') }}

with latest_employee as (
    select *
    from {{ ref('dim_employee_snapshot') }}
    where dbt_valid_to is null
)
select
    employee_id,
    first_name,
    last_name,
    role,
    outlet_id,
    hire_date,
    termination_date,
    email,
    phone,
    dbt_loaded_at
from latest_employee