{{ config(materialized='table', schema='stage') }}

with deduped_source as (
    select
        EMPLOYEEID,
        FIRSTNAME,
        LASTNAME,
        ROLE,
        OUTLETID,
        HIREDATE,
        TERMINATIONDATE,
        EMAIL,
        PHONE,
        row_number() over (
            partition by EMPLOYEEID
            order by HIREDATE desc
        ) as rn
    from {{ source('raw', 'CORE_EMPLOYEES') }}
)
select
    cast(EMPLOYEEID as varchar)              as employee_id,
    trim(FIRSTNAME)                          as first_name,
    trim(LASTNAME)                           as last_name,
    trim(ROLE)                               as role,
    cast(OUTLETID as integer)                as outlet_id,
    try_cast(HIREDATE as date)               as hire_date,
    try_cast(TERMINATIONDATE as date)        as termination_date,
    lower(trim(EMAIL))                       as email,
    trim(PHONE)                              as phone
from deduped_source
where rn = 1