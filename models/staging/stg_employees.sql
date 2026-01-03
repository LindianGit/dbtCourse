{{ 
    config(
        materialized='incremental',
        unique_key='employee_id',
        incremental_strategy='merge',
        schema='stage'
    )
}}

--
-- stg_employees.sql  Staging model for employee records
--
-- BEST PRACTICES APPLIED:
-- - Incremental loading with merge on unique_key (employee_id)
-- - Data cleaning: trims spaces, casts types for consistency.
-- - Deduplication & latest per employee_id (future-proofed).
-- - Data quality flags for common issues.
-- - Metadata columns for audit & traceability.
-- - Clear structure and inline comments.
--
-- Note: If the source table updates retroactively,
--       using HIREDATE as the incremental filter may not capture late-arriving/updated rows.
--       Prefer an "updated_at" column if available.
--

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
    {% if is_incremental() %}
        where HIREDATE > (select coalesce(max(hire_date), to_date('1900-01-01')) from {{ this }})
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['EMPLOYEEID']) }}      as employee_sk,      -- Surrogate key for consistency, NOT NULL
    cast(EMPLOYEEID as varchar)                                 as employee_id,      -- Employee identifier, NOT NULL
    trim(FIRSTNAME)                                            as first_name,       -- First name
    trim(LASTNAME)                                             as last_name,        -- Last name
    trim(ROLE)                                                 as role,             -- Job role/position
    cast(OUTLETID as integer)                                  as outlet_id,        -- Outlet identifier
    try_cast(HIREDATE as date)                                 as hire_date,        -- Date hired
    try_cast(TERMINATIONDATE as date)                          as termination_date, -- Date terminated (nullable)
    lower(trim(EMAIL))                                         as email,            -- Lowercase email
    trim(PHONE)                                                as phone,            -- Phone number
           
    current_timestamp as dbt_loaded_at,      -- after the intial load 
    'stg_employees'                                            as dbt_model_name,   -- Model traceability

    -- Data Quality Flags
    case
        when EMPLOYEEID is null or EMPLOYEEID = '' then TRUE
        else FALSE
    end as has_bad_employeeid,                                 -- TRUE if missing employee id

    case
        when try_cast(HIREDATE as date) is null then TRUE
        else FALSE
    end as has_bad_hiredate,                                   -- TRUE if hire date is null or invalid

    case
        when EMAIL is null or len(trim(EMAIL)) < 6 or EMAIL not like '%@%.__%' then TRUE
        else FALSE
    end as has_bad_email                                       -- TRUE if missing/short/invalid email

from deduped_source
where rn = 1 -- Only keep the latest record for each EMPLOYEEID
