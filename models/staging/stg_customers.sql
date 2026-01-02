{{ 
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge',
        schema='stage' 
    )
}}

--
-- stg_customers.sql  Staging model for customer records
--
-- BEST PRACTICES APPLIED:
-- - Incremental loading with merge on unique_key (customer_id)
-- - Deduplication: keeps only latest record per CUSTOMERID.
-- - Data cleaning: trims spaces, corrects case.
-- - Data quality flags: identifies missing/bad IDs, dates, and emails.
-- - Metadata columns for audit & traceability.
-- - Column-level comments for business clarity.
--
-- Note: If source table gets record updates retroactively, using CREATIONDATE
--       as the incremental filter may not capture late-arriving changes. 
--       Prefer an "updated_at" column if available.
--

with deduped_source as (
    select
        CUSTOMERID,
        FIRSTNAME,
        LASTNAME,
        PHONE,
        EMAIL,
        ADDRESS,
        CITY,
        STATE,
        ZIP,
        CREATIONDATE,
        PREFERREDCONTACTMETHOD,
        row_number() over (
            partition by CUSTOMERID
            order by CREATIONDATE desc
        ) as rn
    from {{ source('raw', 'CORE_CUSTOMERS') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['CUSTOMERID']) }} as customer_sk, -- Surrogate key for warehouse joins, NOT NULL
    cast(CUSTOMERID as varchar)            as customer_id,                 -- Original system customer ID, NOT NULL
    trim(FIRSTNAME)                        as first_name,                  -- Customer first name
    trim(LASTNAME)                         as last_name,                   -- Customer last name
    trim(PHONE)                            as phone,                       -- Customer phone number
    lower(trim(EMAIL))                     as email,                       -- Customer email, lowercased for consistency
    trim(ADDRESS)                          as address,                     -- Street address
    trim(CITY)                             as city,                        -- City
    trim(STATE)                            as state,                       -- State or region
    trim(ZIP)                              as zip,                         -- Postal code
    TRY_TO_DATE(CREATIONDATE)              as creation_date,               -- Date customer was created in source
    upper(trim(PREFERREDCONTACTMETHOD))    as preferred_contact_method,    -- Communication channel (EMAIL, PHONE, etc)
   -- current_timestamp                      as dbt_loaded_at,             -- Load timestamp
    TRY_TO_DATE(CREATIONDATE)              as dbt_loaded_at,               -- use as the effective date
    'stg_customers'                        as dbt_model_name,              -- Traceability for warehouse/admins

    -- Data Quality Flags
    case 
        when TRY_TO_DATE(CREATIONDATE) is null then TRUE
        else FALSE
    end as has_bad_creationdate,                                           -- TRUE if creation_date is null or invalid

    case 
        when CUSTOMERID is null or CUSTOMERID = '' then TRUE
        else FALSE
    end as has_bad_customerid,                                             -- TRUE if missing customer id

    case 
        when EMAIL is null 
            or len(trim(EMAIL)) < 6 
            or EMAIL not like '%@%.__%' -- requires @ and at least a dot+2 after it (minimal)
        then TRUE
        else FALSE
    end as has_bad_email                                                   -- TRUE for null/short/invalid email

from deduped_source
where rn = 1 -- Only keep the latest record for each CUSTOMERID
{% if is_incremental() %}
    -- Incremental filter: only new records by CREATIONDATE (use "updated_at" or similar if available)
    and TRY_TO_DATE(CREATIONDATE) > (select max(creation_date) from {{ this }})
{% endif %}