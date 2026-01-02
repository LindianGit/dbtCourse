{{ 
    config(
        materialized='incremental',
        unique_key='outlet_id',
        incremental_strategy='merge',
        schema='stage' 
    )
}}

--
-- stg_outlets.sql  Staging model for physical outlets
--
-- BEST PRACTICES APPLIED:
-- - Incremental loading with merge on unique_key (outlet_id)
-- - Data cleaning: trims spaces, type casting for consistency.
-- - Deduplication logic for outlet_id (future proof, even if data updates).
-- - Data quality flags for common issues.
-- - Metadata columns for audit & traceability.
-- - Clear structure and inline comments.
--
-- Note: If the source table gets record updates retroactively, introduce a timestamp (e.g., updated_at)
--       and use it for your incremental filter for late-arriving data handling.
--

with deduped_source as (
    select
        OutletID,
        OutletName,
        Address,
        City,
        State,
        Zip,
        row_number() over (
            partition by OutletID
            order by OutletID desc -- No timestamp available, so keep latest (future: use updated_at)
        ) as rn
    from {{ source('raw', 'CORE_OUTLETS') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['OutletID']) }} as outlet_sk, -- Surrogate key for warehouse joins, NOT NULL
    cast(OutletID as integer)                             as outlet_id, -- Primary outlet identifier, NOT NULL
    trim(OutletName)                                      as outlet_name, -- Name of the outlet
    trim(Address)                                         as address,     -- Outlet street address
    trim(City)                                            as city,        -- City
    trim(State)                                           as state,       -- State or region
    trim(Zip)                                             as zip,         -- Postal code
    current_timestamp                                     as dbt_loaded_at, -- Load timestamp
    'stg_outlets'                                         as dbt_model_name, -- Traceability for warehouse/admins

    -- Data Quality Flags
    case 
        when OutletID is null then TRUE
        else FALSE
    end as has_bad_outletid,                                              -- TRUE if missing or invalid outlet id

    case
        when OutletName is null or OutletName = '' then TRUE
        else FALSE
    end as has_bad_outletname                                             -- TRUE if name is missing

from deduped_source
where rn = 1 -- Keep only the latest record for each OutletID