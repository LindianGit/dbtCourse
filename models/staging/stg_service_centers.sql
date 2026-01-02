 {{ 
    config(
        materialized='incremental',
        unique_key='service_center_id',
        incremental_strategy='merge',
        schema='stage' 
    )
}}

--
-- stg_service_centers.sql  Staging model for service centers
--
-- BEST PRACTICES APPLIED:
-- - Incremental loading with merge on unique_key (service_center_id)
-- - Data cleaning: trims spaces, type casting for consistency.
-- - Deduplication logic future-proofing for service_center_id.
-- - Data quality flags for common issues.
-- - Metadata columns for audit & traceability.
-- - Clear structure and inline comments.
--
-- Note: If the source table ever gets record updates retroactively,
--       introduce an "updated_at" timestamp and use it for your incremental filter.
--

with deduped_source as (
    select
        ServiceCenterID,
        ServiceCenterName,
        Address,
        City,
        State,
        Zip,
        OutletID,
        row_number() over (
            partition by ServiceCenterID
            order by ServiceCenterID desc -- No timestamp available, so keep latest (future: use updated_at)
        ) as rn
    from {{ source('raw', 'CORE_SERVICE_CENTERS') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['ServiceCenterID']) }}    as service_center_sk,   -- Surrogate key for warehouse joins, NOT NULL
    cast(ServiceCenterID as integer)                               as service_center_id,   -- Service Center identifier, NOT NULL
    trim(ServiceCenterName)                                        as service_center_name, -- Name of the service center
    trim(Address)                                                  as address,             -- Service center street address
    trim(City)                                                     as city,                -- City
    trim(State)                                                    as state,               -- State or region
    trim(Zip)                                                      as zip,                 -- Postal code
    cast(OutletID as integer)                                      as outlet_id,           -- Outlet identifier
    current_timestamp                                              as dbt_loaded_at,       -- Load timestamp
    'stg_service_centers'                                          as dbt_model_name,      -- Traceability for warehouse/admins

    -- Data Quality Flags
    case 
        when ServiceCenterID is null then TRUE
        else FALSE
    end as has_bad_service_center_id,                                                        -- TRUE if missing/invalid service_center_id

    case
        when ServiceCenterName is null or ServiceCenterName = '' then TRUE
        else FALSE
    end as has_bad_service_center_name                                                        -- TRUE if name is missing

from deduped_source
where rn = 1 -- Only keep the latest record for each ServiceCenterID