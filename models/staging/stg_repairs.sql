{{ 
    config(
        materialized='incremental',
        unique_key='repair_id',
        incremental_strategy='merge',
        schema='stage' 
    )
}}

--
-- stg_repairs.sql  Staging model for vehicle repairs
--
-- BEST PRACTICES APPLIED:
-- - Incremental loading with merge on unique_key (repair_id)
-- - Data cleaning: trims spaces, casts types for consistency.
-- - Deduplication to keep only the latest per RepairID in future-proof logic.
-- - Data quality flags for common issues.
-- - Metadata columns for audit & traceability.
-- - Clear structure and inline comments.
--
-- Note: If the source table gets record updates retroactively, using RepairDate
--       as the incremental filter may not capture late-arriving or updated data. 
--       Prefer an "updated_at" column if available.
--

with deduped_source as (
    select
        RepairID,
        VIN,
        RepairDate,
        ServiceType,
        PartsCost,
        LaborCost,
        ServiceCenterID,
        MechanicID,
        MileageAtService,
        WarrantyWork,
        row_number() over (
            partition by RepairID
            order by RepairDate desc
        ) as rn
    from {{ source('raw', 'CORE_REPAIRS') }}
    {% if is_incremental() %}
        where TRY_TO_DATE(RepairDate) > (select max(repair_date) from {{ this }})
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['RepairID']) }}    as repair_sk,           -- Surrogate key for warehouse joins, NOT NULL
    cast(RepairID as varchar)                               as repair_id,           -- Repair record identifier, NOT NULL
    trim(VIN)                                               as vin,                 -- Vehicle Identification Number
    TRY_TO_DATE(RepairDate)                                 as repair_date,         -- Date of repair
    trim(ServiceType)                                       as service_type,        -- Type of service performed
    TRY_TO_NUMBER(PartsCost)                                as parts_cost,          -- Cost of replacement parts
    TRY_TO_NUMBER(LaborCost)                                as labor_cost,          -- Cost of labor
    cast(ServiceCenterID as integer)                        as service_center_id,   -- Service center identifier
    trim(MechanicID)                                        as mechanic_id,         -- Unique mechanic identifier
    cast(MileageAtService as integer)                       as mileage_at_service,  -- Vehicle mileage at time of service
    upper(trim(WarrantyWork))                               as warranty_work,       -- 'YES'/'NO' indicating warranty status
    current_timestamp                                       as dbt_loaded_at,       -- Load timestamp
    'stg_repairs'                                           as dbt_model_name,      -- Traceability for warehouse/admins

    -- Data Quality Flags
    case 
        when TRY_TO_DATE(RepairDate) is null then TRUE
        else FALSE
    end as has_bad_repairdate,                                                        -- TRUE if repair date is null or invalid

    case
        when RepairID is null or RepairID = '' then TRUE
        else FALSE
    end as has_bad_repairid,                                                          -- TRUE if missing repair id

    case 
        when VIN is null or VIN = '' or len(trim(VIN)) < 6 then TRUE
        else FALSE
    end as has_bad_vin                                                                -- TRUE for null/short/invalid VIN

from deduped_source
where rn = 1 -- Only keep the latest record for each RepairID