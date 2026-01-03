 {{ 
    config(
        materialized='incremental',
        unique_key='vin',
        incremental_strategy='merge',
        schema='stage' 
    )
}}

--
-- stg_inventory.sql  Staging model for vehicle inventory
--
-- BEST PRACTICES APPLIED:
-- - Incremental loading with merge on unique_key (vin)
-- - Data cleaning: trims spaces, casts types for consistency.
-- - Deduplication to keep only the latest per VIN.
-- - Data quality flags for common issues.
-- - Metadata columns for audit & traceability.
-- - Clear structure and inline comments.
--
-- Note: If the source table gets record updates retroactively, using AcquisitionDate
--       as the incremental filter may not capture late-arriving or updated data. 
--       Prefer an "updated_at" column if available.
--

with deduped_source as (
    select
        VIN,
        CarMake,
        CarModel,
        Year,
        Color,
        InitialMileage,
        AcquisitionSource,
        AcquisitionDate,
        AcquisitionCost,
        CurrentStatus,
        OutletID,
        row_number() over (
            partition by VIN
            order by AcquisitionDate desc
        ) as rn
    from {{ source('raw', 'CORE_INVENTORY') }}
    {% if is_incremental() %}
        where TRY_TO_DATE(AcquisitionDate) > (select max(acquisition_date) from {{ this }})
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['VIN']) }}      as inventory_sk,     -- Surrogate key for warehouse joins, NOT NULL
    trim(VIN)                                           as vin,              -- Vehicle Identification Number, NOT NULL
    trim(CarMake)                                       as car_make,         -- Make/Brand of vehicle
    trim(CarModel)                                      as car_model,        -- Model name/number
    cast(Year as integer)                               as year,             -- Vehicle model year
    trim(Color)                                         as color,            -- Color
    cast(InitialMileage as integer)                     as initial_mileage,  -- Initial mileage
    trim(AcquisitionSource)                             as acquisition_source, -- Source for inventory
    TRY_TO_DATE(AcquisitionDate)                        as acquisition_date,   -- Date acquired
    TRY_TO_NUMBER(AcquisitionCost)                      as acquisition_cost,   -- Cost to acquire
    trim(CurrentStatus)                                 as current_status,     -- Status (e.g., available, sold, etc.)
    cast(OutletID as integer)                           as outlet_id,          -- Outlet identifier
    current_timestamp                                   as dbt_loaded_at,      -- Load timestamp

    'stg_inventory'                                     as dbt_model_name,     -- Traceability for warehouse/admins

    -- Data Quality Flags
    case 
        when TRY_TO_DATE(AcquisitionDate) is null then TRUE
        else FALSE
    end as has_bad_acquisitiondate,                                  -- TRUE if acquisition date is null or invalid

    case
        when VIN is null or VIN = '' or len(trim(VIN)) < 6 then TRUE
        else FALSE
    end as has_bad_vin,                                              -- TRUE for null/short/invalid VIN

    case
        when Year is null or cast(Year as integer) < 1980 or cast(Year as integer) > EXTRACT(year from current_date) + 1 then TRUE 
        else FALSE
    end as has_bad_year                                              -- TRUE if year is missing or out of sensible range

from deduped_source
where rn = 1 -- Only keep the latest record for each VIN