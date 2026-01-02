{{ 
    config(
        materialized='incremental',
        unique_key='sale_id',
        incremental_strategy='merge',
        schema='stage' 
    )
}}

--
-- stg_sales.sql  Staging model for vehicle sales
--
-- BEST PRACTICES APPLIED:
-- - Incremental loading with merge on unique_key (sale_id)
-- - Data cleaning: trims, lowercases, casts types for consistency.
-- - Deduplication logic to keep only the latest per SaleID (future-proof).
-- - Data quality flags for common issues.
-- - Metadata columns for audit & traceability.
-- - Clear structure and comments.
--
-- Note: If the source table can have late-arriving updates, consider using an "updated_at" column
--       for incremental loads instead of SaleDate.
--

with deduped_source as (
    select
        SaleID,
        SaleDate,
        VIN,
        SoldBy,
        CustomerID,
        SalePrice,
        PaymentMethod,
        OutletID,
        TradeInVIN,
        row_number() over (
            partition by SaleID
            order by SaleDate desc
        ) as rn
    from {{ source('raw', 'CORE_SALES') }}
    {% if is_incremental() %}
        where TRY_TO_DATE(SaleDate) > (select max(sale_date) from {{ this }})
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['SaleID']) }}     as sale_sk,           -- Surrogate key for warehouse joins, NOT NULL
    cast(SaleID as varchar)                                as sale_id,           -- Sale record identifier, NOT NULL
    TRY_TO_DATE(SaleDate)                                  as sale_date,         -- Date of sale
    trim(VIN)                                              as vin,               -- Vehicle Identification Number
    trim(SoldBy)                                           as sold_by,           -- Salesperson name or ID
    trim(CustomerID)                                       as customer_id,       -- Associated customer id
    TRY_TO_NUMBER(SalePrice)                               as sale_price,        -- Final sale price
    trim(PaymentMethod)                                    as payment_method,    -- Payment method (e.g. CASH, CREDIT)
    cast(OutletID as integer)                              as outlet_id,         -- Outlet identifier
    trim(TradeInVIN)                                       as trade_in_vin,      -- Trade-in vehicle VIN (if any)
    TRY_TO_DATE(SALEDATE) as dbt_loaded_at,             -- use as effective, not current date
    'stg_sales'                                            as dbt_model_name,    -- Traceability for warehouse/admins

    -- Data Quality Flags
    case 
        when TRY_TO_DATE(SaleDate) is null then TRUE
        else FALSE
    end as has_bad_saledate,                                                        -- TRUE if sale date is null or invalid

    case
        when SaleID is null or SaleID = '' then TRUE
        else FALSE
    end as has_bad_saleid,                                                          -- TRUE if missing sale id

    case 
        when VIN is null or VIN = '' or len(trim(VIN)) < 6 then TRUE
        else FALSE
    end as has_bad_vin                                                              -- TRUE for null/short/invalid VIN

from deduped_source
where rn = 1 -- Only keep the latest record for each SaleID